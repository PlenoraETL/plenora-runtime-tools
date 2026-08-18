use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    path::{Path, PathBuf},
};

/// Hard upper bound for argument count.
pub const MAX_ARGUMENT_COUNT: usize = 256;
/// Hard upper bound for all argument bytes.
pub const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;
/// Hard upper bound for explicitly supplied environment entries.
pub const MAX_ENVIRONMENT_ENTRIES: usize = 256;
/// Hard upper bound for environment key and value bytes.
pub const MAX_ENVIRONMENT_BYTES: usize = 1024 * 1024;

/// Scope used when a running child must be terminated.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProcessTreeMode {
    /// Targets the direct child only.
    DirectChild,
    /// Starts the child in an isolated group and targets the complete group on supported hosts.
    #[default]
    IsolatedTree,
}

/// Application-owned executable and argument declaration.
#[derive(Clone, Eq, PartialEq)]
pub struct SubprocessSpec {
    executable: PathBuf,
    arguments: Vec<String>,
    environment: BTreeMap<String, String>,
    current_directory: Option<PathBuf>,
    clear_environment: bool,
}

impl SubprocessSpec {
    /// Creates a specification for a non-empty executable path.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty path.
    pub fn new(executable: impl Into<PathBuf>) -> Result<Self, SubprocessSpecError> {
        let executable = executable.into();
        if executable.as_os_str().is_empty() {
            return Err(SubprocessSpecError::EmptyExecutable);
        }
        Ok(Self {
            executable,
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            current_directory: None,
            clear_environment: true,
        })
    }

    /// Adds one argument without interpreting it through a shell.
    ///
    /// # Errors
    ///
    /// Returns an error when count or aggregate byte bounds would be exceeded.
    pub fn with_argument(
        mut self,
        argument: impl Into<String>,
    ) -> Result<Self, SubprocessSpecError> {
        let argument = argument.into();
        if self.arguments.len() >= MAX_ARGUMENT_COUNT {
            return Err(SubprocessSpecError::TooManyArguments);
        }
        let aggregate = self
            .arguments
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(argument.len());
        if aggregate > MAX_ARGUMENT_BYTES {
            return Err(SubprocessSpecError::ArgumentsTooLarge);
        }
        self.arguments.push(argument);
        Ok(self)
    }

    /// Adds or replaces one explicit environment entry.
    ///
    /// # Errors
    ///
    /// Returns an error for empty/invalid keys or exceeded entry/byte bounds.
    pub fn with_environment(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, SubprocessSpecError> {
        let key = key.into();
        let value = value.into();
        if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
            return Err(SubprocessSpecError::InvalidEnvironmentEntry);
        }
        if !self.environment.contains_key(&key) && self.environment.len() >= MAX_ENVIRONMENT_ENTRIES
        {
            return Err(SubprocessSpecError::TooManyEnvironmentEntries);
        }
        let replaced_bytes = self
            .environment
            .get(&key)
            .map_or(0, |previous| key.len().saturating_add(previous.len()));
        let current_bytes = self.environment_bytes();
        let aggregate = current_bytes
            .saturating_sub(replaced_bytes)
            .saturating_add(key.len())
            .saturating_add(value.len());
        if aggregate > MAX_ENVIRONMENT_BYTES {
            return Err(SubprocessSpecError::EnvironmentTooLarge);
        }
        let _previous = self.environment.insert(key, value);
        Ok(self)
    }

    /// Sets the child working directory.
    #[must_use]
    pub fn with_current_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.current_directory = Some(directory.into());
        self
    }

    /// Allows explicitly inheriting the parent environment.
    ///
    /// Environment inheritance can expose credentials and should be enabled only by the embedding
    /// application after reviewing its deployment environment.
    #[must_use]
    pub const fn with_inherited_environment(mut self) -> Self {
        self.clear_environment = false;
        self
    }

    /// Returns the configured executable path.
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub(crate) const fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub(crate) fn current_directory(&self) -> Option<&Path> {
        self.current_directory.as_deref()
    }

    pub(crate) const fn clear_environment(&self) -> bool {
        self.clear_environment
    }

    fn environment_bytes(&self) -> usize {
        self.environment
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum()
    }
}

impl Debug for SubprocessSpec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubprocessSpec")
            .field("executable", &"<configured; redacted>")
            .field("argument_count", &self.arguments.len())
            .field("environment_entry_count", &self.environment.len())
            .field("current_directory", &self.current_directory.is_some())
            .field("clear_environment", &self.clear_environment)
            .finish()
    }
}

/// Invalid subprocess specification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubprocessSpecError {
    /// Executable path is empty.
    EmptyExecutable,
    /// Argument count exceeded its hard bound.
    TooManyArguments,
    /// Aggregate argument bytes exceeded their hard bound.
    ArgumentsTooLarge,
    /// Environment key or value is invalid.
    InvalidEnvironmentEntry,
    /// Environment entry count exceeded its hard bound.
    TooManyEnvironmentEntries,
    /// Aggregate environment bytes exceeded their hard bound.
    EnvironmentTooLarge,
}

impl Display for SubprocessSpecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyExecutable => "subprocess executable path must not be empty",
            Self::TooManyArguments => "subprocess argument count exceeds the hard maximum",
            Self::ArgumentsTooLarge => "subprocess arguments exceed the aggregate byte bound",
            Self::InvalidEnvironmentEntry => "subprocess environment entry is invalid",
            Self::TooManyEnvironmentEntries => {
                "subprocess environment entry count exceeds the hard maximum"
            }
            Self::EnvironmentTooLarge => "subprocess environment exceeds the aggregate byte bound",
        })
    }
}

impl Error for SubprocessSpecError {}
