use opentelemetry::Context;
use tracing::Span;
use tracing_opentelemetry::{OpenTelemetrySpanExt as _, SetParentError};

/// Returns the `OpenTelemetry` context associated with a tracing span.
#[must_use]
pub fn span_context(span: &Span) -> Context {
    span.context()
}

/// Returns the `OpenTelemetry` context associated with the current tracing span.
#[must_use]
pub fn current_context() -> Context {
    Span::current().context()
}

/// Sets an extracted `OpenTelemetry` context as a tracing span parent.
///
/// No global subscriber, tracer provider, sampler, or exporter is installed.
///
/// # Errors
///
/// Returns the bridge error when the span is disabled, already started, or has no compatible
/// `OpenTelemetry` layer.
pub fn set_parent(span: &Span, parent: &Context) -> Result<(), SetParentError> {
    span.set_parent(parent.clone())
}

#[cfg(test)]
mod tests {
    use opentelemetry::Context;
    use tracing::Span;

    use super::{current_context, set_parent, span_context};

    #[test]
    fn bridge_operations_are_explicit_for_disabled_spans() {
        let span = Span::none();
        let parent = Context::new();

        drop(span_context(&span));
        drop(current_context());
        assert!(set_parent(&span, &parent).is_err());
    }
}
