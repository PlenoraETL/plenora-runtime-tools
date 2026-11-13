//! Enumerate all production callable declarations and type construction surfaces.
use quote::ToTokens;
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};
use syn::{
    Item, Visibility,
    visit::{self, Visit},
};

#[derive(Default)]
struct Inventory {
    entries: Vec<String>,
    scope: Vec<String>,
}
impl Inventory {
    fn record(&mut self, tokens: impl ToTokens) {
        self.entries.push(format!(
            "{} | {}",
            self.scope.join("::"),
            tokens.into_token_stream()
        ));
    }
}
impl<'ast> Visit<'ast> for Inventory {
    fn visit_item(&mut self, item: &'ast Item) {
        // Unit-test modules and out-of-line test fixtures are not production entrypoints.
        if let Item::Mod(module) = item {
            if module.attrs.iter().any(|a| {
                a.path().is_ident("cfg") && a.meta.to_token_stream().to_string() == "cfg (test)"
            }) {
                return;
            }
            let mut header = module.clone();
            header.content = None;
            header.attrs.retain(|a| !a.path().is_ident("doc"));
            self.record(header);
            self.scope.push(module.ident.to_string());
            visit::visit_item(self, item);
            self.scope.pop();
        } else {
            visit::visit_item(self, item);
        }
    }
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.record(item.vis.to_token_stream());
        self.record(&item.sig);
        visit::visit_item_fn(self, item);
    }
    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let mut header = item.clone();
        header.items.clear();
        self.scope.push(header.into_token_stream().to_string());
        visit::visit_item_impl(self, item);
        self.scope.pop();
    }
    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.record(item.vis.to_token_stream());
        self.record(&item.sig);
        visit::visit_impl_item_fn(self, item);
    }
    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        let mut header = item.clone();
        header.items.clear();
        header.attrs.retain(|a| !a.path().is_ident("doc"));
        self.scope.push(header.into_token_stream().to_string());
        visit::visit_item_trait(self, item);
        self.scope.pop();
    }
    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        self.record(&item.sig);
    }
    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        if matches!(item.vis, Visibility::Public(_)) {
            let mut shape = item.clone();
            shape.attrs.retain(|a| !a.path().is_ident("doc"));
            for f in &mut shape.fields {
                f.attrs.retain(|a| !a.path().is_ident("doc"));
            }
            self.record(shape);
        }
    }
    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        self.record(item);
    }
    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        self.record(item);
    }
    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        let mut shape = item.clone();
        shape.attrs.retain(|a| !a.path().is_ident("doc"));
        for variant in &mut shape.variants {
            variant.attrs.retain(|a| !a.path().is_ident("doc"));
        }
        self.record(shape);
    }
    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        self.record(item);
    }
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        if matches!(item.vis, Visibility::Public(_)) {
            self.record(item);
        }
    }
}
fn files(path: &Path, result: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            files(&path, result)?;
        } else if path.extension().is_some_and(|v| v == "rs") {
            result.push(path);
        }
    }
    Ok(())
}
#[test]
fn all_callable_and_constructible_surfaces_match_reviewed_inventory() -> Result<(), Box<dyn Error>>
{
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut sources = Vec::new();
    for layer in ["crates", "adapters", "examples"] {
        for package in fs::read_dir(root.join(layer))? {
            let src = package?.path().join("src");
            if src.is_dir() {
                files(&src, &mut sources)?;
            }
        }
    }
    sources.sort();
    let mut entries = Vec::new();
    for source in sources {
        let mut inventory = Inventory::default();
        inventory.visit_file(&syn::parse_file(&fs::read_to_string(&source)?)?);
        let path = source
            .strip_prefix(&root)?
            .to_string_lossy()
            .replace('\\', "/");
        for entry in inventory.entries {
            entries.push(format!("{path} | {entry}"));
        }
    }
    entries.sort();
    let actual = entries.join("\n") + "\n";
    let snapshot = root.join("docs/boundary-api.txt");
    if std::env::var_os("UPDATE_BOUNDARY_API").is_some() {
        fs::write(&snapshot, &actual)?;
    }
    assert_eq!(
        actual,
        fs::read_to_string(snapshot)?,
        "API changed: collect/classify each new entrance and extend facade parity before reviewing the inventory"
    );
    Ok(())
}
#[test]
fn enumeration_detects_arbitrarily_named_new_entrypoint() -> Result<(), Box<dyn Error>> {
    let mut a = Inventory::default();
    let mut b = Inventory::default();
    a.visit_file(&syn::parse_file("pub struct Boxed { value: u8 }")?);
    b.visit_file(&syn::parse_file("pub struct Boxed { value: u8 } impl Boxed { pub fn unexpected(bytes: &[u8]) -> Self { Self { value: bytes[0] } } }")?);
    assert_ne!(a.entries, b.entries);
    Ok(())
}

#[test]
fn enumeration_captures_macro_enum_and_derive_constructors() -> Result<(), Box<dyn Error>> {
    for source in [
        "macro_rules! fabricate { () => { pub fn x() {} } } fabricate!();",
        "pub enum NewEntrance { Raw(Vec<u8>) }",
        "#[derive(Deserialize)] pub struct Admitted { bytes: Vec<u8> }",
    ] {
        let mut inventory = Inventory::default();
        inventory.visit_file(&syn::parse_file(source)?);
        assert!(!inventory.entries.is_empty());
    }
    Ok(())
}

#[test]
fn module_visibility_and_impl_macros_are_reviewed() -> Result<(), Box<dyn Error>> {
    let mut private = Inventory::default();
    let mut public = Inventory::default();
    private.visit_file(&syn::parse_file("mod inner { pub fn run() {} }")?);
    public.visit_file(&syn::parse_file("pub mod inner { pub fn run() {} }")?);
    assert_ne!(private.entries, public.entries);
    let mut macros = Inventory::default();
    macros.visit_file(&syn::parse_file("impl Admitted { fabricate!(); }")?);
    assert!(macros.entries.iter().any(|v| v.contains("fabricate")));
    Ok(())
}

#[test]
fn every_example_declares_its_boundary_profile() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    for entry in fs::read_dir(root.join("examples"))? {
        let manifest = fs::read_to_string(entry?.path().join("Cargo.toml"))?;
        assert!(
            manifest.contains("boundary-profile = \"outside-runtime-profile\"")
                || manifest.contains("boundary-profile = \"canonical-ingress\"")
        );
    }
    Ok(())
}
