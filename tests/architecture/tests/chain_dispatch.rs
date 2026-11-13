//! Guard the concrete chain executor and every production call into domain adapters.
use quote::ToTokens;
use std::{error::Error, fs, path::Path};
use syn::{
    Item, Type,
    visit::{self, Visit},
};

#[derive(Default)]
struct Invocations {
    owner: String,
    adapter_calls: Vec<String>,
    engine_calls: Vec<String>,
}
impl<'ast> Visit<'ast> for Invocations {
    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let previous = self.owner.clone();
        self.owner = item.self_ty.to_token_stream().to_string();
        visit::visit_item_impl(self, item);
        self.owner = previous;
    }
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let receiver = call.receiver.to_token_stream().to_string();
        if receiver == "self . adapter"
            && matches!(
                call.method.to_string().as_str(),
                "invoke" | "recover" | "request_cancel"
            )
        {
            self.adapter_calls.push(self.owner.clone());
        }
        if receiver == "self . engine" && call.method == "execute" {
            self.engine_calls.push(self.owner.clone());
        }
        visit::visit_expr_method_call(self, call);
    }
}
fn inspect(source: &str) -> Result<Invocations, syn::Error> {
    let mut calls = Invocations::default();
    calls.visit_file(&syn::parse_file(source)?);
    Ok(calls)
}
#[test]
fn chain_engine_requires_the_concrete_durable_executor() -> Result<(), Box<dyn Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let engine = fs::read_to_string(root.join("crates/plenora-runtime-chains/src/engine.rs"))?;
    let parsed = syn::parse_file(&engine)?;
    let structure = parsed
        .items
        .iter()
        .find_map(|item| match item {
            Item::Struct(s) if s.ident == "ChainEngine" => Some(s),
            _ => None,
        })
        .ok_or("engine")?;
    let executor = structure
        .fields
        .iter()
        .find(|f| f.ident.as_ref().is_some_and(|i| i == "executor"))
        .ok_or("concrete executor")?;
    assert!(
        matches!(&executor.ty,Type::Path(p) if p.path.segments.last().is_some_and(|s|s.ident=="StepExecutor"))
    );
    assert!(matches!(executor.vis, syn::Visibility::Inherited));
    assert!(
        !structure
            .fields
            .iter()
            .any(|f| f.ident.as_ref().is_some_and(|i| i == "adapter"))
    );
    assert!(
        !structure
            .generics
            .params
            .iter()
            .any(|p| matches!(p,syn::GenericParam::Type(t) if t.ident=="A"))
    );
    assert!(
        structure
            .generics
            .to_token_stream()
            .to_string()
            .contains("DurableExecutionStore")
    );
    assert!(inspect(&engine)?.adapter_calls.is_empty());
    let execution =
        fs::read_to_string(root.join("crates/plenora-runtime-chains/src/execution.rs"))?;
    assert_eq!(
        inspect(&execution)?.adapter_calls,
        vec!["Component < A >"; 3]
    );
    assert!(execution.contains("impl<A: Adapter> EvidenceComponent for Component<A>"));
    assert!(execution.contains("process_confirmed"));
    let host = fs::read_to_string(root.join("crates/plenora-runtime-chain-host/src/lib.rs"))?;
    assert!(inspect(&host)?.engine_calls.is_empty());
    assert!(inspect(&host)?.adapter_calls.is_empty());
    let control =
        fs::read_to_string(root.join("crates/plenora-runtime-chain-host/src/control.rs"))?;
    assert_eq!(inspect(&control)?.engine_calls, vec!["Component < S >"]);
    Ok(())
}
#[test]
fn guard_detects_a_direct_adapter_bypass() -> Result<(), Box<dyn Error>> {
    let source = "impl<S,A> ChainEngine<S,A> { async fn bypass(&self, i: &Invocation) { self.adapter.invoke(i).await; } }";
    assert_eq!(
        inspect(source)?.adapter_calls,
        vec!["ChainEngine < S , A >"]
    );
    Ok(())
}
