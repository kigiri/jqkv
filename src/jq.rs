use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Filter, Native};
use jaq_json::Val;

pub fn compile(code: &str, arena: &Arena) -> Result<Filter<Native<Val>>, String> {
    let modules = Loader::new(jaq_std::defs().chain(jaq_json::defs()))
        .load(&arena, File { code, path: "main" })
        .map_err(|e| format!("unable to load: {:?}", e))?;

    let filter = Compiler::default()
        .with_funs(jaq_std::funs().chain(jaq_json::funs()))
        .compile(modules)
        .map_err(|e| format!("invalid query: {:?}", e))?;

    Ok(filter)
}
