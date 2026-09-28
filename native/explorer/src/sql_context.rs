use crate::{ExDataFrame, ExLazyFrame, ExplorerError};
use polars::{prelude::IntoLazy, sql::SQLContext};
use rustler::{NifStruct, Resource, ResourceArc};
use std::sync::{Arc, Mutex};
pub struct ExSQLContextRef(pub Arc<Mutex<SQLContext>>);

#[rustler::resource_impl]
impl Resource for ExSQLContextRef {}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.SQLContext"]
pub struct ExSQLContext {
    pub resource: ResourceArc<ExSQLContextRef>,
}

impl ExSQLContextRef {
    pub fn new(ctx: SQLContext) -> Self {
        Self(Arc::new(Mutex::new(ctx)))
    }
}

impl ExSQLContext {
    pub fn new(ctx: SQLContext) -> Self {
        Self {
            resource: ResourceArc::new(ExSQLContextRef::new(ctx)),
        }
    }

    // Function to get a lock on the inner SQLContext
    pub fn lock_inner(&self) -> std::sync::MutexGuard<SQLContext> {
        self.resource.0.lock().unwrap()
    }
}

#[rustler::nif]
fn sql_context_new() -> ExSQLContext {
    let ctx = SQLContext::new();
    ExSQLContext::new(ctx)
}

#[rustler::nif]
fn sql_context_register(context: ExSQLContext, name: &str, df: ExDataFrame) {
    let mut ctx = context.lock_inner();
    let ldf = df.clone_inner().lazy();
    ctx.register(name, ldf)
}

#[rustler::nif]
fn sql_context_unregister(context: ExSQLContext, name: &str) {
    let mut ctx = context.lock_inner();
    ctx.unregister(name)
}

#[rustler::nif]
fn sql_context_execute(context: ExSQLContext, query: &str) -> Result<ExLazyFrame, ExplorerError> {
    let mut ctx = context.lock_inner();
    match ctx.execute(query) {
        Ok(lazy_frame) => Ok(ExLazyFrame::new(lazy_frame)),
        Err(e) => Err(ExplorerError::Other(format!(
            "Failed to execute query: {}",
            e
        ))),
    }
}

#[rustler::nif]
fn sql_context_get_tables(context: ExSQLContext) -> Vec<String> {
    let ctx = context.lock_inner();
    ctx.get_tables()
}
