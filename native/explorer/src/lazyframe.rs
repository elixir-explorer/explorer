use crate::{
    datatypes::ExSeriesDtype, expressions::ex_expr_to_exprs, ExDataFrame, ExExpr, ExLazyFrame,
    ExplorerError,
};
use polars::{lazy::dsl::Selector, prelude::*};

// Loads the IO functions for read/writing CSV, NDJSON, Parquet, etc.
pub mod io;

pub trait GroupByOptOrder {
    fn group_by_opt_order<G, IE>(self, groups: G, stable_groups: bool) -> LazyGroupBy
    where
        G: AsRef<[IE]>,
        IE: Into<Expr> + Clone;
}

impl GroupByOptOrder for LazyFrame {
    fn group_by_opt_order<G, IE>(self, groups: G, stable_groups: bool) -> LazyGroupBy
    where
        G: AsRef<[IE]>,
        IE: Into<Expr> + Clone,
    {
        if stable_groups {
            self.group_by_stable(groups.as_ref())
        } else {
            self.group_by(groups.as_ref())
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_compute(data: ExLazyFrame) -> Result<ExDataFrame, ExplorerError> {
    let lf = data.clone_inner();

    // FIXME: for some reason, without calling this line, it crashes when connecting to S3
    let _ = lf.clone().to_alp();

    let df = lf.collect()?;

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_fetch(data: ExLazyFrame, n_rows: usize) -> Result<ExDataFrame, ExplorerError> {
    Ok(ExDataFrame::new(data.clone_inner().fetch(n_rows)?))
}

#[rustler::nif]
pub fn lf_describe_plan(data: ExLazyFrame, optimized: bool) -> Result<String, ExplorerError> {
    let lf = data.clone_inner();
    let plan = match optimized {
        true => lf.describe_optimized_plan()?,
        false => lf.describe_plan().expect("error"),
    };
    Ok(plan)
}

#[rustler::nif]
pub fn lf_head(
    data: ExLazyFrame,
    length: u32,
    groups: Vec<ExExpr>,
    stable_groups: bool,
) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    let result_lf = if groups.is_empty() {
        lf.limit(length)
    } else {
        lf.group_by_opt_order(groups, stable_groups)
            .head(Some(length.try_into()?))
    };

    Ok(ExLazyFrame::new(result_lf))
}

#[rustler::nif]
pub fn lf_tail(
    data: ExLazyFrame,
    length: u32,
    groups: Vec<ExExpr>,
    stable_groups: bool,
) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    let result_lf = if groups.is_empty() {
        lf.tail(length)
    } else {
        lf.group_by_opt_order(groups, stable_groups)
            .tail(Some(length.try_into()?))
    };

    Ok(ExLazyFrame::new(result_lf))
}

#[rustler::nif]
pub fn lf_names(data: ExLazyFrame) -> Result<Vec<String>, ExplorerError> {
    let mut lf = data.clone_inner();
    let names = lf
        .collect_schema()?
        .iter_names()
        .map(|smart_string| smart_string.to_string())
        .collect();

    Ok(names)
}

#[rustler::nif]
pub fn lf_dtypes(data: ExLazyFrame) -> Result<Vec<ExSeriesDtype>, ExplorerError> {
    let mut dtypes: Vec<ExSeriesDtype> = vec![];
    let schema = data.clone_inner().collect_schema()?;

    for (_name, dtype) in schema.iter_names_and_dtypes() {
        dtypes.push(ExSeriesDtype::try_from(dtype)?)
    }

    Ok(dtypes)
}

#[rustler::nif]
pub fn lf_select(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().select(&[cols(columns)]);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_drop(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().select(&[col("*").exclude(columns)]);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_slice(
    data: ExLazyFrame,
    offset: i64,
    length: u32,
    groups: Vec<String>,
    stable_groups: bool,
) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    let result_lf = if groups.is_empty() {
        lf.slice(offset, length)
    } else {
        let groups_exprs: Vec<Expr> = groups.iter().map(col).collect();
        lf.group_by_opt_order(groups_exprs, stable_groups)
            .agg([col("*").slice(offset, length)])
            .explode([col("*").exclude(groups)])
    };

    Ok(ExLazyFrame::new(result_lf))
}

#[rustler::nif]
pub fn lf_explode(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().explode(columns);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_unnest(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().unnest(columns);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_filter_with(data: ExLazyFrame, ex_expr: ExExpr) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    let expr = ex_expr.clone_inner();

    Ok(ExLazyFrame::new(lf.filter(expr)))
}

#[rustler::nif]
pub fn lf_sort_with(
    data: ExLazyFrame,
    expressions: Vec<ExExpr>,
    directions: Vec<bool>,
    maintain_order: bool,
    nulls_last: bool,
) -> Result<ExLazyFrame, ExplorerError> {
    let exprs = ex_expr_to_exprs(expressions);
    let sort_options = SortMultipleOptions::new()
        .with_nulls_last(nulls_last)
        .with_maintain_order(maintain_order)
        .with_order_descending_multi(directions);

    let ldf = data.clone_inner().sort_by_exprs(exprs, sort_options);

    Ok(ExLazyFrame::new(ldf))
}

#[rustler::nif]
pub fn lf_grouped_sort_with(
    data: ExLazyFrame,
    expressions: Vec<ExExpr>,
    groups: Vec<ExExpr>,
    directions: Vec<bool>,
) -> Result<ExLazyFrame, ExplorerError> {
    let sort_options = SortMultipleOptions::new().with_order_descending_multi(directions);
    // For grouped lazy frames, we need to use the `#sort_by` method that is
    // less powerful, but can be used with `over`.
    // See: https://docs.pola.rs/user-guide/expressions/window/#operations-per-group
    let ldf = data
        .clone_inner()
        .with_columns([col("*").sort_by(expressions, sort_options).over(groups)]);

    Ok(ExLazyFrame::new(ldf))
}

#[rustler::nif]
pub fn lf_distinct(
    data: ExLazyFrame,
    subset: Vec<String>,
    columns_to_keep: Option<Vec<ExExpr>>,
) -> Result<ExLazyFrame, ExplorerError> {
    let df = data.clone_inner();
    let subset = subset.iter().map(|x| x.into()).collect::<Vec<PlSmallStr>>();
    let new_df = df.unique_stable(Some(subset), UniqueKeepStrategy::First);

    match columns_to_keep {
        Some(columns) => Ok(ExLazyFrame::new(new_df.select(ex_expr_to_exprs(columns)))),
        None => Ok(ExLazyFrame::new(new_df)),
    }
}

#[rustler::nif]
pub fn lf_mutate_with(
    data: ExLazyFrame,
    columns: Vec<ExExpr>,
) -> Result<ExLazyFrame, ExplorerError> {
    let mutations = ex_expr_to_exprs(columns);
    // Maybe there is a bug when some expressions are in use without this "comm_subexpr_elim"
    // turned off.
    let ldf = data
        .clone_inner()
        .with_comm_subexpr_elim(false)
        .with_columns(mutations);

    Ok(ExLazyFrame::new(ldf))
}

#[rustler::nif]
pub fn lf_summarise_with(
    data: ExLazyFrame,
    groups: Vec<ExExpr>,
    stable_groups: bool,
    aggs: Vec<ExExpr>,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let groups = ex_expr_to_exprs(groups);
    let aggs = ex_expr_to_exprs(aggs);

    let new_lf = if groups.is_empty() {
        // We do add a "shadow" column to be able to group by it.
        // This is going to force some aggregations like "mode" to be always inside
        // a "list".
        ldf.group_by_opt_order(
            [1.lit().alias("__explorer_literal_for_group__")],
            stable_groups,
        )
        .agg(aggs)
        .select(&[col("*").exclude(["__explorer_literal_for_group__"])])
    } else {
        ldf.group_by_opt_order(groups, stable_groups).agg(aggs)
    };

    Ok(ExLazyFrame::new(new_lf))
}

#[rustler::nif]
pub fn lf_rename_columns(
    data: ExLazyFrame,
    renames: Vec<(&str, &str)>,
) -> Result<ExLazyFrame, ExplorerError> {
    let df = data.clone_inner();
    let (existing, new): (Vec<_>, Vec<_>) = renames.iter().cloned().unzip();

    Ok(ExLazyFrame::new(df.rename(existing, new, true)))
}

#[rustler::nif]
pub fn lf_drop_nils(
    data: ExLazyFrame,
    subset: Option<Vec<ExExpr>>,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let columns = subset.map(ex_expr_to_exprs);

    Ok(ExLazyFrame::new(ldf.drop_nulls(columns)))
}

#[rustler::nif]
pub fn lf_pivot_longer(
    data: ExLazyFrame,
    id_vars: Vec<String>,
    value_vars: Vec<String>,
    names_to: String,
    values_to: String,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let unpivot_opts = polars::lazy::dsl::UnpivotArgsDSL {
        index: to_lazy_selectors(id_vars),
        on: to_lazy_selectors(value_vars),
        variable_name: Some(names_to.into()),
        value_name: Some(values_to.into()),
    };
    let new_df = ldf.unpivot(unpivot_opts);
    Ok(ExLazyFrame::new(new_df))
}

fn to_lazy_selectors(values: Vec<String>) -> Vec<Selector> {
    values
        .into_iter()
        .map(Selector::from)
        .collect::<Vec<Selector>>()
}

#[rustler::nif]
pub fn lf_join(
    data: ExLazyFrame,
    other: ExLazyFrame,
    left_on: Vec<ExExpr>,
    right_on: Vec<ExExpr>,
    how: &str,
    suffix: &str,
) -> Result<ExLazyFrame, ExplorerError> {
    let how = match how {
        "left" => JoinType::Left,
        "inner" => JoinType::Inner,
        "outer" => JoinType::Full,
        "cross" => JoinType::Cross,
        _ => {
            return Err(ExplorerError::Other(format!(
                "Join method {how} not supported"
            )))
        }
    };

    let ldf = data.clone_inner();
    let ldf1 = other.clone_inner();

    let new_ldf = match how {
        // Cross-joins no longer accept keys.
        // https://github.com/pola-rs/polars/pull/17305
        JoinType::Cross => ldf
            .join_builder()
            .with(ldf1)
            .how(JoinType::Cross)
            .suffix(suffix)
            .finish(),
        _ => ldf
            .join_builder()
            .with(ldf1)
            .how(how)
            .left_on(ex_expr_to_exprs(left_on))
            .right_on(ex_expr_to_exprs(right_on))
            .suffix(suffix)
            .finish(),
    };

    Ok(ExLazyFrame::new(new_ldf))
}

#[allow(clippy::too_many_arguments)]
#[rustler::nif]
pub fn lf_join_asof(
    data: ExLazyFrame,
    other: ExLazyFrame,
    left_on: Vec<ExExpr>,
    right_on: Vec<ExExpr>,
    by_left: Vec<&str>,
    by_right: Vec<&str>,
    strategy: &str,
    suffix: &str,
) -> Result<ExLazyFrame, ExplorerError> {
    let strategy = match strategy {
        "backward" => AsofStrategy::Backward,
        "forward" => AsofStrategy::Forward,
        "nearest" => AsofStrategy::Nearest,
        _ => {
            return Err(ExplorerError::Other(format!(
                "AsOfJoin method {strategy} not supported"
            )))
        }
    };

    let left_by = match by_left.is_empty() {
        true => None,
        false => Some(by_left.iter().map(|l| PlSmallStr::from_str(l)).collect()),
    };

    let right_by = match by_right.is_empty() {
        true => None,
        false => Some(by_left.iter().map(|l| PlSmallStr::from_str(l)).collect()),
    };

    let ldf = data.clone_inner();
    let ldf1 = other.clone_inner();

    let new_ldf = ldf
        .join_builder()
        .with(ldf1)
        .coalesce(JoinCoalesce::CoalesceColumns)
        .how(JoinType::AsOf(AsOfOptions {
            strategy,
            tolerance: None,
            tolerance_str: None,
            left_by: left_by,
            right_by: right_by,
            allow_eq: true,
            check_sortedness: true,
        }))
        .left_on(ex_expr_to_exprs(left_on))
        .right_on(ex_expr_to_exprs(right_on))
        .suffix(suffix)
        .finish();

    Ok(ExLazyFrame::new(new_ldf))
}

#[rustler::nif]
pub fn lf_concat_rows(lazy_frames: Vec<ExLazyFrame>) -> Result<ExLazyFrame, ExplorerError> {
    let inputs: Vec<LazyFrame> = lazy_frames.iter().map(|lf| lf.clone_inner()).collect();
    let union_args = UnionArgs::default();
    let out_df = concat(inputs, union_args)?;

    Ok(ExLazyFrame::new(out_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_concat_columns(ldfs: Vec<ExLazyFrame>) -> Result<ExLazyFrame, ExplorerError> {
    let mut previous_names = PlHashSet::new();

    let renamed_ldfs: Vec<LazyFrame> = ldfs
        .iter()
        .enumerate()
        .map(|(idx, ex_ldf)| {
            let mut ldf = ex_ldf.clone_inner();
            let names: Vec<String> = ldf
                .collect_schema()
                .expect("should be able to get schema")
                .iter_names()
                .map(|smart_string| smart_string.to_string())
                .collect();

            let mut substitutions = vec![];

            for name in names {
                if previous_names.contains(&name) {
                    let new_name = format!("{name}_{idx}");
                    previous_names.insert(new_name.clone());
                    substitutions.push((name, new_name))
                } else {
                    previous_names.insert(name.clone());
                }
            }

            if substitutions.is_empty() {
                ldf
            } else {
                let (existing, new): (Vec<String>, Vec<String>) =
                    substitutions.iter().cloned().unzip();
                ldf.rename(existing, new, true)
            }
        })
        .collect();

    let out_ldf = concat_lf_horizontal(renamed_ldfs, UnionArgs::default())?;

    Ok(ExLazyFrame::new(out_ldf))
}

#[rustler::nif]
pub fn lf_sql(
    lf: ExLazyFrame,
    sql_string: &str,
    table_name: &str,
) -> Result<ExLazyFrame, ExplorerError> {
    let mut ctx = polars::sql::SQLContext::new();

    let lf = lf.clone_inner();
    ctx.register(table_name, lf);

    match ctx.execute(sql_string) {
        Ok(lf_sql) => Ok(ExLazyFrame::new(lf_sql)),
        Err(polars_error) => Err(ExplorerError::Polars(polars_error)),
    }
}
