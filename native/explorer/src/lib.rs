// MiMalloc won´t compile on Windows with the GCC compiler
#[cfg(not(all(windows, target_env = "gnu")))]
use mimalloc::MiMalloc;
use rustler::{Env, Term};

#[cfg(not(all(windows, target_env = "gnu")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod dataframe;
mod datatypes;
mod error;
mod lazyframe;
mod series;

pub use datatypes::{
    ExAnyValue, ExDataFrame, ExDataFrameRef, ExLazyFrame, ExLazyFrameRef, ExSeries, ExSeriesRef,
};
pub use error::ExplorerError;
use lazyframe::*;
use series::*;

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(ExDataFrameRef, env);
    rustler::resource!(ExLazyFrameRef, env);
    rustler::resource!(ExSeriesRef, env);
    true
}

mod atoms {
    rustler::atoms! {
        date = "Elixir.Date",
        datetime = "Elixir.NaiveDateTime",
        calendar = "Elixir.Calendar.ISO"
    }
}

rustler::init!(
    "Elixir.Explorer.PolarsBackend.Native",
    [
        // lazyframe
        lf_as_str,
        lf_collect,
        lf_column_dtypes,
        lf_column_names,
        lf_describe_plan,
        lf_drop,
        lf_drop_nulls,
        lf_fetch,
        lf_from_keyword_rows,
        lf_from_map_rows,
        lf_get_column,
        lf_get_columns,
        lf_height,
        lf_read_csv,
        lf_read_parquet,
        lf_select,
        lf_shape,
        lf_tail,
        lf_to_csv_string,
        lf_width,
        lf_write_csv,
        lf_write_parquet,
        lf_new,
        lf_distinct,
        lf_melt,
        lf_sort,
        lf_drop_nulls,
        // series
        s_add,
        s_and,
        s_append,
        s_arg_true,
        s_argsort,
        s_as_str,
        s_cast,
        s_clone,
        s_cum_max,
        s_cum_min,
        s_cum_sum,
        s_distinct,
        s_div,
        s_drop_nulls,
        s_dtype,
        s_eq,
        s_explode,
        s_fill_none,
        s_fill_none_with_int,
        s_fill_none_with_float,
        s_fill_none_with_bin,
        s_filter,
        s_get,
        s_gt,
        s_gt_eq,
        s_head,
        s_is_duplicated,
        s_is_not_null,
        s_is_null,
        s_is_unique,
        s_len,
        s_limit,
        s_lt,
        s_lt_eq,
        s_max,
        s_mean,
        s_median,
        s_min,
        s_mul,
        s_n_chunks,
        s_n_unique,
        s_name,
        s_neq,
        s_new_bool,
        s_new_date32,
        s_new_date64,
        s_new_f64,
        s_new_i64,
        s_new_str,
        s_not,
        s_null_count,
        s_or,
        s_peak_max,
        s_peak_min,
        s_pow,
        s_int_pow,
        s_quantile,
        s_rechunk,
        s_rename,
        s_reverse,
        s_rolling_max,
        s_rolling_mean,
        s_rolling_min,
        s_rolling_sum,
        s_seedable_random_indices,
        s_series_equal,
        s_shift,
        s_slice,
        s_sort,
        s_std,
        s_str_contains,
        s_str_lengths,
        s_str_parse_date32,
        s_str_parse_date64,
        s_str_replace,
        s_str_replace_all,
        s_str_to_lowercase,
        s_str_to_uppercase,
        s_sub,
        s_sum,
        s_tail,
        s_take,
        s_take_every,
        s_to_dummies,
        s_to_list,
        s_unordered_distinct,
        s_var,
        s_value_counts,
        s_zip_with,
    ],
    load = on_load
);
