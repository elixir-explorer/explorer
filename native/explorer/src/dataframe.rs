use polars::prelude::*;

use std::result::Result;

use crate::series::{to_ex_series_collection, to_series_collection};

use crate::{ExDataFrame, ExLazyFrame, ExSeries, ExplorerError};

macro_rules! df_read {
    ($data: ident, $df: ident, $body: block) => {
        match $data.resource.0.read() {
            Ok($df) => $body,
            Err(_) => Err(ExplorerError::Internal(
                "Failed to take read lock for df".into(),
            )),
        }
    };
}

macro_rules! df_read_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.resource.0.read(), $other.resource.0.read()) {
            (Ok($df), Ok($df1)) => $body,
            _ => Err(ExplorerError::Internal(
                "Failed to take read locks for left and right".into(),
            )),
        }
    };
}

#[rustler::nif]
pub fn df_join(
    data: ExDataFrame,
    other: ExDataFrame,
    left_on: Vec<&str>,
    right_on: Vec<&str>,
    how: &str,
) -> Result<ExDataFrame, ExplorerError> {
    let how = match how {
        "left" => JoinType::Left,
        "inner" => JoinType::Inner,
        "outer" => JoinType::Outer,
        "cross" => JoinType::Cross,
        _ => {
            return Err(ExplorerError::Other(format!(
                "Join method {} not supported",
                how
            )))
        }
    };

    df_read_read!(data, other, df, df1, {
        let new_df = df.join(&*df1, left_on, right_on, how, None)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_hstack(data: ExDataFrame, cols: Vec<ExSeries>) -> Result<ExDataFrame, ExplorerError> {
    let cols = to_series_collection(cols);
    df_read!(data, df, {
        let new_df = df.hstack(&cols)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_vstack(data: ExDataFrame, other: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    df_read_read!(data, other, df, df1, {
        Ok(ExDataFrame::new(df.vstack(&df1.clone())?))
    })
}

#[rustler::nif]
pub fn df_filter(data: ExDataFrame, mask: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let filter_series = &mask.resource.0;
        if let Ok(ca) = filter_series.bool() {
            let new_df = df.filter(ca)?;
            Ok(ExDataFrame::new(new_df))
        } else {
            Err(ExplorerError::Other("Expected a boolean mask".into()))
        }
    })
}

#[rustler::nif]
pub fn df_take(data: ExDataFrame, indices: Vec<u32>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let idx = UInt32Chunked::from_vec("idx", indices);
        let new_df = df.take(&idx)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_to_dummies(data: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.to_dummies()?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_with_column(data: ExDataFrame, col: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let mut new_df = df.clone();
        new_df.with_column(col.resource.0.clone())?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_new(cols: Vec<ExSeries>) -> Result<ExLazyFrame, ExplorerError> {
    let cols = to_series_collection(cols);
    let lf = DataFrame::new(cols)?.lazy();
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn df_set_column_names(
    data: ExDataFrame,
    names: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let mut new_df = df.clone();
        new_df.set_column_names(&names)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_groups(data: ExDataFrame, groups: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let groups = df.groupby(groups)?.groups()?;
        Ok(ExDataFrame::new(groups))
    })
}

#[rustler::nif]
pub fn df_groupby_agg(
    data: ExDataFrame,
    groups: Vec<&str>,
    aggs: Vec<(&str, Vec<&str>)>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.groupby(groups)?.agg(&aggs)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_pivot_wider(
    data: ExDataFrame,
    id_cols: Vec<&str>,
    pivot_column: &str,
    values_column: &str,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df
            .groupby(id_cols)?
            .pivot([pivot_column], [values_column])
            .first()?;
        Ok(ExDataFrame::new(new_df))
    })
}
