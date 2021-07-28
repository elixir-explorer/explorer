defmodule Explorer.PolarsBackend.Native do
  @moduledoc false

  use Rustler,
    otp_app: :explorer,
    crate: :explorer

  defstruct [:inner]

  def df_read_csv(
        _filename,
        _infer_schema_length,
        _has_header,
        _stop_after_n_rows,
        _skip_rows,
        _projection,
        _sep,
        _rechunk,
        _with_columns,
        _dtypes,
        _null_char,
        _encoding
      ),
      do: err()

  def df_to_csv_file(
        _df,
        _filename,
        _has_headers,
        _delimiter
      ),
      do: err()

  def df_as_str(_df), do: err()
  def df_cast(_df, _column, _dtype), do: err()
  def df_clone(_df), do: err()
  def df_column(_df, _name), do: err()
  def df_columns(_def), do: err()
  def df_drop(_df, _name), do: err()
  def df_drop_duplicates(_df, _maintain_order, _subset), do: err()
  def df_drop_nulls(_df, _subset), do: err()
  def df_dtypes(_df), do: err()
  def df_explode(_df, _cols), do: err()
  def df_fill_none(_df, _strategy), do: err()
  def df_filter(_df, _mask), do: err()
  def df_find_idx_by_name(_df, _name), do: err()
  def df_frame_equal(_df, _other, _null_equal), do: err()
  def df_get_columns(_df), do: err()
  def df_groups(_df, _colnames), do: err()
  def df_head(_df, _length), do: err()
  def df_height(_df), do: err()
  def df_hstack(_df, _cols), do: err()
  def df_is_duplicated(_df), do: err()
  def df_is_unique(_df), do: err()
  def df_join(_df, _other, _left_on, _right_on, _how), do: err()
  def df_max(_df), do: err()
  def df_mean(_df), do: err()
  def df_median(_df), do: err()
  def df_melt(_df, _id_vars, _value_vars), do: err()
  def df_min(_df), do: err()
  def df_n_chunks(_df), do: err()
  def df_new(_cols), do: err()
  def df_quantile(_df, _quant), do: err()
  def df_read_json(_filename, _type), do: err()
  def df_read_parquet(_filename), do: err()
  def df_replace(_df, _col, _new_col), do: err()
  def df_select(_df, _selection), do: err()
  def df_select_at_idx(_df, _idx), do: err()
  def df_set_column_names(_df, _names), do: err()
  def df_shape(_df), do: err()
  def df_shift(_df, _periods), do: err()
  def df_slice(_df, _offset, _length), do: err()
  def df_sort(_df, _by, _reverse), do: err()
  def df_stdev(_df), do: err()
  def df_sum(_df), do: err()
  def df_tail(_df, _length), do: err()
  def df_take(_df, _indices), do: err()
  def df_take_with_series(_df, _indices), do: err()
  def df_to_dummies(_df), do: err()
  def df_var(_df), do: err()
  def df_vstack(_df, _other), do: err()
  def df_width(_df), do: err()
  def df_with_column(_df, _col), do: err()

  # Series
  def s_add(_s, _other), do: err()
  def s_append(_s, _other), do: err()
  def s_arg_true(_s), do: err()
  def s_argsort(_s, _reverse), do: err()
  def s_as_str(_s), do: err()
  def s_cast(_s, _dtype), do: err()
  def s_clone(_s), do: err()
  def s_cum_max(_s, _reverse), do: err()
  def s_cum_min(_s, _reverse), do: err()
  def s_cum_sum(_s, _reverse), do: err()
  def s_div(_s, _other), do: err()
  def s_drop_nulls(_s), do: err()
  def s_dtype(_s), do: err()
  def s_eq(_s, _rhs), do: err()
  def s_explode(_s), do: err()
  def s_fill_none(_s, _strategy), do: err()
  def s_filter(_s, _filter), do: err()
  def s_get(_s, _idx), do: err()
  def s_gt(_s, _rhs), do: err()
  def s_gt_eq(_s, _rhs), do: err()
  def s_head(_s, _length), do: err()
  def s_is_duplicated(_s), do: err()
  def s_is_not_null(_s), do: err()
  def s_is_null(_s), do: err()
  def s_is_unique(_s), do: err()
  def s_len(_s), do: err()
  def s_limit(_s, _num_elem), do: err()
  def s_lt(_s, _rhs), do: err()
  def s_lt_eq(_s, _rhs), do: err()
  def s_max(_s), do: err()
  def s_mean(_s), do: err()
  def s_median(_s), do: err()
  def s_min(_s), do: err()
  def s_mul(_s, _other), do: err()
  def s_n_chunks(_s), do: err()
  def s_name(_s), do: err()
  def s_neq(_s, _rhs), do: err()
  def s_new_bool(_name, _val), do: err()
  def s_new_date32(_name, _val), do: err()
  def s_new_date64(_name, _val), do: err()
  def s_new_f64(_name, _val), do: err()
  def s_new_i64(_name, _val), do: err()
  def s_new_str(_name, _val), do: err()
  def s_new_u64(_name, _val), do: err()
  def s_not(_s), do: err()
  def s_null_count(_s), do: err()
  def s_peak_max(_s), do: err()
  def s_peak_min(_s), do: err()
  def s_pow(_s, _exponent), do: err()
  def s_quantile(_s, _quantile), do: err()
  def s_rechunk(_s), do: err()
  def s_rename(_s, _name), do: err()
  def s_reverse(_s), do: err()
  def s_rolling_max(_s, _window_size, _weight, _ignore_null, _min_periods), do: err()
  def s_rolling_mean(_s, _window_size, _weight, _ignore_null, _min_periods), do: err()
  def s_rolling_min(_s, _window_size, _weight, _ignore_null, _min_periods), do: err()
  def s_rolling_sum(_s, _window_size, _weight, _ignore_null, _min_periods), do: err()
  def s_seedable_random_indices(_length, _n_samples, _with_replacement, _seed), do: err()
  def s_series_equal(_s, _other, _null_equal), do: err()
  def s_shift(_s, _periods), do: err()
  def s_slice(_s, _offset, _length), do: err()
  def s_sort(_s, _reverse), do: err()
  def s_std(_s), do: err()
  def s_str_contains(_s, _pat), do: err()
  def s_str_lengths(_s), do: err()
  def s_str_parse_date32(_s, _fmt), do: err()
  def s_str_parse_date64(_s, _fmt), do: err()
  def s_str_replace(_s, _pat, _val), do: err()
  def s_str_replace_all(_s, _pat, _val), do: err()
  def s_str_to_lowercase(_s), do: err()
  def s_str_to_uppercase(_s), do: err()
  def s_sub(_s, _other), do: err()
  def s_sum(_s), do: err()
  def s_tail(_s, _length), do: err()
  def s_take(_s, _indices), do: err()
  def s_take_every(_s, _n), do: err()
  def s_to_dummies(_s), do: err()
  def s_to_list(_s), do: err()
  def s_unique(_s), do: err()
  def s_value_counts(_s), do: err()
  def s_var(_s), do: err()
  def s_zip_with(_s, _mask, _other), do: err()
  def s_n_unique(_s), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
