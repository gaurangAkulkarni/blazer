use std::collections::HashMap;
use std::sync::Arc;

use arrow2::array::{BooleanArray, PrimitiveArray, Utf8Array};

use crate::compute::backend::global_backend;
use crate::dataframe::DataFrame;
use crate::dtype::DataType;
use crate::error::{BlazeError, Result};
use crate::expr::{AggFunc, BinaryOp, Expr, LitValue, StringOp};
use crate::lazy::LogicalPlan;
use crate::series::Series;

/// Physical plan executor: evaluates a LogicalPlan into a DataFrame.
pub struct PhysicalExecutor;

impl PhysicalExecutor {
    pub fn execute(plan: LogicalPlan) -> Result<DataFrame> {
        match plan {
            LogicalPlan::DataFrameScan { df, .. } => Ok(df),

            LogicalPlan::Filter { input, predicate } => {
                let df = Self::execute(*input)?;
                let mask_series = Self::eval_expr(&predicate, &df)?;
                let mask = mask_series.as_bool()?;
                df.filter(&mask.0)
            }

            LogicalPlan::Select { input, exprs } => {
                let df = Self::execute(*input)?;
                let mut columns = Vec::with_capacity(exprs.len());
                for expr in &exprs {
                    let series = Self::eval_expr(expr, &df)?;
                    columns.push(series);
                }
                DataFrame::new(columns)
            }

            LogicalPlan::WithColumns { input, exprs } => {
                let mut df = Self::execute(*input)?;
                for expr in &exprs {
                    let series = Self::eval_expr(expr, &df)?;
                    df = df.with_column(series)?;
                }
                Ok(df)
            }

            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => {
                let df = Self::execute(*input)?;
                df.sort(&by_column, options.descending)
            }

            LogicalPlan::GroupBy {
                input,
                keys,
                aggs,
            } => {
                let df = Self::execute(*input)?;
                Self::execute_group_by(&df, &keys, &aggs)
            }

            LogicalPlan::Join {
                left,
                right,
                left_on,
                right_on,
                join_type,
            } => {
                let left_df = Self::execute(*left)?;
                let right_df = Self::execute(*right)?;
                Self::execute_join(&left_df, &right_df, &left_on, &right_on, join_type)
            }

            LogicalPlan::Limit { input, n } => {
                let df = Self::execute(*input)?;
                Ok(df.head(n))
            }

            LogicalPlan::Distinct { input } => {
                // Simple distinct: not fully implemented, just pass through
                Self::execute(*input)
            }

            LogicalPlan::DatasetScan {
                root, format, projection, n_rows, ..
            } => {
                let ext = match format {
                    crate::dataset::FileFormat::Parquet => "parquet",
                    crate::dataset::FileFormat::Csv => "csv",
                    crate::dataset::FileFormat::Json => "json",
                };
                let files = crate::dataset::collect_files(std::path::Path::new(&root), ext);
                if files.is_empty() {
                    return Ok(DataFrame::empty());
                }

                let mut reader = crate::io::ParquetReader::from_path(&files[0])?;
                if let Some(proj) = projection {
                    reader = reader.with_projection(proj);
                }
                if let Some(n) = n_rows {
                    reader = reader.with_n_rows(n);
                }
                let mut result = reader.finish()?;

                for file in &files[1..] {
                    let reader = crate::io::ParquetReader::from_path(file)?;
                    let df = reader.finish()?;
                    result = result.vstack(&df)?;
                }

                Ok(result)
            }
        }
    }

    /// Evaluate an expression against a DataFrame, producing a Series.
    pub fn eval_expr(expr: &Expr, df: &DataFrame) -> Result<Series> {
        match expr {
            Expr::Column(name) => {
                let col = df.column(name)?;
                Ok(col.clone())
            }

            Expr::Literal(lit) => {
                let len = df.height().max(1);
                match lit {
                    LitValue::Int64(v) => Ok(Series::new_scalar_i64("literal", *v, len)),
                    LitValue::Float64(v) => Ok(Series::new_scalar_f64("literal", *v, len)),
                    LitValue::Utf8(v) => Ok(Series::new_scalar_str("literal", v, len)),
                    LitValue::Boolean(v) => Ok(Series::new_scalar_bool("literal", *v, len)),
                    LitValue::Null => {
                        Ok(Series::from_opt_i64("literal", vec![None; len]))
                    }
                }
            }

            Expr::BinaryExpr { left, op, right } => {
                let left_series = Self::eval_expr(left, df)?;
                let right_series = Self::eval_expr(right, df)?;
                Self::eval_binary(&left_series, *op, &right_series)
            }

            Expr::Agg { input, func } => {
                let series = Self::eval_expr(input, df)?;
                Self::eval_agg(&series, *func)
            }

            Expr::Alias { expr, name } => {
                let mut series = Self::eval_expr(expr, df)?;
                series.rename(name);
                Ok(series)
            }

            Expr::Sort { expr, options } => {
                let series = Self::eval_expr(expr, df)?;
                series.sort(options.descending)
            }

            Expr::StringExpr { input, op } => {
                let series = Self::eval_expr(input, df)?;
                match op {
                    StringOp::ToUppercase => series.str_to_uppercase(),
                    StringOp::ToLowercase => series.str_to_lowercase(),
                    StringOp::Contains(pattern) => series.str_contains(pattern),
                    _ => Err(BlazeError::InvalidOperation(
                        "Unsupported string operation".into(),
                    )),
                }
            }

            Expr::Rolling {
                input,
                func,
                window_size,
            } => {
                let series = Self::eval_expr(input, df)?;
                match func {
                    AggFunc::Mean => series.rolling_mean(*window_size),
                    _ => Err(BlazeError::InvalidOperation(
                        "Unsupported rolling function".into(),
                    )),
                }
            }

            Expr::Window {
                input,
                partition_by,
            } => {
                Self::eval_window(input, partition_by, df)
            }

            Expr::Not(e) => {
                let series = Self::eval_expr(e, df)?;
                let mask = series.as_bool()?;
                let result = arrow2::compute::boolean::not(&mask.0);
                Series::from_arrow(series.name(), Arc::new(result))
            }

            Expr::IsNull(e) => {
                let series = Self::eval_expr(e, df)?;
                let arr = series.to_array();
                let validity = arr.validity();
                let result = match validity {
                    Some(bitmap) => {
                        let values: Vec<bool> = (0..arr.len())
                            .map(|i| !bitmap.get_bit(i))
                            .collect();
                        BooleanArray::from_slice(values)
                    }
                    None => BooleanArray::from_slice(vec![false; arr.len()]),
                };
                Ok(Series::from_arrow(series.name(), Arc::new(result))?)
            }

            Expr::IsNotNull(e) => {
                let series = Self::eval_expr(e, df)?;
                let arr = series.to_array();
                let validity = arr.validity();
                let result = match validity {
                    Some(bitmap) => {
                        let values: Vec<bool> = (0..arr.len())
                            .map(|i| bitmap.get_bit(i))
                            .collect();
                        BooleanArray::from_slice(values)
                    }
                    None => BooleanArray::from_slice(vec![true; arr.len()]),
                };
                Ok(Series::from_arrow(series.name(), Arc::new(result))?)
            }

            Expr::Wildcard => {
                Err(BlazeError::InvalidOperation(
                    "Cannot evaluate wildcard expression directly".into(),
                ))
            }

            Expr::Cast { expr, dtype } => {
                let series = Self::eval_expr(expr, df)?;
                let arr = series.to_array();
                let target = dtype.to_arrow();
                let casted = arrow2::compute::cast::cast(arr.as_ref(), &target, Default::default())?;
                Series::from_arrow(series.name(), casted.into())
            }
        }
    }

    /// Evaluate binary operations using the global backend.
    fn eval_binary(left: &Series, op: BinaryOp, right: &Series) -> Result<Series> {
        let backend = global_backend();
        match op {
            BinaryOp::Add => backend.add(left, right),
            BinaryOp::Sub => backend.sub(left, right),
            BinaryOp::Mul => backend.mul(left, right),
            BinaryOp::Div => backend.div(left, right),
            BinaryOp::Mod => backend.modulo(left, right),
            BinaryOp::Eq => backend.eq_series(left, right),
            BinaryOp::NotEq => backend.neq_series(left, right),
            BinaryOp::Lt => backend.lt_series(left, right),
            BinaryOp::LtEq => backend.lte_series(left, right),
            BinaryOp::Gt => backend.gt_series(left, right),
            BinaryOp::GtEq => backend.gte_series(left, right),
            BinaryOp::And => backend.and_series(left, right),
            BinaryOp::Or => backend.or_series(left, right),
        }
    }

    /// Evaluate an aggregation function, returning a single-element Series.
    fn eval_agg(series: &Series, func: AggFunc) -> Result<Series> {
        let name = series.name().to_string();
        match func {
            AggFunc::Sum => {
                let v = series.sum_as_f64()?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Mean => {
                let v = series.mean_as_f64()?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Min => {
                let v = series.min_as_f64()?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Max => {
                let v = series.max_as_f64()?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Count => {
                Ok(Series::from_i64(&name, vec![series.count() as i64]))
            }
            AggFunc::NUnique => {
                let mut seen = std::collections::HashSet::new();
                let arr = series.to_array();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        seen.insert(Self::series_value_to_string(series, i));
                    }
                }
                Ok(Series::from_i64(&name, vec![seen.len() as i64]))
            }
            AggFunc::First => {
                if series.is_empty() {
                    return Err(BlazeError::InvalidOperation("Empty series".into()));
                }
                Ok(series.slice(0, 1))
            }
            AggFunc::Last => {
                if series.is_empty() {
                    return Err(BlazeError::InvalidOperation("Empty series".into()));
                }
                Ok(series.slice(series.len() - 1, 1))
            }
        }
    }

    /// Execute a group-by aggregation.
    fn execute_group_by(
        df: &DataFrame,
        keys: &[Expr],
        aggs: &[Expr],
    ) -> Result<DataFrame> {
        // Evaluate key columns
        let key_series: Vec<Series> = keys
            .iter()
            .map(|k| Self::eval_expr(k, df))
            .collect::<Result<Vec<_>>>()?;

        // Build groups using a hash map
        let n = df.height();
        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();

        for i in 0..n {
            let key: Vec<String> = key_series
                .iter()
                .map(|ks| {
                    let arr = ks.to_array();
                    if arr.is_null(i) {
                        return "<<NULL>>".to_string();
                    }
                    match ks.dtype() {
                        DataType::Int64 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                                .unwrap();
                            format!("{}", p.value(i))
                        }
                        DataType::Float64 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<PrimitiveArray<f64>>()
                                .unwrap();
                            format!("{}", p.value(i))
                        }
                        DataType::Utf8 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<Utf8Array<i32>>()
                                .unwrap();
                            p.value(i).to_string()
                        }
                        DataType::Boolean => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap();
                            format!("{}", p.value(i))
                        }
                        _ => format!("{:?}", i),
                    }
                })
                .collect();
            groups.entry(key).or_default().push(i);
        }

        // For each group, compute aggregations
        let group_count = groups.len();
        let mut result_columns: Vec<Series> = Vec::new();

        // Key columns in output
        let group_keys: Vec<Vec<String>> = groups.keys().cloned().collect();
        let group_indices: Vec<&Vec<usize>> = groups.values().collect();

        for (ki, ks) in key_series.iter().enumerate() {
            let values: Vec<String> = group_keys.iter().map(|gk| gk[ki].clone()).collect();
            let series = match ks.dtype() {
                DataType::Utf8 => {
                    let str_vals: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                    Series::from_str(ks.name(), str_vals)
                }
                DataType::Int64 => {
                    let int_vals: Vec<i64> = values
                        .iter()
                        .map(|s| s.parse::<i64>().unwrap_or(0))
                        .collect();
                    Series::from_i64(ks.name(), int_vals)
                }
                DataType::Float64 => {
                    let float_vals: Vec<f64> = values
                        .iter()
                        .map(|s| s.parse::<f64>().unwrap_or(0.0))
                        .collect();
                    Series::from_f64(ks.name(), float_vals)
                }
                _ => {
                    let str_vals: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                    Series::from_str(ks.name(), str_vals)
                }
            };
            result_columns.push(series);
        }

        // Agg columns
        for agg_expr in aggs {
            let (input_expr, func, alias) = Self::extract_agg(agg_expr)?;
            let source_series = Self::eval_expr(&input_expr, df)?;

            let mut agg_values: Vec<f64> = Vec::with_capacity(group_count);
            for indices in &group_indices {
                // Build group slice
                let idx_arr = PrimitiveArray::<u32>::from_vec(
                    indices.iter().map(|&i| i as u32).collect(),
                );
                let group_series = source_series.take(&idx_arr)?;
                let v = match func {
                    AggFunc::Sum => group_series.sum_as_f64()?,
                    AggFunc::Mean => group_series.mean_as_f64()?,
                    AggFunc::Min => group_series.min_as_f64()?,
                    AggFunc::Max => group_series.max_as_f64()?,
                    AggFunc::Count => group_series.count() as f64,
                    AggFunc::NUnique => {
                        let mut seen = std::collections::HashSet::new();
                        for i in 0..group_series.len() {
                            let arr = group_series.to_array();
                            if !arr.is_null(i) {
                                seen.insert(Self::series_value_to_string(&group_series, i));
                            }
                        }
                        seen.len() as f64
                    }
                    AggFunc::First => {
                        if group_series.is_empty() {
                            f64::NAN
                        } else {
                            group_series.cast_f64()?.as_f64()?.value(0)
                        }
                    }
                    AggFunc::Last => {
                        if group_series.is_empty() {
                            f64::NAN
                        } else {
                            let n = group_series.len();
                            group_series.cast_f64()?.as_f64()?.value(n - 1)
                        }
                    }
                };
                agg_values.push(v);
            }

            let col_name = alias.unwrap_or_else(|| source_series.name().to_string());
            result_columns.push(Series::from_f64(&col_name, agg_values));
        }

        DataFrame::new(result_columns)
    }

    /// Extract the input expression, aggregation function, and optional alias from an agg expression.
    fn extract_agg(expr: &Expr) -> Result<(Expr, AggFunc, Option<String>)> {
        match expr {
            Expr::Alias { expr, name } => {
                let (input, func, _) = Self::extract_agg(expr)?;
                Ok((input, func, Some(name.clone())))
            }
            Expr::Agg { input, func } => Ok((*input.clone(), *func, None)),
            _ => Err(BlazeError::InvalidOperation(format!(
                "Expected aggregation expression, got: {:?}",
                expr
            ))),
        }
    }

    /// Execute a join operation.
    fn execute_join(
        left: &DataFrame,
        right: &DataFrame,
        left_on: &[Expr],
        right_on: &[Expr],
        join_type: JoinType,
    ) -> Result<DataFrame> {
        // Only support single-column joins for now, and inner join
        if left_on.len() != 1 || right_on.len() != 1 {
            return Err(BlazeError::InvalidOperation(
                "Only single-column joins supported currently".into(),
            ));
        }

        let left_key_name = match &left_on[0] {
            Expr::Column(name) => name.clone(),
            _ => return Err(BlazeError::InvalidOperation("Join key must be a column".into())),
        };
        let right_key_name = match &right_on[0] {
            Expr::Column(name) => name.clone(),
            _ => return Err(BlazeError::InvalidOperation("Join key must be a column".into())),
        };

        let left_key = left.column(&left_key_name)?;
        let right_key = right.column(&right_key_name)?;

        // Build hash index on right
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();
        for i in 0..right_key.len() {
            let key = Self::series_value_to_string(right_key, i);
            right_index.entry(key).or_default().push(i);
        }

        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();

        match join_type {
            JoinType::Inner => {
                for i in 0..left_key.len() {
                    let key = Self::series_value_to_string(left_key, i);
                    if let Some(matches) = right_index.get(&key) {
                        for &j in matches {
                            left_indices.push(i as u32);
                            right_indices.push(j as u32);
                        }
                    }
                }
            }
            JoinType::Left => {
                for i in 0..left_key.len() {
                    let key = Self::series_value_to_string(left_key, i);
                    if let Some(matches) = right_index.get(&key) {
                        for &j in matches {
                            left_indices.push(i as u32);
                            right_indices.push(j as u32);
                        }
                    }
                    // For left join, if no match, we'd need null handling.
                    // Simplified: inner-like for now.
                }
            }
            _ => {
                return Err(BlazeError::InvalidOperation(format!(
                    "Join type {:?} not yet implemented",
                    join_type
                )));
            }
        }

        let left_idx = PrimitiveArray::<u32>::from_vec(left_indices);
        let right_idx = PrimitiveArray::<u32>::from_vec(right_indices);

        // Build result columns
        let mut columns = Vec::new();

        // All left columns
        for col in left.columns() {
            columns.push(col.take(&left_idx)?);
        }

        // Right columns except the join key
        for col in right.columns() {
            if col.name() != right_key_name {
                let mut taken = col.take(&right_idx)?;
                // If name conflicts with left, suffix with _right
                if left.schema().index_of(col.name()).is_some() {
                    taken.rename(&format!("{}_right", col.name()));
                }
                columns.push(taken);
            }
        }

        DataFrame::new(columns)
    }

    fn series_value_to_string(series: &Series, idx: usize) -> String {
        let arr = series.to_array();
        if arr.is_null(idx) {
            return "<<NULL>>".to_string();
        }
        match series.dtype() {
            DataType::Int64 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            DataType::Float64 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            DataType::Utf8 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap();
                p.value(idx).to_string()
            }
            DataType::Boolean => {
                let p = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            DataType::Int32 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            _ => format!("row_{}", idx),
        }
    }

    /// Evaluate a window function.
    fn eval_window(
        input: &Expr,
        partition_by: &[Expr],
        df: &DataFrame,
    ) -> Result<Series> {
        // Evaluate partition keys
        let partition_series: Vec<Series> = partition_by
            .iter()
            .map(|e| Self::eval_expr(e, df))
            .collect::<Result<Vec<_>>>()?;

        // Build partition groups
        let n = df.height();
        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let key: Vec<String> = partition_series
                .iter()
                .map(|s| Self::series_value_to_string(s, i))
                .collect();
            groups.entry(key).or_default().push(i);
        }

        // Extract the aggregation from input
        let (inner_expr, func) = match input {
            Expr::Agg { input, func } => (input.as_ref(), *func),
            _ => {
                return Err(BlazeError::InvalidOperation(
                    "Window function requires an aggregation expression".into(),
                ));
            }
        };

        let source = Self::eval_expr(inner_expr, df)?;

        // Compute per-partition aggregate and broadcast back
        let mut result_values: Vec<f64> = vec![0.0; n];
        for (_key, indices) in &groups {
            let idx_arr = PrimitiveArray::<u32>::from_vec(
                indices.iter().map(|&i| i as u32).collect(),
            );
            let group_series = source.take(&idx_arr)?;
            let agg_val = match func {
                AggFunc::Sum => group_series.sum_as_f64()?,
                AggFunc::Mean => group_series.mean_as_f64()?,
                AggFunc::Min => group_series.min_as_f64()?,
                AggFunc::Max => group_series.max_as_f64()?,
                AggFunc::Count => group_series.count() as f64,
                AggFunc::NUnique => {
                    let mut seen = std::collections::HashSet::new();
                    for i in 0..group_series.len() {
                        let arr = group_series.to_array();
                        if !arr.is_null(i) {
                            seen.insert(Self::series_value_to_string(&group_series, i));
                        }
                    }
                    seen.len() as f64
                }
                _ => {
                    return Err(BlazeError::InvalidOperation(
                        "Unsupported window function".into(),
                    ));
                }
            };
            for &idx in indices {
                result_values[idx] = agg_val;
            }
        }

        Ok(Series::from_f64(source.name(), result_values))
    }
}

/// Join type enum (re-exported from lazy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    Cross,
}
