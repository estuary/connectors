use anyhow::Context;
use anyhow::Result;
use arrow::array::Array;
use arrow::array::BooleanArray;
use arrow::array::Float64Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use tuple::Element;

/// A column bound's value in native Rust form, used both to compute written
/// `lower_bounds`/`upper_bounds` (`src/transactor.rs`) and to compare against
/// requested keys when deciding whether a file or row group can be skipped
/// (`src/iceberg/scan.rs`). Comparisons only ever happen between bounds
/// derived from the same column (and therefore the same variant); ordering
/// across different variants is never meaningful and shouldn't occur.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Bound {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8(String),
}

impl Bound {
    /// Encodes per the Iceberg spec's single-value binary serialization
    /// (<https://iceberg.apache.org/spec/#appendix-d-single-value-serialization>),
    /// for the column types this connector supports.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Bound::Int64(v) => v.to_le_bytes().to_vec(),
            Bound::Float64(v) => v.to_le_bytes().to_vec(),
            Bound::Boolean(v) => vec![u8::from(*v)],
            Bound::Utf8(v) => v.as_bytes().to_vec(),
        }
    }

    /// Decodes bytes previously written by `encode`, interpreted per
    /// `data_type`.
    pub fn decode(data_type: &DataType, bytes: &[u8]) -> Result<Bound> {
        Ok(match data_type {
            DataType::Int64 => Bound::Int64(i64::from_le_bytes(
                bytes.try_into().context("decoding int64 bound")?,
            )),
            DataType::Float64 => Bound::Float64(f64::from_le_bytes(
                bytes.try_into().context("decoding float64 bound")?,
            )),
            DataType::Boolean => Bound::Boolean(bytes.first().is_some_and(|b| *b != 0)),
            _ => Bound::Utf8(String::from_utf8(bytes.to_vec()).context("decoding utf8 bound")?),
        })
    }

    /// Coerces a decoded key-tuple element into the bound representation it
    /// would have taken on if stored in a column of `data_type`, mirroring
    /// `ColumnBuilder::append`'s coercion rules so comparisons are
    /// consistent with what was actually written. Returns `None` for
    /// combinations `ColumnBuilder::append` would reject.
    pub fn from_element(data_type: &DataType, element: &Element) -> Option<Bound> {
        Some(match (data_type, element) {
            (DataType::Int64, Element::Int(v)) => Bound::Int64(*v),
            (DataType::Float64, Element::Float(v)) => Bound::Float64(*v as f64),
            (DataType::Float64, Element::Double(v)) => Bound::Float64(*v),
            (DataType::Boolean, Element::Bool(v)) => Bound::Boolean(*v),
            (DataType::Utf8, Element::String(v)) => Bound::Utf8(v.to_string()),
            (DataType::Utf8, Element::Bytes(v)) => {
                Bound::Utf8(String::from_utf8_lossy(v).to_string())
            }
            (DataType::Utf8, Element::Int(v)) => Bound::Utf8(v.to_string()),
            (DataType::Utf8, Element::Float(v)) => Bound::Utf8(v.to_string()),
            (DataType::Utf8, Element::Double(v)) => Bound::Utf8(v.to_string()),
            (DataType::Utf8, Element::Bool(v)) => Bound::Utf8(v.to_string()),
            _ => return None,
        })
    }

    /// Reads the value at `row` from an Arrow array of `data_type`, for
    /// comparing a scanned row's key column against a requested key's
    /// `Bound` (from `from_element`). Returns `None` for a null value.
    pub fn from_arrow_scalar(data_type: &DataType, array: &dyn Array, row: usize) -> Option<Bound> {
        if array.is_null(row) {
            return None;
        }
        Some(match data_type {
            DataType::Int64 => {
                Bound::Int64(array.as_any().downcast_ref::<Int64Array>()?.value(row))
            }
            DataType::Float64 => {
                Bound::Float64(array.as_any().downcast_ref::<Float64Array>()?.value(row))
            }
            DataType::Boolean => {
                Bound::Boolean(array.as_any().downcast_ref::<BooleanArray>()?.value(row))
            }
            _ => Bound::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()?
                    .value(row)
                    .to_string(),
            ),
        })
    }
}

/// Tracks a running [lower, upper] bound for one column as rows are
/// appended, for later encoding into a manifest entry's `lower_bounds`/
/// `upper_bounds`.
pub struct BoundTracker {
    data_type: DataType,
    bounds: Option<(Bound, Bound)>,
}

impl BoundTracker {
    pub fn new(data_type: &DataType) -> BoundTracker {
        BoundTracker {
            data_type: data_type.clone(),
            bounds: None,
        }
    }

    pub fn update(&mut self, element: &Element) {
        let Some(value) = Bound::from_element(&self.data_type, element) else {
            return;
        };
        self.bounds = Some(match self.bounds.take() {
            None => (value.clone(), value),
            Some((lower, upper)) => {
                let lower = if value < lower { value.clone() } else { lower };
                let upper = if value > upper { value } else { upper };
                (lower, upper)
            }
        });
    }

    /// Returns the encoded `(lower, upper)` bounds, if any non-null value
    /// was ever observed.
    pub fn encoded(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.bounds
            .as_ref()
            .map(|(lower, upper)| (lower.encode(), upper.encode()))
    }

    /// Clears the tracked bounds, for reuse across the next data file.
    pub fn reset(&mut self) {
        self.bounds = None;
    }
}
