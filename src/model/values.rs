/*!
 * Types in iceberg
 */

use anyhow::{ensure, Result};
use std::{
    collections::{BTreeMap, HashMap},
    ops::Index,
};

use serde::{Deserialize, Serialize};

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum IcebergValue {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    LongInt(i64),
    /// Stored as 4-byte little-endian
    Double(f32),
    /// Stored as 8-byte little-endian
    LongFloat(f64),
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(chrono::NaiveDate),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(chrono::NaiveTime),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    Timestamp(chrono::naive::NaiveDateTime),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    TimestampTZ(chrono::NaiveDateTime),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(uuid::Uuid),
    /// Binary value
    Fixed(usize, Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    #[serde(skip_serializing, skip_deserializing)]
    Decimal(Decimal),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    #[serde(skip_serializing, skip_deserializing)]
    Struct(Struct),
    /// A list is a collection of values with some element type.
    /// The element field has an integer id that is unique in the table schema.
    /// Elements can be either optional or required. Element types may be any type.
    #[serde(skip_serializing, skip_deserializing)]
    List(Vec<(Field, Option<String>)>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    /// Both the key field and value field each have an integer id that is unique in the table schema.
    /// Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types.
    #[serde(skip_serializing, skip_deserializing)]
    Map(HashMap<String, Field>),
}

/// Optional or required value
#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    /// Required value
    Required(IcebergValue),
    /// Optional value, can be null
    Optional(Option<IcebergValue>),
}

/// An iceberg struct in partition value
#[derive(Debug, Clone, PartialEq)]
pub struct Struct {
    fields: Vec<(Field, Option<String>)>,
    lookup: BTreeMap<String, usize>,
}

impl Struct {
    /// Get a reference to a struct field
    pub fn get(&self, name: &str) -> Option<&(Field, Option<String>)> {
        self.lookup.get(name).map(|index| &self.fields[*index])
    }
}

impl Index<usize> for Struct {
    type Output = (Field, Option<String>);

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

/// The decimal type
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Decimal {
    value: i128,
    /// The number of digits in the number. Must be 38 or less
    /// This must be calculated
    precision: u8,
    /// The number of digits to the right of the decimal point.
    /// This is part of the big_num
    scale: u32,
}

impl Decimal {
    /// Create a new Decimal object.
    pub fn new(value: i128, precision: u8, scale: u32) -> Result<Self> {
        // check that the provided value has the correct precision.
        ensure!(precision <= 38, "Precision {precision} must be 38 or less");
        ensure!(
            scale <= precision as u32,
            "Scale {scale} is greater the Precision {precision}"
        );
        Ok(Decimal {
            value,
            precision,
            scale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scale_gt_precision() {
        let dec = Decimal::new(123, 2, 3);
        assert!(dec.is_err());
    }

    #[test]
    fn test_scale_error_precision_gt_38() {
        let dec = Decimal::new(123, 39, 3);
        assert!(dec.is_err());
    }

    #[test]
    fn test_precision_less_than_bytes() {
        let dec = Decimal::new(123, 1, 3);
        assert!(dec.is_err());
    }
}
