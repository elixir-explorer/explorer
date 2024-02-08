use crate::ExplorerError;
use polars::datatypes::CategoricalOrdering;
use polars::datatypes::DataType;
use polars::datatypes::Field;
use polars::datatypes::TimeUnit;
use rustler::NifTaggedEnum;
use std::ops::Deref;

impl rustler::Encoder for Box<ExSeriesDtype> {
    fn encode<'a>(&self, env: rustler::Env<'a>) -> rustler::Term<'a> {
        let dtype: &ExSeriesDtype = self.deref();

        dtype.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for Box<ExSeriesDtype> {
    fn decode(term: rustler::Term<'a>) -> rustler::NifResult<Self> {
        let dtype: ExSeriesDtype = term.decode()?;
        Ok(Box::new(dtype))
    }
}

#[derive(NifTaggedEnum)]
pub enum ExTimeUnit {
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TryFrom<&ExTimeUnit> for TimeUnit {
    type Error = ExplorerError;

    fn try_from(value: &ExTimeUnit) -> Result<Self, Self::Error> {
        match value {
            ExTimeUnit::Millisecond => Ok(TimeUnit::Milliseconds),
            ExTimeUnit::Microsecond => Ok(TimeUnit::Microseconds),
            ExTimeUnit::Nanosecond => Ok(TimeUnit::Nanoseconds),
        }
    }
}

#[derive(NifTaggedEnum)]
pub enum ExSeriesDtype {
    Null,
    Binary,
    Boolean,
    Category,
    Date,
    F(u8),
    S(u8),
    U(u8),
    String,
    Time,
    Datetime(ExTimeUnit),
    Duration(ExTimeUnit),
    List(Box<ExSeriesDtype>),
    Struct(Vec<(String, ExSeriesDtype)>),
}

impl TryFrom<&DataType> for ExSeriesDtype {
    type Error = ExplorerError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Null => Ok(ExSeriesDtype::Null),
            DataType::Binary => Ok(ExSeriesDtype::Binary),
            DataType::Boolean => Ok(ExSeriesDtype::Boolean),
            DataType::Categorical(_, _) => Ok(ExSeriesDtype::Category),
            DataType::Date => Ok(ExSeriesDtype::Date),
            DataType::Float64 => Ok(ExSeriesDtype::F(64)),
            DataType::Float32 => Ok(ExSeriesDtype::F(32)),
            DataType::Int8 => Ok(ExSeriesDtype::S(8)),
            DataType::Int16 => Ok(ExSeriesDtype::S(16)),
            DataType::Int32 => Ok(ExSeriesDtype::S(32)),
            DataType::Int64 => Ok(ExSeriesDtype::S(64)),

            DataType::UInt8 => Ok(ExSeriesDtype::U(8)),
            DataType::UInt16 => Ok(ExSeriesDtype::U(16)),
            DataType::UInt32 => Ok(ExSeriesDtype::U(32)),
            DataType::UInt64 => Ok(ExSeriesDtype::U(64)),

            DataType::Time => Ok(ExSeriesDtype::Time),
            DataType::String => Ok(ExSeriesDtype::String),
            DataType::Datetime(TimeUnit::Nanoseconds, _) => {
                Ok(ExSeriesDtype::Datetime(ExTimeUnit::Nanosecond))
            }
            DataType::Datetime(TimeUnit::Microseconds, _) => {
                Ok(ExSeriesDtype::Datetime(ExTimeUnit::Microsecond))
            }
            DataType::Datetime(TimeUnit::Milliseconds, _) => {
                Ok(ExSeriesDtype::Datetime(ExTimeUnit::Millisecond))
            }

            DataType::Duration(TimeUnit::Nanoseconds) => {
                Ok(ExSeriesDtype::Duration(ExTimeUnit::Nanosecond))
            }
            DataType::Duration(TimeUnit::Microseconds) => {
                Ok(ExSeriesDtype::Duration(ExTimeUnit::Microsecond))
            }
            DataType::Duration(TimeUnit::Milliseconds) => {
                Ok(ExSeriesDtype::Duration(ExTimeUnit::Millisecond))
            }

            DataType::List(inner) => Ok(ExSeriesDtype::List(Box::new(Self::try_from(
                inner.as_ref(),
            )?))),

            DataType::Struct(fields) => {
                let mut struct_fields = Vec::new();

                for field in fields {
                    struct_fields
                        .push((field.name().to_string(), Self::try_from(field.data_type())?));
                }

                struct_fields.sort_by(|(a, _), (b, _)| a.cmp(b));

                Ok(ExSeriesDtype::Struct(struct_fields))
            }

            _ => Err(ExplorerError::Other(format!(
                "cannot cast to dtype: {value}"
            ))),
        }
    }
}

impl TryFrom<&ExSeriesDtype> for DataType {
    type Error = ExplorerError;

    fn try_from(value: &ExSeriesDtype) -> Result<Self, Self::Error> {
        match value {
            ExSeriesDtype::Null => Ok(DataType::Null),
            ExSeriesDtype::Binary => Ok(DataType::Binary),
            ExSeriesDtype::Boolean => Ok(DataType::Boolean),
            ExSeriesDtype::Category => {
                Ok(DataType::Categorical(None, CategoricalOrdering::default()))
            }
            ExSeriesDtype::Date => Ok(DataType::Date),
            ExSeriesDtype::F(64) => Ok(DataType::Float64),
            ExSeriesDtype::F(32) => Ok(DataType::Float32),
            ExSeriesDtype::F(size) => Err(ExplorerError::Other(format!(
                "float dtype of size {size} is not valid"
            ))),
            ExSeriesDtype::S(8) => Ok(DataType::Int8),
            ExSeriesDtype::S(16) => Ok(DataType::Int16),
            ExSeriesDtype::S(32) => Ok(DataType::Int32),
            ExSeriesDtype::S(64) => Ok(DataType::Int64),
            ExSeriesDtype::S(size) => Err(ExplorerError::Other(format!(
                "signed integer dtype of size {size} is not valid"
            ))),

            ExSeriesDtype::U(8) => Ok(DataType::UInt8),
            ExSeriesDtype::U(16) => Ok(DataType::UInt16),
            ExSeriesDtype::U(32) => Ok(DataType::UInt32),
            ExSeriesDtype::U(64) => Ok(DataType::UInt64),
            ExSeriesDtype::U(size) => Err(ExplorerError::Other(format!(
                "unsigned integer dtype of size {size} is not valid"
            ))),
            ExSeriesDtype::String => Ok(DataType::String),
            ExSeriesDtype::Time => Ok(DataType::Time),
            ExSeriesDtype::Datetime(ExTimeUnit::Nanosecond) => {
                Ok(DataType::Datetime(TimeUnit::Nanoseconds, None))
            }
            ExSeriesDtype::Datetime(ExTimeUnit::Microsecond) => {
                Ok(DataType::Datetime(TimeUnit::Microseconds, None))
            }
            ExSeriesDtype::Datetime(ExTimeUnit::Millisecond) => {
                Ok(DataType::Datetime(TimeUnit::Milliseconds, None))
            }
            ExSeriesDtype::Duration(ExTimeUnit::Nanosecond) => {
                Ok(DataType::Duration(TimeUnit::Nanoseconds))
            }
            ExSeriesDtype::Duration(ExTimeUnit::Microsecond) => {
                Ok(DataType::Duration(TimeUnit::Microseconds))
            }
            ExSeriesDtype::Duration(ExTimeUnit::Millisecond) => {
                Ok(DataType::Duration(TimeUnit::Milliseconds))
            }
            ExSeriesDtype::List(inner) => {
                Ok(DataType::List(Box::new(Self::try_from(inner.as_ref())?)))
            }
            ExSeriesDtype::Struct(fields) => Ok(DataType::Struct(
                fields
                    .iter()
                    .map(|(k, v)| Ok(Field::new(k.as_str(), v.try_into()?)))
                    .collect::<Result<Vec<Field>, Self::Error>>()?,
            )),
        }
    }
}
