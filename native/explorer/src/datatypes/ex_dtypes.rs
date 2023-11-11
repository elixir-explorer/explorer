use crate::ExplorerError;
use polars::datatypes::DataType;
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

#[derive(NifTaggedEnum)]
pub enum ExSeriesDtype {
    Binary,
    Boolean,
    Category,
    Date,
    Float,
    Integer,
    String,
    Time,
    Datetime(ExTimeUnit),
    Duration(ExTimeUnit),
    List(Box<ExSeriesDtype>),
}

impl TryFrom<&DataType> for ExSeriesDtype {
    type Error = ExplorerError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Binary => Ok(ExSeriesDtype::Binary),
            DataType::Boolean => Ok(ExSeriesDtype::Boolean),
            DataType::Categorical(_) => Ok(ExSeriesDtype::Category),
            DataType::Date => Ok(ExSeriesDtype::Date),
            DataType::Float64 => Ok(ExSeriesDtype::Float),
            DataType::Int64 => Ok(ExSeriesDtype::Integer),
            DataType::Time => Ok(ExSeriesDtype::Time),
            DataType::Utf8 => Ok(ExSeriesDtype::String),
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
            ExSeriesDtype::Binary => Ok(DataType::Binary),
            ExSeriesDtype::Boolean => Ok(DataType::Boolean),
            ExSeriesDtype::Category => Ok(DataType::Categorical(None)),
            ExSeriesDtype::Date => Ok(DataType::Date),
            ExSeriesDtype::Float => Ok(DataType::Float64),
            ExSeriesDtype::Integer => Ok(DataType::Int64),
            ExSeriesDtype::String => Ok(DataType::Utf8),
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
        }
    }
}

// Unsigned, Signed, Float
#[derive(NifTaggedEnum)]
pub enum ExSeriesIoType {
    U(u8),
    S(u8),
    F(u8),
}

impl TryFrom<&DataType> for ExSeriesIoType {
    type Error = ExplorerError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Boolean => Ok(ExSeriesIoType::U(8)),
            DataType::UInt8 => Ok(ExSeriesIoType::U(8)),
            DataType::UInt32 => Ok(ExSeriesIoType::U(32)),
            DataType::Int32 => Ok(ExSeriesIoType::S(32)),
            DataType::Int64 => Ok(ExSeriesIoType::S(64)),
            DataType::Float64 => Ok(ExSeriesIoType::F(64)),
            DataType::Date => Ok(ExSeriesIoType::S(32)),
            DataType::Datetime(_, _) => Ok(ExSeriesIoType::S(64)),
            DataType::Duration(_) => Ok(ExSeriesIoType::S(64)),
            DataType::Time => Ok(ExSeriesIoType::S(64)),
            DataType::Categorical(_) => Ok(ExSeriesIoType::U(32)),
            _ => Err(ExplorerError::Other(format!(
                "cannot convert dtype {value} to iotype"
            ))),
        }
    }
}
