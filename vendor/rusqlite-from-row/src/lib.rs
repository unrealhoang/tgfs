#![deny(missing_docs)]
#![doc = include_str!("../README.md")]

pub use rusqlite;
pub use rusqlite_from_row_derive::FromRow;

/// A trait that allows mapping a [`rusqlite::Row`] to other types.
pub trait FromRow: Sized {
    /// Performs the conversion.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_row(row: &rusqlite::Row) -> Self {
        Self::try_from_row(row).expect("from row failed")
    }

    /// Try's to perform the conversion.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        Self::try_from_row_prefixed(row, None)
    }

    /// Perform the conversion. Each row will be extracted using it's name prefixed with
    /// `prefix`.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_row_prefixed(row: &rusqlite::Row, prefix: Option<&str>) -> Self {
        Self::try_from_row_prefixed(row, prefix).expect("from row failed")
    }

    /// Try's to perform the conversion. Each row will be extracted using it's name prefixed with
    /// `prefix`.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_row_prefixed(
        row: &rusqlite::Row,
        prefix: Option<&str>,
    ) -> Result<Self, rusqlite::Error>;

    /// Try's to check if all the columns that are needed by this struct are sql 'null' values.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn is_all_null(row: &rusqlite::Row, prefix: Option<&str>) -> Result<bool, rusqlite::Error>;
}

impl<T: FromRow> FromRow for Option<T> {
    fn try_from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        if T::is_all_null(row, None)? {
            Ok(None)
        } else {
            Ok(Some(T::try_from_row(row)?))
        }
    }

    fn try_from_row_prefixed(
        row: &rusqlite::Row,
        prefix: Option<&str>,
    ) -> Result<Self, rusqlite::Error> {
        if T::is_all_null(row, prefix)? {
            Ok(None)
        } else {
            Ok(Some(T::try_from_row_prefixed(row, prefix)?))
        }
    }

    fn is_all_null(row: &rusqlite::Row, prefix: Option<&str>) -> Result<bool, rusqlite::Error> {
        T::is_all_null(row, prefix)
    }
}

macro_rules! impl_from_row_for_sql_types {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl FromRow for $ty {
                fn try_from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
                    row.get(0)
                }

                fn try_from_row_prefixed(
                    row: &rusqlite::Row,
                    _prefix: Option<&str>,
                ) -> Result<Self, rusqlite::Error> {
                    Self::try_from_row(row)
                }

                fn is_all_null(
                    row: &rusqlite::Row,
                    _prefix: Option<&str>,
                ) -> Result<bool, rusqlite::Error> {
                    Ok(row.get_ref(0)? == rusqlite::types::ValueRef::Null)
                }
            }
        )+
    };
}

impl_from_row_for_sql_types!(
    bool,
    i8,
    i16,
    i32,
    i64,
    isize,
    u8,
    u16,
    u32,
    u64,
    usize,
    f32,
    f64,
    String,
    Box<str>,
    std::rc::Rc<str>,
    std::sync::Arc<str>,
    Vec<u8>,
    std::num::NonZeroI8,
    std::num::NonZeroI16,
    std::num::NonZeroI32,
    std::num::NonZeroI64,
    std::num::NonZeroIsize,
    std::num::NonZeroU8,
    std::num::NonZeroU16,
    std::num::NonZeroU32,
    std::num::NonZeroU64,
    std::num::NonZeroUsize,
);

impl<const N: usize> FromRow for [u8; N] {
    fn try_from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        row.get(0)
    }

    fn try_from_row_prefixed(
        row: &rusqlite::Row,
        _prefix: Option<&str>,
    ) -> Result<Self, rusqlite::Error> {
        Self::try_from_row(row)
    }

    fn is_all_null(row: &rusqlite::Row, _prefix: Option<&str>) -> Result<bool, rusqlite::Error> {
        Ok(row.get_ref(0)? == rusqlite::types::ValueRef::Null)
    }
}
