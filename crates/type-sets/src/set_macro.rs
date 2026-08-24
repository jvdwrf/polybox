/// A macro for creating a [`Set`] of types.
///
/// Examples of valid sets include:
/// - `Set!()` - an empty set  (`Set<()>`)
/// - `Set!(A, B, C)` - a set containing types A, B, and C (`Set<(A, B, C)>`)
#[macro_export]
macro_rules! Set {
    ($($es:ty),* $(,)?) => {
        $crate::Set<($($es,)*)>
    };
}
