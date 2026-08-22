#[macro_export]
macro_rules! Set {
    ($($es:path),* $(,)?) => {
        $crate::Set<($($es,)*)>
    };
}
