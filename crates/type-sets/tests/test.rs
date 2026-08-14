use type_sets::*;

fn accepts_superset<T: SupersetOf<Set!(u32)>>() {}
fn accepts_subset<T: SubsetOf<Set!(u32, u64)>>() {}
fn accepts_contains<T: Contains<u32>>() {}

#[test]
fn test() {
    accepts_superset::<Set<(u32,)>>();
    accepts_superset::<Set<(u64, u32)>>();
    // accepts_superset::<Set<(String, u64, u128)>>();

    accepts_contains::<Set!(u32)>();
    accepts_contains::<Set!(u32, u64)>();
    // accepts_contains::<Set!(String, u64, u128)>();

    accepts_subset::<Set!(u32, u64)>();
    // accepts_subset::<Set!(u32, u64, u128)>();
}
