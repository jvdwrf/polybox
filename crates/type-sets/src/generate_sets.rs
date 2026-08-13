#![allow(unused)]
use super::*;
use std::{any::TypeId, marker::PhantomData, sync::OnceLock};

macro_rules! generate_sets {
    ($(
        for<$($el:ident),*> $n:literal $($true:literal)? = {
            pub struct $struct:ident;

            trait $trait:ident: $(
                $sub_trait:ident <$($sub_el:ident),*>
            ),* {}
        }
    )*) => {
        // Trait definitions
        mod _priv {
            use super::*;
            $(
                /// A private trait that defines the set of types contained in a type set.
                pub trait $trait<$($el),*>: $($sub_trait <$($sub_el),*> +)* {}
            )*
        }
        pub(crate) use _priv::*;

        // Struct definitions
        $(
            /// A [`TypeSet`] of `n` types.
            ///
            /// Look at the [`Set`] macro for a more convenient way to define type sets.
            pub struct $struct<$($el),*>(PhantomData<fn() -> ($($el),*)>);
        )*

        // Subset implementations
        $(
            #[diagnostic::do_not_recommend]
            impl<S: ?Sized, $($el),*> SubsetOf<S> for dyn $trait<$($el),*>
                where S: $trait<$($el),*>
            {}
        )*

        // TypeSet implementations
        $(
            #[diagnostic::do_not_recommend]
            impl<$($el),*> TypeSet for $struct<$($el),*>
            {
                type Set = dyn $trait<$($el),*>;

                fn members() -> &'static [TypeId]
                where
                    Self: 'static,
                {
                    static MEMBERS: OnceLock<[TypeId; $n]> = OnceLock::new();
                    MEMBERS.get_or_init(|| [$(
                        TypeId::of::<$el>()
                    ),*])
                }
            }
        )*

        // Contains for Subset implementations
        $(
            #[diagnostic::do_not_recommend]
            impl<S: ?Sized, $($el),*> $trait<$($el),*> for S
            where
                S: TypeSet,
                S::Set: $trait<$($el),*>
            {}
        )*
    };
}

generate_sets! {
    // Set0 / Contains0 is implemented manually below, because it has no constraints

    for<E1> 1 = {
        pub struct Set1;
        trait Contains1: Contains0<> {}
    }

    for<E1, E2> 2 = {
        pub struct Set2;
        trait Contains2: Contains1<E1>, Contains1<E2> {}
    }

    for<E1, E2, E3> 3 = {
        pub struct Set3;
        trait Contains3: Contains2<E1, E2>, Contains1<E3> {}
    }

    for<E1, E2, E3, E4> 4 = {
        pub struct Set4;
        trait Contains4: Contains3<E1, E2, E3>, Contains1<E4> {}
    }

    for<E1, E2, E3, E4, E5> 5 = {
        pub struct Set5;
        trait Contains5: Contains4<E1, E2, E3, E4>, Contains1<E5> {}
    }

    for<E1, E2, E3, E4, E5, E6> 6 = {
        pub struct Set6;
        trait Contains6: Contains5<E1, E2, E3, E4, E5>, Contains1<E6> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7> 7 = {
        pub struct Set7;
        trait Contains7: Contains6<E1, E2, E3, E4, E5, E6>, Contains1<E7> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8> 8 = {
        pub struct Set8;
        trait Contains8: Contains7<E1, E2, E3, E4, E5, E6, E7>, Contains1<E8> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9> 9 = {
        pub struct Set9;
        trait Contains9: Contains8<E1, E2, E3, E4, E5, E6, E7, E8>, Contains1<E9> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10> 10 = {
        pub struct Set10;
        trait Contains10: Contains9<E1, E2, E3, E4, E5, E6, E7, E8, E9>, Contains1<E10> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11> 11 = {
        pub struct Set11;
        trait Contains11: Contains10<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10>, Contains1<E11> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12> 12 = {
        pub struct Set12;
        trait Contains12: Contains11<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11>, Contains1<E12> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13> 13 = {
        pub struct Set13;
        trait Contains13: Contains12<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12>, Contains1<E13> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14> 14 = {
        pub struct Set14;
        trait Contains14: Contains13<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13>, Contains1<E14> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15> 15 = {
        pub struct Set15;
        trait Contains15: Contains14<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14>, Contains1<E15> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16> 16 = {
        pub struct Set16;
        trait Contains16: Contains15<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15>, Contains1<E16> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17> 17 = {
        pub struct Set17;
        trait Contains17: Contains16<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16>, Contains1<E17> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18> 18 = {
        pub struct Set18;
        trait Contains18: Contains17<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17>, Contains1<E18> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19> 19 = {
        pub struct Set19;
        trait Contains19: Contains18<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18>, Contains1<E19> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20> 20 = {
        pub struct Set20;
        trait Contains20: Contains19<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19>, Contains1<E20> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21> 21 = {
        pub struct Set21;
        trait Contains21: Contains20<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20>, Contains1<E21> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21, E22> 22 = {
        pub struct Set22;
        trait Contains22: Contains21<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21>, Contains1<E22> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21, E22, E23> 23 = {
        pub struct Set23;
        trait Contains23: Contains22<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21, E22>, Contains1<E23> {}
    }

    for<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21, E22, E23, E24> 24 = {
        pub struct Set24;
        trait Contains24: Contains23<E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21, E22, E23>, Contains1<E24> {}
    }
}

mod _priv_0 {
    pub trait Contains0 {}
}
pub(crate) use _priv_0::*;

pub struct Set0;

#[diagnostic::do_not_recommend]
impl<S: ?Sized> SubsetOf<S> for dyn Contains0 {}

#[diagnostic::do_not_recommend]
impl<S: ?Sized> Contains0 for S {}

impl TypeSet for Set0 {
    type Set = dyn Contains0;

    fn members() -> &'static [TypeId] {
        &[]
    }
}
