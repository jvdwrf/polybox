// #[macro_export]
// macro_rules! Set {
//     () => { $crate::Set0 };

//     ($e1:path) => {
//         $crate::Set1<$e1>
//     };

//     ($e1:path, $e2:path) => {
//         $crate::Set2<$e1, $e2>
//     };

//     ($e1:path, $e2:path, $e3:path) => {
//         $crate::Set3<$e1, $e2, $e3>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path) => {
//         $crate::Set4<$e1, $e2, $e3, $e4>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path) => {
//         $crate::Set5<$e1, $e2, $e3, $e4, $e5>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path) => {
//         $crate::Set6<$e1, $e2, $e3, $e4, $e5, $e6>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path) => {
//         $crate::Set7<$e1, $e2, $e3, $e4, $e5, $e6, $e7>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path) => {
//         $crate::Set8<$e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path) => {
//         $crate::Set9<$e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path) => {
//         $crate::Set10<$e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10>
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path) => {
//         $crate::Set11<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10, $e11
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path) => {
//         $crate::Set12<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10, $e11, $e12
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path) => {
//         $crate::Set13<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10, $e11, $e12, $e13
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path) => {
//         $crate::Set14<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10, $e11,
//             $e12, $e13, $e14
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path) => {
//         $crate::Set15<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path) => {
//         $crate::Set16<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10, $e11,
//             $e12, $e13, $e14, $e15, $e16
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path) => {
//         $crate::Set17<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10, $e11,
//             $e12, $e13, $e14, $e15, $e16, $e17
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path) => {
//         $crate::Set18<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path, $e19:path) => {
//         $crate::Set19<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18, $e19
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path, $e19:path, $e20:path) => {
//         $crate::Set20<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18, $e19, $e20
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path, $e19:path, $e20:path, $e21:path) => {
//         $crate::Set21<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18, $e19, $e20,
//             $e21
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path, $e19:path, $e20:path, $e21:path, $e22:path) => {
//         $crate::Set22<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18, $e19, $e20,
//             $e21, $e22
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path, $e19:path, $e20:path, $e21:path, $e22:path, $e23:path) => {
//         $crate::Set23<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18, $e19,
//             $e20, $e21, $e22, $e23
//         >
//     };

//     ($e1:path, $e2:path, $e3:path, $e4:path, $e5:path, $e6:path, $e7:path, $e8:path, $e9:path, $e10:path, $e11:path, $e12:path, $e13:path, $e14:path, $e15:path, $e16:path, $e17:path, $e18:path, $e19:path, $e20:path, $e21:path, $e22:path, $e23:path, $e24:path) => {
//         $crate::Set24<
//             $e1, $e2, $e3, $e4, $e5, $e6, $e7, $e8, $e9, $e10,
//             $e11, $e12, $e13, $e14, $e15, $e16, $e17, $e18,
//             $e19, $e20, $e21, $e22, $e23, $e24
//         >
//     };
// }

#[macro_export]
macro_rules! Set {
    ($($es:path),* $(,)?) => {
        $crate::Set<($($es,)*)>
    };
}
