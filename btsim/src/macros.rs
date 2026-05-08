macro_rules! define_entity_id_and_handle {
    ( $base:ident ) => {
        paste::paste! {
            /// Type safe, copyable references into the simulation structure,
            /// used for internal references and as non-borrow pointer
            /// analogues.
            #[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)] // TODO Remove Ord?
            pub(crate) struct [<$base Id>](pub (crate) usize); // TODO Outpoint is OutputId, InputId is also a tuple

            /// Ephemeral view into primary and second information of $base.
            #[derive(Debug, Clone, Copy)]
            #[allow(dead_code)]
            pub(crate) struct [<$base Handle>]<'a> {

                pub(crate) sim: &'a crate::Simulation,
                pub(crate) id: [<$base Id>],
            }

            impl<'a> [<$base Id>] {
                /// Borrow the simulation, reifying the id to a [<$base Handle>]
                /// for read access to entity.
                #[allow(dead_code)]
                pub(crate) fn with(&self, sim: &'a crate::Simulation) -> [<$base Handle>]<'a>
                {
                    [<$base Handle>]::new(sim, *self)
                }
            }

            impl<'a> [<$base Handle>]<'a> {
                #[allow(dead_code)]
                pub(crate) fn new(sim: &'a crate::Simulation, id: [<$base Id>]) -> Self {
                    Self { sim, id }
                }
            }

            impl<'a> From<[<$base Handle>]<'a>> for [<$base Id>] {
                fn from(handle: [<$base Handle>]) -> [<$base Id>] {
                    handle.id
                }
            }
        }
    };
}

macro_rules! define_entity_data {
    (
        $base:ident,
        $data_fields:tt
    ) => {
        paste::paste! {
            /// Primary information associated with $base.
            #[derive(Debug, PartialEq, Eq, Clone)]
            pub(crate) struct [<$base Data>] $data_fields
        }
    };
    (
        $base:ident,
        $data_fields:tt,
        skip_eq_clone
    ) => {
        paste::paste! {
            /// Primary information associated with $base.
            #[derive(Debug, Clone)]
            pub(crate) struct [<$base Data>] $data_fields
        }
    };
}

macro_rules! define_entity_info {
    (
        $base:ident,
        $info_fields:tt
    ) => {
        paste::paste! {
            /// Secondary (derived) information associated with $base.
            #[derive(Debug, PartialEq, Eq, Clone)]
            #[allow(dead_code)]
            pub(crate) struct [<$base Info>] $info_fields
        }
    };
}

macro_rules! define_entity_info_id {
    (
        $base:ident
    ) => {
        paste::paste! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
            pub(crate) struct [<$base InfoId>](pub(crate) usize);
        }
    };
}

macro_rules! define_entity_handle_mut {
    (
        $base:ident
    ) => {
        paste::paste! {
            #[derive(Debug)]
            #[allow(dead_code)]
            pub(crate) struct [<$base HandleMut>]<'a> {

                pub(crate) sim: &'a mut crate::Simulation,
                pub(crate) id: [<$base Id>],
            }

            impl<'a> [<$base Id>] {
                #[allow(dead_code)]
                pub(crate) fn with_mut(&self, sim: &'a mut crate::Simulation) -> [<$base HandleMut>]<'a> {
                    [<$base HandleMut>]::new(sim, *self)
                }
            }

            impl<'a> [<$base HandleMut>]<'a> {
                fn new(sim: &'a mut crate::Simulation, id: [<$base Id>]) -> Self {
                    Self { sim, id }
                }
            }

            // Implement Deref to allow [<$base HandleMut>] to be used as [<$base Handle>]
            impl<'a> std::ops::Deref for [<$base HandleMut>]<'a> {
                type Target = [<$base Handle>]<'a>;

                fn deref(&self) -> &Self::Target {
                    // safety: [<$base Handle>] does not allow mutating sim
                    unsafe {
                        &*(self as *const [<$base HandleMut>]<'a> as *const [<$base Handle>]<'a>)
                    }
                }
            }

            impl<'a> From<[<$base HandleMut>]<'a>> for [<$base Id>] {
                fn from(handle: [<$base HandleMut>]) -> [<$base Id>] {
                    handle.id
                }
            }
        }
    };
}

#[macro_export]
macro_rules! define_entity {
    (
        $base:ident,
        $data_fields:tt,
        $info_fields:tt
    ) => {
        define_entity_id_and_handle!($base);
        define_entity_data!($base, $data_fields);
        define_entity_info!($base, $info_fields);
    };
}

// only wallet.. eliminate?
#[macro_export]
macro_rules! define_entity_mut_updatable {
    (
        $base:ident,
        $data_fields:tt,
        $info_fields:tt
    ) => {
        paste::paste! {
            define_entity_id_and_handle!($base);
            define_entity_handle_mut!($base);
            define_entity_info_id!($base);
            define_entity_data!($base, $data_fields);
            define_entity_info!($base, $info_fields);
        }
    };
}
