/// This module contains the code for the [`message::DBRequest`] that
/// is used to communicate with the [`actor::DBActor`].
/// This includes trait implementations to conveniently convert between types.
pub mod message;

/// This module contains the enum of data types that can be stored in the [`actor::DBActor]s 
/// HashMap.
pub mod map_entry;

/// This module contains the code to the [`actor::DBActor`]:
/// Its message handling and its operations on the HashMap
pub mod actor;