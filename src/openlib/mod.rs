//! OpenLibrary data layouts and parsing logic.
pub mod author;
pub mod edition;
pub mod key;
pub mod source;
pub mod subject;
pub mod work;

pub use author::AuthorProcessor;
pub use edition::EditionProcessor;
pub use source::Row;
pub use work::WorkProcessor;
