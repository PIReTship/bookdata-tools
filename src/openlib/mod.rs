//! OpenLibrary data layouts and parsing logic.
pub mod source;
pub mod author;
pub mod edition;
pub mod work;

pub use source::Row;
pub use author::AuthorProcessor;
pub use work::WorkProcessor;
pub use edition::EditionProcessor;
