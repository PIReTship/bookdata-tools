use arrow::datatypes::Schema;
use arrow::array::PrimitiveBuilder;

pub trait Record {
  fn schema() -> Schema;
}
