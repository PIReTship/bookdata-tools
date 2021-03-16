use std::fmt::Debug;

pub trait QueryContext {
  fn isbn_table(&self) -> String;
  fn node_limit(&self) -> String;
}

pub struct FullTable;

pub trait EdgeQuery<Q: QueryContext>: Debug {
  fn q_edges(&self, qc: &Q) -> String;
}

pub trait NodeQuery<Q: QueryContext>: Debug {
  fn q_node_ids(&self, qc: &Q) -> String;
  fn q_node_full(&self, qc: &Q) -> String;
}

#[derive(Debug)]
pub struct ISBN;
#[derive(Debug)]
pub struct LOC;
#[derive(Debug)]
pub struct OLEditions;
#[derive(Debug)]
pub struct OLWorks;
#[derive(Debug)]
pub struct GRBooks;
#[derive(Debug)]
pub struct GRWorks;

pub fn edge_sources<Q: QueryContext>() -> Vec<Box<dyn EdgeQuery<Q>>> {
  vec![
    Box::new(LOC),
    Box::new(OLEditions),
    Box::new(OLWorks),
    Box::new(GRBooks),
    Box::new(GRWorks)
  ]
}

pub fn node_sources<Q: QueryContext>() -> Vec<Box<dyn NodeQuery<Q>>> {
  vec![
    Box::new(ISBN),
    Box::new(LOC),
    Box::new(OLEditions),
    Box::new(OLWorks),
    Box::new(GRBooks),
    Box::new(GRWorks)
  ]
}

impl QueryContext for FullTable {
  fn isbn_table(&self) -> String {
    "isbn_id".to_owned()
  }

  fn node_limit(&self) -> String {
    "".to_owned()
  }
}

impl <Q: QueryContext> NodeQuery<Q> for ISBN {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("SELECT bc_of_isbn(isbn_id) AS id FROM {}", qc.isbn_table())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("SELECT bc_of_isbn(isbn_id) AS id, isbn AS label FROM {}", qc.isbn_table())
  }
}

impl <Q: QueryContext> NodeQuery<Q> for LOC {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_loc_rec(rec_id) AS id
      FROM locmds.book_rec_isbn {}
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_loc_rec(rec_id) AS id, title
      FROM locmds.book_rec_isbn {}
      LEFT JOIN locmds.book_title USING (rec_id)
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for LOC {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT bc_of_isbn(isbn_id) AS src, bc_of_loc_rec(rec_id) AS dst
      FROM locmds.book_rec_isbn {}
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> NodeQuery<Q> for OLEditions {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_edition(edition_id) AS id
      FROM ol.isbn_link {}
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT
          bc_of_edition(edition_id) AS id, edition_key AS label,
          NULLIF(edition_data->>'title', '') AS title
      FROM ol.isbn_link {}
      JOIN ol.edition USING (edition_id)
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for OLEditions {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_isbn(isbn_id) AS src, bc_of_edition(edition_id) AS dst
      FROM ol.isbn_link {}
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> NodeQuery<Q> for OLWorks {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_work(work_id) AS id
      FROM ol.isbn_link {}
      WHERE work_id IS NOT NULL
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT
          bc_of_work(work_id) AS id, work_key AS label,
          NULLIF(work_data->>'title', '') AS title
      FROM ol.isbn_link {}
      JOIN ol.work USING (work_id)
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for OLWorks {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_edition(edition_id) AS src, bc_of_work(work_id) AS dst
      FROM ol.isbn_link {}
      WHERE work_id IS NOT NULL
    ", qc.node_limit())
  }
}


impl <Q: QueryContext> NodeQuery<Q> for GRBooks {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_book(gr_book_id) AS id
      FROM gr.book_isbn {}
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_book(gr_book_id) AS id
      FROM gr.book_isbn {}
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for GRBooks {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_isbn(isbn_id) AS src, bc_of_gr_book(gr_book_id) AS dst
      FROM gr.book_isbn {}
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> NodeQuery<Q> for GRWorks {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_work(gr_work_id) AS id
      FROM gr.book_isbn {}
      JOIN gr.book_ids ids USING (gr_book_id)
      WHERE ids.gr_work_id IS NOT NULL
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_work(gr_work_id) AS id, work_title AS title
      FROM gr.book_isbn {}
      JOIN gr.book_ids ids USING (gr_book_id)
      LEFT JOIN gr.work_title USING (gr_work_id)
      WHERE ids.gr_work_id IS NOT NULL
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for GRWorks {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_book(gr_book_id) AS src, bc_of_gr_work(gr_work_id) AS dst
      FROM gr.book_isbn {}
      JOIN gr.book_ids ids USING (gr_book_id)
      WHERE ids.gr_work_id IS NOT NULL
    ", qc.node_limit())
  }
}
