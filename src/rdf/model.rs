//! Types implementing the RDF data model.

/// An IRI for a named node
pub type IRI = String;

/// The object IRI for the designated empty triple.
static EMPTY_IRI: &'static str = "https://bookdata.piret.info/markers/empty";

/// A named or blank node
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Node {
  Named(IRI),
  Blank(String)
}

impl Node {
  #[inline]
  pub fn named<S: Into<String>>(s: S) -> Node {
    Node::Named(s.into())
  }

  #[inline]
  pub fn blank<S: Into<String>>(s: S) -> Node {
    Node::Blank(s.into())
  }
}

/// A literal value
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Literal {
  pub value: String,
  pub lang: Option<String>,
  pub schema: Option<String>,
}

/// An object term
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Term {
  Node(Node),
  Literal(Literal)
}

impl Term {
  #[inline]
  pub fn named<S: Into<String>>(s: S) -> Term {
    Term::Node(Node::named(s))
  }

  #[inline]
  pub fn blank<S: Into<String>>(s: S) -> Term {
    Term::Node(Node::blank(s))
  }

  #[inline]
  pub fn literal<V: Into<String>>(s: V) -> Term {
    Term::Literal(Literal {
      value: s.into(),
      lang: None, schema: None
    })
  }

  #[allow(dead_code)] // TODO use or remove
  pub fn literal_lang<V: Into<String>, L: Into<String>>(s: V, lang: L) -> Term {
    Term::Literal(Literal {
      value: s.into(),
      lang: Some(lang.into()),
      schema: None
    })
  }

  #[allow(dead_code)] // TODO use or remove
  pub fn literal_schema<V: Into<String>, S: Into<String>>(s: V, schema: S) -> Term {
    Term::Literal(Literal {
      value: s.into(),
      lang: None,
      schema: Some(schema.into())
    })
  }
}

/// An RDF triple.
///
/// This represents a complete triple read from an RDF file.
///
/// There is also the concept of an *empty* triple.  When parsed from a string using
/// the [FromStr] trait implementation, valid lines that do not contain triples (e.g.
/// blank lines or comment lines) will parse to the empty triple.  A triple's emptiness
/// can be tested with [is_empty].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Triple {
  pub subject: Node,
  pub predicate: IRI,
  pub object: Term
}

impl Triple {
  /// Construct a designated ‘empty’ triple.
  pub fn empty() -> Triple {
    Triple {
      subject: Node::named(EMPTY_IRI),
      predicate: EMPTY_IRI.to_owned(),
      object: Term::literal(""),
    }
  }

  /// Test whether this is the empty triple.
  pub fn is_empty(&self) -> bool {
    match &self.subject {
      Node::Named(n) if n.as_str() == EMPTY_IRI => true,
      _ => false
    }
  }
}

impl Default for Triple {
  fn default() -> Triple {
    Triple::empty()
  }
}
