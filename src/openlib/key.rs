use std::str::FromStr;

use thiserror::Error;

/// An OpenLibrary keyspace.
pub struct OLKS {
    keyspace: &'static str,
    codechar: char,
}

/// Error parsing an OpenLibrary key.
#[derive(Error, Debug)]
pub enum OLKeyError {
    #[error("could not parse {0}: {1}")]
    InvalidFormat(String, String),
    #[error("bad keyspace for ‘{0}’, expected {1}")]
    BadKeyspace(String, &'static str),
    #[error("trailing code character mismatch")]
    InvalidCodeChar,
}

pub const KS_AUTHOR: OLKS = OLKS {
    keyspace: "authors",
    codechar: 'A',
};
#[allow(dead_code)]
pub const KS_WORK: OLKS = OLKS {
    keyspace: "works",
    codechar: 'W',
};
#[allow(dead_code)]
pub const KS_EDITION: OLKS = OLKS {
    keyspace: "editions",
    codechar: 'M',
};

struct OLKey<'a> {
    keyspace: &'a str,
    codechar: char,
    id: u32,
}

peg::parser! {
    grammar key_parser() for str {
        rule lcchar() = quiet!{['a'..='z']}
        rule lcword() -> &'input str = s:$(lcchar()+) {s}
        rule digit() = quiet!{['0'..='9']}
        rule number() -> u32 = n:$(digit()+) {?
            u32::from_str(n).or(Err("invalid number"))
        }

        pub rule ol_key() -> OLKey<'input>
        = "/" ks:lcword() "/OL" id:number() c:['A'..='Z'] {
            OLKey {
                keyspace: ks,
                codechar: c,
                id
            }
        }
    }
}

/// Parse an OpenLibrary key.
pub fn parse_ol_key(key: &str, ks: OLKS) -> Result<u32, OLKeyError> {
    let k = key_parser::ol_key(key)
        .map_err(|e| OLKeyError::InvalidFormat(key.to_string(), format!("{:?}", e)))?;
    if k.codechar != ks.codechar {
        Err(OLKeyError::InvalidCodeChar)
    } else if k.keyspace != ks.keyspace {
        Err(OLKeyError::BadKeyspace(key.to_string(), ks.keyspace))
    } else {
        Ok(k.id)
    }
}

#[test]
fn test_parse_work() {
    let id = parse_ol_key("/works/OL38140W", KS_WORK).expect("parse failed");
    assert_eq!(id, 38140);
}
