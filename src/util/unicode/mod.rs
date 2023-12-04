#[allow(dead_code)]
mod tables;

pub static NONSPACING_MARK: TableSet<'static> = TableSet::from_table(tables::NONSPACING_MARK);

#[cfg(test)]
pub static UPPERCASE_LETTER: TableSet<'static> = TableSet::from_table(tables::UPPERCASE_LETTER);

/// A set of items from a ucd-generate table.
pub struct TableSet<'a> {
    char_seqs: &'a [(char, char)],
}

impl<'a> TableSet<'a> {
    const fn from_table(seqs: &'a [(char, char)]) -> TableSet<'a> {
        TableSet { char_seqs: seqs }
    }

    pub fn contains(&self, c: char) -> bool {
        let res = self.char_seqs.binary_search_by_key(&c, |(s, _)| *s);
        if let Err(pos) = res {
            // we didn't find it, but we have the insert position
            // the *preceeding* position may have it!
            if pos > 0 {
                let (lb, ub) = self.char_seqs[pos - 1];
                assert!(c > lb);
                c <= ub // contain it if c is less than upper bound
            } else {
                false // character is before lb of first entry
            }
        } else {
            true // character is exactly lower bound
        }
    }
}

#[test]
pub fn test_contains_lb() {
    assert!(UPPERCASE_LETTER.contains('A'));
}

#[test]
pub fn test_contains_ub() {
    assert!(UPPERCASE_LETTER.contains('Z'));
}

#[test]
pub fn test_contains_mid() {
    assert!(UPPERCASE_LETTER.contains('Q'));
}

#[test]
pub fn test_does_not_contain_early() {
    assert!(!UPPERCASE_LETTER.contains(' '));
}

#[test]
pub fn test_contains_higher_unicode() {
    assert!(UPPERCASE_LETTER.contains('𝔸'));
    assert!(UPPERCASE_LETTER.contains('ℚ'));
    assert!(UPPERCASE_LETTER.contains('𝑍'));
}

#[test]
pub fn test_omits_higher_unicode() {
    assert!(!UPPERCASE_LETTER.contains('ᏺ'));
}
