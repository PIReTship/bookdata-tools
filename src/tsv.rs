pub fn split_first<'a>(line: &'a str) -> Option<(&'a str, &'a str)> {
    match line.find('\t') {
        Some(i) => Some((&line[0..i], &line[(i + 1)..])),
        None => None,
    }
}

#[test]
fn split_empty() {
    assert_eq!(split_first(""), None)
}

#[test]
fn split_tab() {
    assert_eq!(split_first("foo\tbar"), Some(("foo", "bar")))
}

#[test]
fn split_end() {
    assert_eq!(split_first("foo\t"), Some(("foo", "")))
}

#[test]
fn split_2() {
    assert_eq!(split_first("foo\tbar\tblatz"), Some(("foo", "bar\tblatz")))
}
