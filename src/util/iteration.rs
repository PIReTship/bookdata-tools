//! Iteration utilities.

use std::mem::replace;

/// A chunking iterator that owns its delegate.
///
/// See [chunk_owned] for details.
pub struct ChunkIter<I: Iterator> {
    chunk_size: usize,
    chunk: Vec<I::Item>,
    iter: I,
}

/// Wrap an iterator in an iterator that returns chunks.
///
/// This works like the chunking support in Itertools, but it actually owns the iterator so
/// it can be shipped across threads, etc.
pub fn chunk_owned<I: Iterator>(iter: I, chunk_size: usize) -> ChunkIter<I> {
    ChunkIter {
        chunk_size,
        chunk: Vec::with_capacity(chunk_size),
        iter,
    }
}

impl<I: Iterator> Iterator for ChunkIter<I> {
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Vec<I::Item>> {
        loop {
            if let Some(i) = self.iter.next() {
                self.chunk.push(i);
                if self.chunk.len() >= self.chunk_size {
                    return Some(replace(
                        &mut self.chunk,
                        Vec::with_capacity(self.chunk_size),
                    ));
                }
            } else if self.chunk.len() > 0 {
                // since we are at the end, replace with empty array
                return Some(replace(&mut self.chunk, Vec::new()));
            } else {
                // nothing left and no batch
                return None;
            }
        }
    }
}

#[test]
fn test_empty() {
    let data: Vec<i32> = vec![];
    let mut wrap = chunk_owned(data.into_iter(), 5);

    assert!(wrap.next().is_none());
    // make sure calling it twice is fine
    assert!(wrap.next().is_none());
}

#[test]
fn test_chunk() {
    let data = 0..5;
    let mut wrap = chunk_owned(data.into_iter(), 5);

    let c1 = wrap.next();
    assert!(c1.is_some());
    let c1 = c1.unwrap();
    assert_eq!(c1, vec![0, 1, 2, 3, 4]);

    // make sure it is empty
    assert!(wrap.next().is_none());

    // and still empty
    assert!(wrap.next().is_none());
}

#[test]
fn test_two_chunks() {
    let data = 0..8;
    let mut wrap = chunk_owned(data.into_iter(), 5);

    let c1 = wrap.next();
    assert!(c1.is_some());
    let c1 = c1.unwrap();
    assert_eq!(c1, vec![0, 1, 2, 3, 4]);

    let c2 = wrap.next();
    assert!(c2.is_some());
    let c2 = c2.unwrap();
    assert_eq!(c2, vec![5, 6, 7]);

    // make sure it is empty
    assert!(wrap.next().is_none());

    // and still empty
    assert!(wrap.next().is_none());
}
