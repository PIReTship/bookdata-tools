--- #dep loc-mds-index-books
--- #dep gr-index-books
--- #dep ol-index

--- #step Extract normalized ISBNs
DROP TABLE IF EXISTS isbn_norm CASCADE;
CREATE TABLE isbn_norm (
    isbn_id INTEGER PRIMARY KEY,
    norm_isbn EAN13 NOT NULL
);
INSERT INTO isbn_norm
SELECT isbn_id, make_valid(isbn(isbn || '!'))
FROM isbn_id WHERE isbn ~ '^\d{9}[\dxX]$';
INSERT INTO isbn_norm
SELECT isbn_id, make_valid(ean13(regexp_replace(isbn, '[xX]$', '0') || '!'))
FROM isbn_id WHERE isbn ~ '^9\d{11}[\dxX]$';