DROP MATERIALIZED VIEW IF EXISTS loc_isbn_peer CASCADE;
CREATE MATERIALIZED VIEW loc_isbn_peer
  AS WITH RECURSIVE
      peer (isbn1, isbn2) AS (SELECT li1.isbn, li2.isbn
                              FROM loc_isbn li1
                                JOIN loc_isbn li2 USING (rec_id)
                              UNION DISTINCT
                              SELECT p.isbn1, li2.isbn
                              FROM peer p
                                JOIN loc_isbn li1 ON (p.isbn1 = li1.isbn)
                                JOIN loc_isbn li2 USING (rec_id)
                              WHERE li1.isbn != li2.isbn)
  SELECT isbn1, isbn2 FROM peer;
CREATE INDEX loc_isbn_peer_i1_idx ON loc_isbn_peer (isbn1);
CREATE INDEX loc_isbn_peer_i2_idx ON loc_isbn_peer (isbn2);

DROP MATERIALIZED VIEW IF EXISTS ol_work_isbn_peer CASCADE;
CREATE MATERIALIZED VIEW ol_work_isbn_peer
  AS WITH RECURSIVE
      peer (isbn1, isbn2) AS (SELECT il1.isbn, il2.isbn
                              FROM ol_isbn_links il1
                                JOIN ol_isbn_links il2 USING (book_code)
                              UNION DISTINCT
                              SELECT p.isbn1, il2.isbn
                              FROM peer p
                                JOIN ol_isbn_links il1 ON (p.isbn1 = il1.isbn)
                                JOIN ol_isbn_links il2 USING (book_code)
                              WHERE il1.isbn != il2.isbn)
  SELECT isbn1, isbn2 FROM peer;
CREATE INDEX ol_work_isbn_peer_i1_idx ON ol_work_isbn_peer (isbn1);
CREATE INDEX ol_work_isbn_peer_i2_idx ON ol_work_isbn_peer (isbn2);
