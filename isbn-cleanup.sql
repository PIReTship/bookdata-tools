-- Debug query
SELECT raw_isbn
FROM locid.instance_isbn
WHERE extract_isbn(raw_isbn) IS NULL
AND raw_isbn !~* '^\s*(\$\d|[[:digit:].]+ru?b|xx[xv]*|\*)'
LIMIT 100;
