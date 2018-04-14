#!/bin/sh

for db in "$@"; do
    nrecords=$(sqlite3 $db 'SELECT COUNT(*) FROM MARC_DB')
    echo "Exporting $nrecords records from $db"
    sqlite3 -csv $db 'SELECT * FROM MARC_DB' |psql -c '\copy loc_marc_records FROM stdin WITH (FORMAT csv)'
    nfields=$(sqlite3 $db 'SELECT COUNT(*) FROM MARC_FIELDS')
    echo "Exporting $nfields fields from $db"
    sqlite3 -csv $db 'SELECT * FROM MARC_FIELDS' |psql -c '\copy loc_marc_fields FROM stdin WITH (FORMAT csv)'
done
