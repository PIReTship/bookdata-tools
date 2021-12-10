---
title: Common Identifiers
---

# Common Identifiers

There are two key identifiers that are used across data sets.

## ISBNs

We use ISBNs for a lot of data linking.  In order to speed up ISBN-based
operations, we map textual ISBNs to numeric 'ISBN IDs`.

The `book-links/all-isbns.parquet` file manages ISBN IDs and their mappings,
along with statistics about their usage in other records.

| Column  | Purpose         |
| ------- | --------------- |
| isbn_id | ISBN identifier |
| isbn    | Textual ISBNs   |

Each type of ISBN (ISBN-10, ISBN-13) is considered a distinct ISBN. We also consider other ISBN-like things, particularly ASINs, to be ISBNs.

Most other tables that work with ISBNs use `isbn_id`s.

Additional fields in this table contain the number of records from different sources that reference this ISBN.

## Book Codes

We also use *book codes*, common identifiers for integrated 'books' across data sets. These are derived from identifiers in the various data sets, with `bc_of_*` functions.  Each book code source is assigned to a different 10M number band so we can, if needed, derive the source from a book code.

| Source       | Function             | Numspace |
| ------------ | -------------------- | -------- |
| OL Work      | `bc_of_work`         | 10M      |
| OL Edition   | `bc_of_edition`      | 20M      |
| LOC Record   | `bc_of_loc_rec`      | 30M      |
| GR Work      | `bc_of_gr_work`      | 40M      |
| GR Book      | `bc_of_gr_book`      | 50M      |
| LOC Work     | `bc_of_loc_work`     | 60M      |
| LOC Instance | `bc_of_loc_instance` | 70M      |
| ISBN         | `bc_of_isbn`         | 90M      |

The LOC Work and Instance sources are not currently used; they are intended for future use when we are able to import BIBFRAME data from the Library of Congress.
