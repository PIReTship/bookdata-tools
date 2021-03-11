---
title: Common Identifiers
parent: Data Model
nav_order: 1
---

# Common Identifiers

There are two key identifiers that are used across data sets.

## ISBNs

We use ISBNs for a lot of data linking.  In order to speed up ISBN-based operations, we map textual ISBNs to numeric 'ISBN IDs`.

The `isbn_id` table manages ISBN IDs and their mappings:

| Column  | Purpose         |
| ------- | --------------- |
| isbn_id | ISBN identifier |
| isbn    | Textual ISBNs   |

Each type of ISBN (ISBN-10, ISBN-13) is considered a distinct ISBN. We also consider other ISBN-like things, particularly ASINs, to be ISBNs.

Most derived tables that work with ISBNs use `isbn_id`s.

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
