"""
Data schema information for the book data tools.
"""

import pandas as pd

MULT_BASE = 100_000_000


class NS:
    def __init__(self, name, num):
        self.name = name
        self.code = num
        self.offset = num * MULT_BASE


ns_work = NS('OL-W', 1)
ns_edition = NS('OL-E', 2)
ns_loc_rec = NS('LOC', 3)
ns_gr_work = NS('GR-W', 4)
ns_gr_book = NS('GR-B', 5)
ns_loc_work = NS('LOC-W', 6)
ns_loc_instance = NS('LOC-I', 7)
ns_isbn = NS('ISBN', 9)

numspaces = [
    ns_work, ns_edition,
    ns_loc_rec,
    ns_gr_work, ns_gr_book,
    ns_loc_work, ns_loc_instance,
    ns_isbn
]

src_labels = pd.Series(dict((_ns.name, _ns.code) for _ns in numspaces))
src_label_rev = pd.Series(src_labels.index, index=src_labels.values)
