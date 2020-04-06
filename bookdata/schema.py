"""
Data schema information for the book data tools.
"""

import pandas as pd

ns_work=100000000
ns_edition=200000000
ns_rec=300000000
ns_gr_work=400000000
ns_gr_book=500000000
ns_loc_work=600000000
ns_loc_instance=700000000
ns_isbn=900000000

src_labels = pd.Series({
    'OL-W': 1,
    'OL-E': 2,
    'LOC': 3,
    'GR-W': 4,
    'GR-B': 5,
    'LOC-W': 6,
    'LOC-I': 7,
    'ISBN': 9
})
src_label_rev = pd.Series(src_labels.index, index=src_labels.values)
