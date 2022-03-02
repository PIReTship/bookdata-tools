# -*- coding: utf-8 -*-

from os import fspath
from pathlib import Path
import sys
import pybtex.plugin
from pybtex.style.sorting import BaseSortingStyle
from pybtex.style.formatting import plain

_root_dir = Path(__file__).parent.parent
sys.path.insert(0, fspath(_root_dir))

project = 'Book Data Tools'
copyright = '2020–2021 Boise State University'
author = 'Michael D. Ekstrand'
version = '2.0'
release = version
ogp_site_url = 'https://bookdata.piret.info'

extensions = [
    'myst_parser',
    'sphinx.ext.githubpages',
    'sphinxcontrib.bibtex',
    # 'sphinxext.opengraph',
    'bookdata.sphinx',
]

myst_enable_extensions = [
    'deflist',
    'colon_fence',
    'substitution',
]

bibtex_bibfiles = [
    'papers.bib'
]

html_theme = 'furo'
html_theme_options = {
    'light_css_variables': {
        'font-stack': 'Lato, sans-serif',
        'font-stack--monospace': 'Source Code Pro, monospace'
    }
}
html_baseurl = 'https://bookdata.piret.info'
templates_path = ['_templates']
# html_extra_path = [
#     '_extra'
# ]


class ChronoSort(BaseSortingStyle):
    def sorting_key(self, entry):
        year = entry.fields.get('year', '')
        month = entry.fields.get('month', '')
        title = entry.fields.get('title', '')
        return year, month, title

    def sort(self, entries):
        sorted = super().sort(entries)
        sorted.reverse()
        return sorted


class ChronoStyle(plain.Style):
    default_sorting_style = 'chrono'


pybtex.plugin.register_plugin('pybtex.style.sorting', 'chrono', ChronoSort)
pybtex.plugin.register_plugin('pybtex.style.formatting', 'chrono', ChronoStyle)
