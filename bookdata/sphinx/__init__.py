"""
Extension code for rendering the book data documentation using Sphinx.
"""

from sphinx.application import Sphinx


def setup(app: Sphinx):
    app.add_object_type('file', 'file', '%s', override=True)

    return {
        'version': 1,
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
