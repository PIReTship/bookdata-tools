"""
Extension code for rendering the book data documentation using Sphinx.
"""

from sphinx.application import Sphinx


def missing_ref(app: Sphinx, env, node, contnode):
    print('missing reference: ', node)


def setup(app: Sphinx):
    app.add_object_type('file', 'file', '%s', override=True)
    app.connect('missing-reference', missing_ref)

    return {
        'version': 1,
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
