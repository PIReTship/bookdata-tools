"""
Extension code for rendering the book data documentation using Sphinx.
"""

from sphinx.application import Sphinx
from docutils import nodes


def missing_ref(app: Sphinx, env, node, content):
    tgt = node['reftarget']
    if tgt.startswith('/apidocs/'):
        # it's an API doc ref, let it go
        node = nodes.reference('', '', internal=False)
        node['refuri'] = tgt
        node += content
        return node


def setup(app: Sphinx):
    app.add_object_type('file', 'file', 'data files; %s', override=True)
    app.connect('missing-reference', missing_ref)

    return {
        'version': 1,
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
