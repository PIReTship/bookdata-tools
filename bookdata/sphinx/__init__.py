"""
Extension code for rendering the book data documentation using Sphinx.
"""

from sphinx.application import Sphinx
from docutils import nodes
from sphinx.roles import XRefRole
from sphinx.domains import ObjType

from .data import FileDirective
from .rust import RustDomain


def missing_ref(app: Sphinx, env, node, content):
    tgt = node['reftarget']
    if tgt.startswith('/apidocs/'):
        # it's an API doc ref, let it go
        node = nodes.reference('', '', internal=False)
        node['refuri'] = tgt
        node += content
        return node


def setup(app: Sphinx):
    app.add_directive_to_domain('std', 'file', FileDirective, True)
    app.add_role_to_domain('std', 'file', XRefRole(), True)
    ot = app.registry.domain_object_types.setdefault('std', {})
    ot['file'] = ObjType('file', 'file')

    app.add_domain(RustDomain)
    app.connect('missing-reference', missing_ref)

    return {
        'version': 1,
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
