from docutils import nodes
from sphinx.application import Sphinx
from sphinx.domains import Domain
from sphinx.roles import XRefRole


class RustDomain(Domain):
    """
    Domain allowing easy references to Rust objects.
    """
    name = 'rust'
    label = 'Rust'

    roles = {
        'mod': XRefRole(),
        'const': XRefRole(),
        'fn': XRefRole(),
    }

    uri_base = '/apidocs/'

    def resolve_any_xref(self, env, fromdocname, builder, target, node, contnode):
        return []

    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):
        parts = target.split('::')

        if typ == 'mod':
            uri = '/'.join(parts)

        uri = self.uri_base + uri

        node = nodes.reference('', '', classes=['rust', typ])
        node['refuri'] = uri
        node += contnode

        return node
