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
        'struct': XRefRole(),
    }

    uri_base = '/apidocs/'

    def resolve_any_xref(self, env, fromdocname, builder, target, node, contnode):
        return []

    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):
        parts = target.split('::')
        if parts[0][0] == '~':
            parts[0] = parts[0][1:]
            contnode.children[0] = nodes.Text(parts[-1])

        if typ == 'mod':
            uri = '/'.join(parts)
        elif typ == 'struct':
            uri = '/'.join(parts[:-1])
            uri += f'/struct.{parts[-1]}.html'
        elif typ == 'fn':
            uri = '/'.join(parts[:-1])
            uri += f'/fn.{parts[-1]}.html'

        uri = self.uri_base + uri

        node = nodes.reference('', '', classes=['rust', typ])
        node['refuri'] = uri
        node += contnode

        return node
