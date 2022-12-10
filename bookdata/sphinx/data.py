from typing import TypeVar
from pathlib import Path
import json

from docutils import nodes
from sphinx import addnodes
from sphinx.util.nodes import clean_astext, make_id, make_refnode
from sphinx.directives import ObjectDescription

T = TypeVar('T')


class FileDirective(ObjectDescription):
    def add_target_and_index(self, name: T, sig: str, signode: addnodes.desc_signature) -> None:
        node_id = make_id(self.env, self.state.document, self.objtype, name)
        signode['ids'].append(node_id)

        # add to the index
        entry = f'data files; {name}'
        self.indexnode['entries'].append(('single', entry, node_id, '', None))

        std = self.env.get_domain('std')
        std.note_object('file', name, node_id, location=signode)

    def handle_signature(self, sig: str, signode: addnodes.desc_signature) -> T:
        # The signature is the name
        signode.clear()
        signode += addnodes.desc_name(sig, sig)
        return sig

    def transform_content(self, contentnode: addnodes.desc_content) -> None:
        name = self.names[0]
        path = Path(name)
        js_path = path.with_suffix('.json')

        if not js_path.exists():
            w = self.state.reporter.warning(f'schema file {js_path} does not exist')
            contentnode += w
            return

        schema = json.loads(js_path.read_text())

        table = nodes.table(classes=['colwidths-auto'])
        tg = nodes.tgroup(cols=2)
        tg += nodes.colspec(colwidth=1)
        tg += nodes.colspec(colwidth=0)
        table += tg

        # set up the table heading
        head = nodes.thead()
        hrow = nodes.row()
        head += hrow
        hrow += nodes.entry('', nodes.paragraph('', '', nodes.inline('', 'Field')))
        hrow += nodes.entry('', nodes.paragraph('', '', nodes.inline('', 'Type')))
        tg += head

        # write the field table
        tb = nodes.tbody()
        tg += tb
        for field in schema.get('fields', []):
            row = nodes.row()
            row += nodes.entry('', nodes.paragraph('', '', nodes.literal('', field['name'])))

            type = field['data_type']
            if field['nullable']:
                type += '?'
            row += nodes.entry('', nodes.paragraph('', '', nodes.literal('', type)))
            tb += row

        contentnode += table
