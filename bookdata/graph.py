"""
Utiltiies for loading & working with the book identifier graph.
"""

import logging

import pandas as pd
import numpy as np
from graph_tool import Graph
from .schema import *

_log = logging.getLogger(__name__)

class MinGraphBuilder:
    def __init__(self):
        self.graph = Graph(directed=False)
        self.codes = []
        self.labels = []
        self.sources = []

    def add_nodes(self, df, ns):
        n = len(df)
        _log.info('adding %d nodes to graph', n)
        start = self.graph.num_vertices()
        vs = self.graph.add_vertex(n)
        end = self.graph.num_vertices()
        assert end - start == n
        nodes = pd.Series(np.arange(start, end, dtype='i4'), index=df['id'])
        self.codes.append(df['id'].values + ns.offset)
        self.labels.append(df['id'].values)
        self.sources.append(np.full(n, ns.code, dtype='i2'))
        return nodes

    def add_edges(self, f, src, dst):
        _log.info('adding %d edges to graph', len(f))
        edges = np.zeros((len(f), 2), dtype='i4')
        edges[:, 0] = src.loc[f.iloc[:, 0]]
        edges[:, 1] = dst.loc[f.iloc[:, 1]]
        self.graph.add_edge_list(edges)

    def finish(self):
        _log.info('setting code attributes')
        code_a = self.graph.new_vp('int64_t')
        code_a.a[:] = np.concatenate(self.codes)
        self.graph.vp['code'] = code_a

        _log.info('setting label attributes')
        label_a = self.graph.new_vp('int64_t')
        label_a.a[:] = np.concatenate(self.labels)
        self.graph.vp['label'] = label_a

        _log.info('setting source attributes')
        source_a = self.graph.new_vp('int16_t')
        source_a.a[:] = np.concatenate(self.sources)
        self.graph.vp['source'] = source_a

        return self.graph


class FullGraphBuilder:
    def __init__(self):
        self.graph = Graph(directed=False)
        self.codes = []
        self.sources = []
        self.labels = []
        self.attrs = set()

    def add_nodes(self, df, ns):
        n = len(df)
        _log.info('adding %d nodes to graph', n)
        start = self.graph.num_vertices()
        vs = self.graph.add_vertex(n)
        end = self.graph.num_vertices()
        assert end - start == n
        nodes = pd.Series(np.arange(start, end, dtype='i4'), index=df['id'])
        self.codes.append(df['id'].values + ns.offset)
        self.sources.append(np.full(n, ns.code, dtype='i2'))
        if 'label' in df.columns:
            self.labels += list(df['label'].values)
        else:
            self.labels += list(df['id'].astype('str').values)

        for c in df.columns:
            if c in ['id', 'label']:
                continue
            if c not in self.attrs:
                vp = self.graph.new_vp('string')
                self.graph.vp[c] = vp
                self.attrs.add(c)
            else:
                vp = self.graph.vp[c]

            for v, val in zip(vs, df[c].values):
                vp[v] = val

        return nodes

    def add_edges(self, f, src, dst):
        _log.info('adding %d edges to graph', len(f))
        edges = np.zeros((len(f), 2), dtype='i4')
        edges[:, 0] = src.loc[f.iloc[:, 0]]
        edges[:, 1] = dst.loc[f.iloc[:, 1]]
        self.graph.add_edge_list(edges)

    def finish(self):
        _log.info('setting code attributes')
        code_a = self.graph.new_vp('int64_t')
        code_a.a[:] = np.concatenate(self.codes)
        self.graph.vp['code'] = code_a

        _log.info('setting source attributes')
        source_a = self.graph.new_vp('string')
        for v, s in zip(self.graph.vertices(), np.concatenate(self.sources)):
            source_a[v] = src_label_rev[s]
        self.graph.vp['source'] = source_a

        _log.info('setting source attributes')
        label_a = self.graph.new_vp('string')
        for v, l in zip(self.graph.vertices(), self.labels):
            label_a[v] = l
        self.graph.vp['label'] = label_a

        return self.graph


class GraphLoader:
    cluster = None
    isbn_table = 'isbn_id'

    def set_cluster(self, cluster, cur):
        _log.info('restricting graph load to cluster %s', cluster)
        self.cluster = cluster
        self.isbn_table = 'gc_isbns'
        cur.execute('''
            CREATE TEMPORARY TABLE gc_isbns
            AS SELECT isbn_id, isbn
            FROM isbn_cluster JOIN isbn_id USING (isbn_id)
            WHERE cluster = %s
        ''', [self.cluster])

    def q_isbns(self, full=True):
        if full:
            return f'SELECT isbn_id AS id, isbn FROM {self.isbn_table}'
        else:
            return f'SELECT isbn_id AS id FROM {self.isbn_table}'

    @property
    def limit(self):
        if self.isbn_table == 'isbn_id':
            return ''
        else:
            return f'JOIN {self.isbn_table} USING (isbn_id)'

    def q_loc_nodes(self, full=False):
        if full:
            return f'''
                SELECT DISTINCT rec_id AS id, title
                FROM locmds.book_rec_isbn {self.limit}
                LEFT JOIN locmds.book_title USING (rec_id)
            '''
        else:
            return f'''
                SELECT DISTINCT rec_id AS id
                FROM locmds.book_rec_isbn {self.limit}
                '''

    def q_loc_edges(self):
        return f'''
            SELECT isbn_id, rec_id
            FROM locmds.book_rec_isbn {self.limit}
        '''

    def q_ol_edition_nodes(self, full=False):
        if full:
            return f'''
                SELECT DISTINCT
                    edition_id AS id, edition_key AS label,
                    NULLIF(edition_data->>'title', '') AS title
                FROM ol.isbn_link {self.limit}
                JOIN ol.edition USING (edition_id)
            '''
        else:
            return f'''
                SELECT DISTINCT edition_id AS id
                FROM ol.isbn_link {self.limit}
            '''

    def q_ol_work_nodes(self, full=False):
        if full:
            return f'''
                SELECT DISTINCT
                    work_id AS id, work_key AS label,
                    NULLIF(work_data->>'title', '') AS title
                FROM ol.isbn_link {self.limit}
                JOIN ol.work USING (work_id)
            '''
        else:
            return f'''
                SELECT DISTINCT work_id AS id
                FROM ol.isbn_link {self.limit}
                WHERE work_id IS NOT NULL
            '''

    def q_ol_edition_edges(self):
        return f'''
            SELECT DISTINCT isbn_id, edition_id
            FROM ol.isbn_link {self.limit}
        '''

    def q_ol_work_edges(self):
        return f'''
            SELECT DISTINCT edition_id, work_id
            FROM ol.isbn_link {self.limit}
            WHERE work_id IS NOT NULL
        '''

    def q_gr_book_nodes(self, full=False):
        return f'''
            SELECT DISTINCT gr_book_id AS id
            FROM gr.book_isbn {self.limit}
        '''

    def q_gr_work_nodes(self, full=False):
        if full:
            return f'''
                SELECT DISTINCT gr_work_id AS id, work_title AS title
                FROM gr.book_isbn {self.limit}
                JOIN gr.book_ids ids USING (gr_book_id)
                LEFT JOIN gr.work_title USING (gr_work_id)
                WHERE ids.gr_work_id IS NOT NULL
            '''
        else:
            return f'''
                SELECT DISTINCT gr_work_id AS id
                FROM gr.book_isbn {self.limit}
                JOIN gr.book_ids ids USING (gr_book_id)
                WHERE ids.gr_work_id IS NOT NULL
            '''

    def q_gr_book_edges(self, full=False):
        return f'''
            SELECT DISTINCT isbn_id, gr_book_id
            FROM gr.book_isbn {self.limit}
        '''

    def q_gr_work_edges(self):
        return f'''
            SELECT DISTINCT gr_book_id, gr_work_id
            FROM gr.book_isbn {self.limit}
            JOIN gr.book_ids ids USING (gr_book_id)
            WHERE ids.gr_work_id IS NOT NULL
        '''

    def load_graph(self, cxn, full=False):
        if full:
            gb = FullGraphBuilder()
        else:
            gb = MinGraphBuilder()

        _log.info('fetching ISBNs')
        isbns = pd.read_sql_query(self.q_isbns(full), cxn)
        isbn_nodes = gb.add_nodes(isbns.rename({'isbn': 'label'}), ns_isbn)

        _log.info('fetching LOC records')
        loc_recs = pd.read_sql_query(self.q_loc_nodes(full), cxn)
        loc_nodes = gb.add_nodes(loc_recs, ns_loc_rec)

        _log.info('fetching LOC ISBN links')
        loc_edges = pd.read_sql_query(self.q_loc_edges(), cxn)
        gb.add_edges(loc_edges, isbn_nodes, loc_nodes)

        _log.info('fetching OL editions')
        ol_eds = pd.read_sql_query(self.q_ol_edition_nodes(full), cxn)
        ol_e_nodes = gb.add_nodes(ol_eds, ns_edition)

        _log.info('fetching OL works')
        ol_wks = pd.read_sql_query(self.q_ol_work_nodes(full), cxn)
        ol_w_nodes = gb.add_nodes(ol_wks, ns_work)

        _log.info('fetching OL ISBN edges')
        ol_ie_edges = pd.read_sql_query(self.q_ol_edition_edges(), cxn)
        gb.add_edges(ol_ie_edges, isbn_nodes, ol_e_nodes)

        _log.info('fetching OL edition/work edges')
        ol_ew_edges = pd.read_sql_query(self.q_ol_work_edges(), cxn)
        gb.add_edges(ol_ew_edges, ol_e_nodes, ol_w_nodes)

        _log.info('fetching GR books')
        gr_books = pd.read_sql_query(self.q_gr_book_nodes(full), cxn)
        gr_b_nodes = gb.add_nodes(gr_books, ns_gr_book)

        _log.info('fetching GR ISBN edges')
        gr_ib_edges = pd.read_sql_query(self.q_gr_book_edges(), cxn)
        gb.add_edges(gr_ib_edges, isbn_nodes, gr_b_nodes)

        _log.info('fetching GR works')
        gr_works = pd.read_sql_query(self.q_gr_work_nodes(full), cxn)
        gr_w_nodes = gb.add_nodes(gr_works, ns_gr_work)

        _log.info('fetching GR work/edition edges')
        gr_bw_edges = pd.read_sql_query(self.q_gr_work_edges(), cxn)
        gb.add_edges(gr_bw_edges, gr_b_nodes, gr_w_nodes)

        g = gb.finish()
        _log.info('imported %s', g)

        return g
