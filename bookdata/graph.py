"""
Utiltiies for loading & working with the book identifier graph.
"""

import logging

import pandas as pd
import numpy as np
from graph_tool import Graph
from .schema import *

_log = logging.getLogger(__name__)

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

    def q_isbns(self):
        return f'SELECT isbn_id AS id, isbn FROM {self.isbn_table}'

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
                SELECT DISTINCT gr_work_id AS id, work_title
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

    def load_minimal_graph(self, cxn):
        g = Graph(directed=False)
        codes = []
        sources = []

        def add_nodes(df, ns, src):
            n = len(df)
            _log.info('adding %d nodes to graph', n)
            start = g.num_vertices()
            vs = g.add_vertex(n)
            end = g.num_vertices()
            assert end - start == n
            nodes = pd.Series(np.arange(start, end, dtype='i4'), index=df['id'])
            codes.append(df['id'].values + ns)
            sources.append(np.full(n, src, dtype='i2'))
            return nodes

        def add_edges(f, src, dst):
            _log.info('adding %d edges to graph', len(f))
            edges = np.zeros((len(f), 2), dtype='i4')
            edges[:, 0] = src.loc[f.iloc[:, 0]]
            edges[:, 1] = dst.loc[f.iloc[:, 1]]
            g.add_edge_list(edges)

        _log.info('fetching ISBNs')
        isbns = pd.read_sql_query(self.q_isbns(), cxn)
        isbn_nodes = add_nodes(isbns.drop(columns=['isbn']), ns_isbn, 9)

        _log.info('fetching LOC records')
        loc_recs = pd.read_sql_query(self.q_loc_nodes(False), cxn)
        loc_nodes = add_nodes(loc_recs, ns_rec, 3)

        _log.info('fetching LOC ISBN links')
        loc_edges = pd.read_sql_query(self.q_loc_edges(), cxn)
        add_edges(loc_edges, isbn_nodes, loc_nodes)

        _log.info('fetching OL editions')
        ol_eds = pd.read_sql_query(self.q_ol_edition_nodes(False), cxn)
        ol_e_nodes = add_nodes(ol_eds, ns_edition, 2)

        _log.info('fetching OL works')
        ol_wks = pd.read_sql_query(self.q_ol_work_nodes(False), cxn)
        ol_w_nodes = add_nodes(ol_wks, ns_work, 1)

        _log.info('fetching OL ISBN edges')
        ol_ie_edges = pd.read_sql_query(self.q_ol_edition_edges(), cxn)
        add_edges(ol_ie_edges, isbn_nodes, ol_e_nodes)

        _log.info('fetching OL edition/work edges')
        ol_ew_edges = pd.read_sql_query(self.q_ol_work_edges(), cxn)
        add_edges(ol_ew_edges, ol_e_nodes, ol_w_nodes)

        _log.info('fetching GR books')
        gr_books = pd.read_sql_query(self.q_gr_book_nodes(False), cxn)
        gr_b_nodes = add_nodes(gr_books, ns_gr_book, 5)

        _log.info('fetching GR ISBN edges')
        gr_ib_edges = pd.read_sql_query(self.q_gr_book_edges(), cxn)
        add_edges(gr_ib_edges, isbn_nodes, gr_b_nodes)

        _log.info('fetching GR works')
        gr_works = pd.read_sql_query(self.q_gr_work_nodes(False), cxn)
        gr_w_nodes = add_nodes(gr_works, ns_gr_work, 4)

        _log.info('fetching GR work/edition edges')
        gr_bw_edges = pd.read_sql_query(self.q_gr_work_edges(), cxn)
        add_edges(gr_bw_edges, gr_b_nodes, gr_w_nodes)

        _log.info('setting code attributes')
        code_a = g.new_vp('int64_t')
        code_a.a[:] = np.concatenate(codes)
        g.vp['code'] = code_a

        _log.info('setting source attributes')
        source_a = g.new_vp('int16_t')
        source_a.a[:] = np.concatenate(sources)
        g.vp['source'] = source_a

        _log.info('imported %s', g)

        return g
