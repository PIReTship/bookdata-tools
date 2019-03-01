from io import StringIO
from psycopg2 import errorcodes

import support as s


def test_single_query():
    query = 'SELECT * FROM pizza'
    script = s.SqlScript(StringIO(query))
    assert len(script.chunks) == 1
    assert script.chunks[0].allowed_errors == []
    assert script.chunks[0].src == query


def test_two_queries():
    query = '''
SELECT * FROM pizza;
---
SELECT fish FROM chips;
'''
    script = s.SqlScript(StringIO(query))
    assert len(script.chunks) == 2
    assert script.chunks[0].src.strip() == 'SELECT * FROM pizza;'
    assert script.chunks[0].allowed_errors == []
    assert script.chunks[1].src.strip() == 'SELECT fish FROM chips;'
    assert script.chunks[1].allowed_errors == []


def test_label_query():
    query = '''
--- #step Select from pizza
SELECT * FROM pizza;
'''
    script = s.SqlScript(StringIO(query))
    assert len(script.chunks) == 1
    assert script.chunks[0].src.strip() == 'SELECT * FROM pizza;'
    assert script.chunks[0].allowed_errors == []
    assert script.chunks[0].label == 'Select from pizza'


def test_allow_errors():
    query = '''
--- #allow invalid_object_definition
SELECT * FROM pizza;
'''
    script = s.SqlScript(StringIO(query))
    assert len(script.chunks) == 1
    assert script.chunks[0].src.strip() == 'SELECT * FROM pizza;'
    assert script.chunks[0].allowed_errors == [errorcodes.INVALID_OBJECT_DEFINITION]


def test_allow_multiple():
    query = '''
--- #allow invalid_object_definition
SELECT * FROM pizza;
--- #allow invalid_object_definition
--- #allow duplicate_function
CREATE FUNCTION foobar;
'''
    script = s.SqlScript(StringIO(query))
    assert len(script.chunks) == 2
    assert script.chunks[0].src.strip() == 'SELECT * FROM pizza;'
    assert script.chunks[0].allowed_errors == [errorcodes.INVALID_OBJECT_DEFINITION]

    assert script.chunks[1].src.strip() == 'CREATE FUNCTION foobar;'
    assert script.chunks[1].allowed_errors == \
        [errorcodes.INVALID_OBJECT_DEFINITION, errorcodes.DUPLICATE_FUNCTION]
