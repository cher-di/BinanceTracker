import os

import asyncpg
import pytest

from datetime import datetime

from binance.collector.db import DB


TESTS_ROOT = os.path.dirname(os.path.abspath(__file__))
COLLECTOR_ROOT = os.path.dirname(TESTS_ROOT)
CREATE_TABLES_SCRIPT = os.path.join(COLLECTOR_ROOT, 'sql/create_tables.sql')


@pytest.fixture
async def conn():
    conn = await asyncpg.connect(
        host=os.environ['TEST_BINANCE_DB_HOST'],
        port=os.environ['TEST_BINANCE_DB_PORT'],
        user=os.environ['TEST_BINANCE_DB_USER'],
        password=os.environ['TEST_BINANCE_DB_PASSWORD'],
        database=os.environ['TEST_BINANCE_DB_NAME'],
    )
    await conn.execute('''
        DROP TABLE IF EXISTS labels CASCADE;
        DROP TABLE IF EXISTS samples CASCADE;
    ''')
    with open(CREATE_TABLES_SCRIPT, 'r', encoding='utf-8') as f:
        await conn.execute(f.read())
    try:
        yield conn
    finally:
        await conn.execute('''
            DROP TABLE labels CASCADE;
            DROP TABLE samples CASCADE;
        ''')
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('label', 'measurement_time', 'value'),
    [
        ('WINUSDT', datetime(2021, 11, 25, 12, 39, 15), 192.359),
    ]
)
async def test_insert_value_correct(label, measurement_time, value, conn):
    await conn.execute('INSERT INTO labels (name) VALUES ($1)', label)
    db = DB(conn)
    await db.insert_value(label, measurement_time, value)
    expected_label_id = await conn.fetchval(
        'SELECT label_id FROM labels WHERE (name = $1)', label)
    actual_row = await conn.fetchrow('SELECT * FROM samples')
    assert actual_row['label_id'] == expected_label_id
    assert actual_row['measurement_time'] == measurement_time
    assert abs(actual_row['value'] - value) < 0.1


@pytest.mark.asyncio
async def test_insert_value_wrong_label(conn):
    db = DB(conn)
    with pytest.raises(ValueError):
        await db.insert_value('test_label', datetime.now(), 1928.38942)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'labels',
    [
        ('label1', 'label2', 'label3'),
    ]
)
async def test_get_labels(labels, conn):
    await conn.executemany('INSERT INTO labels (name) VALUES ($1)',
                           [(label,) for label in labels])
    db = DB(conn)
    actual_labels = await db.get_labels()
    assert sorted(actual_labels) == sorted(labels)
