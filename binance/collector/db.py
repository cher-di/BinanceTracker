import asyncpg

from datetime import datetime


class DB:
    def __init__(self, conn: asyncpg.Connection):
        self._conn = conn

    async def insert_value(self, label: str, measurement_time: datetime,
                           value: float):
        label_id = await self._conn.fetchval(
            'SELECT label_id FROM labels WHERE (name = $1)', label)
        if not label_id:
            raise ValueError('No such label in database: {}'.format(label))
        await self._conn.execute('''
            INSERT INTO samples (label_id, measurement_time, value)
            VALUES ($1, $2, $3)
            ''', label_id, measurement_time, value)

    async def get_labels(self):
        return [row['name'] for row in
                await self._conn.fetch('SELECT name FROM labels')]
