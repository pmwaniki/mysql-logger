import sqlite3
import os

from settings import base_dir


class KeyValueStore(dict):
    def __init__(self):
        self.conn = sqlite3.connect(os.path.join(base_dir,'store.sqlite'))
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv (key text unique, value text)")
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()

    def __len__(self):
        rows = self.conn.execute('SELECT COUNT(*) FROM kv').fetchone()[0]
        return rows if rows is not None else 0

    def iterkeys(self):
        c = self.conn.cursor()
        for row in c.execute('SELECT key FROM kv'):
            yield row[0]

    def itervalues(self):
        c = self.conn.cursor()
        for row in c.execute('SELECT value FROM kv'):
            yield row[0]

    def iteritems(self):
        c = self.conn.cursor()
        for row in c.execute('SELECT key, value FROM kv'):
            yield row[0], row[1]

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def __contains__(self, key):
        return self.conn.execute('SELECT 1 FROM kv WHERE key = ?', (key,)).fetchone() is not None

    def __getitem__(self, key):
        item = self.conn.execute('SELECT value FROM kv WHERE key = ?', (key,)).fetchone()
        if item is None:
            raise KeyError(key)
        return item[0]

    def __setitem__(self, key, value):
        self.conn.execute('REPLACE INTO kv (key, value) VALUES (?,?)', (key, value))
        self.conn.commit()

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.conn.execute('DELETE FROM kv WHERE key = ?', (key,))
        self.conn.commit()

    def __iter__(self):
        return self.iterkeys()