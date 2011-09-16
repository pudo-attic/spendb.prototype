from json import dumps, loads
from sqlalchemy.types import Text, MutableType, TypeDecorator

from spendb.core import db 

class JSONType(MutableType, TypeDecorator):
    impl = Text

    def __init__(self):
        super(JSONType, self).__init__()

    def process_bind_param(self, value, dialect):
        return dumps(value)

    def process_result_value(self, value, dialiect):
        return loads(value)

    def copy_value(self, value):
        return loads(dumps(value))

class TableHandler(object):

    def _ensure_table(self, meta, name):
        if not meta.bind.has_table(name):
            self.table = db.Table(name, meta)
            col = db.Column('id', db.Integer, primary_key=True)
            self.table.append_column(col)
            self.table.create(meta.bind)
        else:
            self.table = db.Table(name, meta, autoload=True)

    def _upsert(self, bind, data, unique_columns):
        key = db.and_(*[self.table.c[c]==data.get(c) for c in unique_columns])
        q = self.table.update(key, data)
        if bind.execute(q).rowcount == 0:
            q = self.table.insert(data)
            rs = bind.execute(q)
            return rs.inserted_primary_key[0]
        else:
            q = self.table.select(key)
            row = bind.execute(q).fetchone()
            return row['id']

    def _flush(self, bind):
        q = self.table.delete()
        bind.execute(q)

    def _drop(self, bind):
        if bind.has_table(self.table.name):
            self.table.drop()
        del self.table


