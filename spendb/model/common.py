from spendb.core import db 

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
        self.table.drop()
        del self.table


