import json 
from itertools import chain
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import Integer, UnicodeText, Float
from sqlalchemy.sql import and_, or_, func
from sqlalchemy.schema import Table, MetaData, Column
from migrate.versioning.util import construct_engine
from pprint import pprint
from collections import defaultdict
from datetime import datetime

class TableHandler(object):

    def _ensure_table(self, meta, name):
        if not meta.bind.has_table(name):
            self.table = Table(name, meta)
            col = Column('id', Integer, primary_key=True)
            self.table.append_column(col)
            self.table.create(meta.bind)
        else:
            self.table = Table(name, meta, autoload=True)

    def _upsert(self, bind, data, unique_columns):
        key = and_(*[self.table.c[c]==data.get(c) for c in unique_columns])
        q = self.table.update(key, data)
        if bind.execute(q).rowcount == 0:
            q = self.table.insert(data)
            rs = bind.execute(q)
            return rs.inserted_primary_key[0]
        else:
            q = self.table.select(key)
            row = bind.execute(q).fetchone()
            return row['id']

class Attribute(object):

    def __init__(self, dataset, data):
        self._data = data
        self.dataset = dataset
        self.name = data.get('name')
        self.source_column = data.get('column')
        self.default = data.get('default', data.get('constant'))
        self.description = data.get('description')
        self.datatype = data.get('datatype')

    @property
    def selectable(self):
        return self.column

    def generate(self, meta, table):
        if self.name in table.c:
            self.column = table.c[self.name]
            return
        types = {
            'string': UnicodeText,
            'constant': UnicodeText,
            'date': UnicodeText,
            'float': Float,
                }
        type_ = types.get(self.datatype, UnicodeText)
        self.column = Column(self.name, type_)
        self.column.create(table)

    def load(self, bind, row):
        value = row.get(self.source_column, self.default) if \
                self.source_column else self.default
        return {self.column.name: value.decode('utf-8') if value else None}

    def __repr__(self):
        return "<Attribute(%s)>" % self.name

class Dimension(object):

    def __init__(self, dataset, name, data):
        self._data = data
        self.dataset = dataset
        self.name = name
        self.label = data.get('label', name)
        self.facet = data.get('facet')
    
    def join(self, from_clause):
        return from_clause

    def __getitem__(self, name):
        raise KeyError()

    def __repr__(self):
        return "<Dimension(%s)>" % self.name

class ValueDimension(Dimension, Attribute):

    def __init__(self, dataset, name, data):
        Attribute.__init__(self, dataset, data)
        Dimension.__init__(self, dataset, name, data)
    
    def __repr__(self):
        return "<ValueDimension(%s)>" % self.name

class Metric(Attribute):

    def __init__(self, dataset, name, data):
        Attribute.__init__(self, dataset, data)
        self.name = name
        self.label = data.get('label', name)

    def join(self, from_clause):
        return from_clause

    def __getitem__(self, name):
        raise KeyError()

    def __repr__(self):
        return "<Metric(%s)>" % self.name

class ComplexDimension(Dimension, TableHandler):

    def __init__(self, dataset, name, data):
        Dimension.__init__(self, dataset, name, data)
        self.scheme = data.get('scheme', data.get('taxonomy', 'entity'))
        self.attributes = []
        for attr in data.get('attributes', data.get('fields', [])):
            self.attributes.append(Attribute(dataset, attr))

    def join(self, from_clause):
        return from_clause.join(self.alias, self.alias.c.id==self.column)

    @property
    def selectable(self):
        return self.alias

    def __getitem__(self, name):
        for attr in self.attributes:
            if attr.name == name:
                return attr
        raise KeyError()

    def generate(self, meta, entry_table):
        self._ensure_table(meta, self.scheme)
        for attr in self.attributes:
            attr.generate(meta, self.table)
        fk = self.name + '_id'
        if not fk in entry_table.c:
            self.column = Column(self.name + '_id', Integer)
            self.column.create(entry_table)
        else:
            self.column = entry_table.c[fk]
        self.alias = self.table.alias(self.name)

    def load(self, bind, row):
        dim = dict()
        for attr in self.attributes:
            dim.update(attr.load(bind, row))
        #pprint(dim)
        pk = self._upsert(bind, dim, ['name'])
        return {self.column.name: pk}

    def __repr__(self):
        return "<ComplexDimension(%s/%s:%s)>" % (self.scheme, self.name, 
                                                 self.attributes)


class Dataset(TableHandler):

    def __init__(self, data):
        self._data = data
        dataset = data.get('dataset', {})
        self.label = dataset.get('label')
        self.name = dataset.get('name')
        self.description = dataset.get('description')
        self.currency = dataset.get('currency')

        self.dimensions = []
        self.metrics = []
        for dim, data in data.get('mapping', {}).items():
            if data.get('type') == 'metric' or dim == 'amount':
                self.metrics.append(Metric(self, dim, data))
                continue
            elif data.get('type', 'value') == 'value':
                dimension = ValueDimension(self, dim, data)
            else:
                dimension = ComplexDimension(self, dim, data)
            self.dimensions.append(dimension)

    def __getitem__(self, name):
        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError()

    @property
    def fields(self):
        return self.dimensions + self.metrics

    def generate(self, meta):
        self._ensure_table(meta, 'entry')
        for field in self.fields:
            field.generate(meta, self.table)
        self.alias = self.table.alias('entry')

    def load(self, bind, row):
        entry = dict()
        for field in self.fields:
            entry.update(field.load(bind, row))
        self._upsert(bind, entry, ['id'])

    def load_all(self, bind, rows):
        for row in rows:
            self.load(bind, row)
        #bind.commit()

    def key(self, key):
        attr = None
        if '.' in key:
            key, attr = key.split('.', 1)
        dimension = self[key]
        if hasattr(dimension, 'alias'):
            attr_name = dimension[attr].column.name if attr else 'id'
            return dimension.alias.c[attr_name]
        return self.alias.c[dimension.column.name]

    def materialize(self, bind, conditions="1=1", order_by=None):
        """ Generate a fully denormalized view of the entries on this 
        table. """
        joins = self.alias
        for f in self.fields:
            joins = f.join(joins)
        query = select([f.selectable for f in self.fields], 
                       conditions, joins, order_by=order_by,
                       use_labels=True)
        rp = bind.execute(query)
        while True:
            row = rp.fetchone()
            if row is None:
                break
            result = {}
            for k, v in row.items():
                field, attr = k.split('_', 1)
                if field == 'entry':
                    result[attr] = v
                else:
                    if not field in result:
                        result[field] = dict()
                    result[field][attr] = v
            yield result

    def aggregate(self, bind, metric='amount', drilldowns=None, cuts=None, 
            page=1, pagesize=10000, order=None):

        cuts = cuts or []
        drilldowns = drilldowns or []
        joins = self.alias
        for dimension in set(drilldowns + [k for k,v in cuts]):
            joins = self[dimension.split('.')[0]].join(joins)

        group_by = []
        fields = [func.sum(self.alias.c.amount).label(metric), 
                  func.count(self.alias.c.id).label("entries")]
        for key in drilldowns:
            column = self.key(key)
            if '.' in key or column.table == self.alias:
                fields.append(column)
            else:
                fields.append(column.table)
            group_by.append(column)
     
        conditions = and_()
        filters = defaultdict(set)
        for key, value in cuts:
            column = self.key(key)
            filters[column].add(value)
        for attr, values in filters.items():
            conditions.append(or_(*[attr==v for v in values]))

        order_by = []
        for key, direction in order or []:
            # TODO: handle case in which order criterion is not joined.
            column = self.key(key)
            order_by.append(column.desc() if direction else column.asc())

        query = select(fields, conditions, joins,
                       order_by=order_by or [metric + ' desc'],
                       group_by=group_by, use_labels=True)
        summary = {metric: 0.0, 'num_entries': 0}
        drilldown = []
        rp = bind.execute(query)
        while True:
            row = rp.fetchone()
            if row is None:
                break
            result = {}
            for key, value in row.items():
                if key == metric:
                    summary[metric] += value
                if key == 'entries':
                    summary['num_entries'] += value
                if '_' in key:
                    dimension, attribute = key.split('_', 1)
                    if dimension == 'entry':
                        result[attribute] = value
                    else:
                        if not dimension in result:
                            result[dimension] = {}
                        result[dimension][attribute] = value
                else:
                    if key == 'entries':
                        key = 'num_entries'
                    result[key] = value
            drilldown.append(result)
        offset = ((page-1)*pagesize)
        return {'drilldown': drilldown[offset:offset+pagesize],
                'summary': summary}

    def __repr__(self):
        return "<Dataset(%s:%s:%s)>" % (self.name, self.dimensions,
                self.metrics)
    




def load(file_name, db_url):
    fh = open(file_name, 'r')
    model_data = json.loads(fh.read())
    del model_data['views']
    fh.close()
    #pprint(model_data)
    dataset = Dataset(model_data)

    engine = create_engine(db_url)
    engine = construct_engine(engine)
    meta = MetaData()
    meta.bind = engine
    
    dataset.generate(meta)
    a = dataset.aggregate(engine, drilldowns=['titel', 'flow'],
                          pagesize=10)
    pprint(a)
    print "-" * 80
    a = dataset.aggregate(engine, drilldowns=['titel.name', 'flow'],
                          cuts=[('to', '04'), ('to', '02')], pagesize=5)
    pprint(a)
    #import csv
    #reader = csv.DictReader(open('/Users/fl/Data/bundeshaushalt/bund_2010.csv', 'r'))
    #dataset.load_all(engine, reader)
    
    #pprint(dataset)
    return dataset


import unittest

SIMPLE_MODEL = {
    'dataset': {
        'name': 'test',
        'label': 'Test Case Model',
        'description': 'I\'m a banana!'
    },
    'mapping': {
        'amount': {
            'type': 'value',
            'label': 'Amount',
            'column': 'amount',
            'datatype': 'float'
            },
        'time': {
            'type': 'value',
            'label': 'Year',
            'column': 'year',
            'datatype': 'date'
            },
        'field': {
            'type': 'value',
            'label': 'Field 1',
            'column': 'field',
            'datatype': 'string'
            },
        'to': {
            'label': 'Einzelplan',
            'type': 'entity',
            'facet': True,
            'fields': [
                {'column': 'to_name', 'name': 'name', 'datatype': 'string'},
                {'column': 'to_label', 'name': 'label', 'datatype': 'string'},
                {'constant': 'true', 'name': 'const', 'datatype': 'constant'}
            ]
            },
        'function': {
            'label': 'Function code',
            'type': 'classifier',
            'taxonomy': 'funny',
            'facet': False,
            'fields': [
                {'column': 'func_name', 'name': 'name', 'datatype': 'string'},
                {'column': 'func_label', 'name': 'label', 'datatype': 'string'}
            ]
            }
        }
    }

TEST_DATA="""year,amount,field,to_name,to_label,func_name,func_label
2010,200,foo,"bcorp","Big Corp",food,Food & Nutrition
2009,190,bar,"bcorp","Big Corp",food,Food & Nutrition
2010,500,foo,"acorp","Another Corp",food,Food & Nutrition
2009,900,qux,"acorp","Another Corp",food,Food & Nutrition
2010,300,foo,"ccorp","Central Corp",school,Schools & Education
2009,600,qux,"ccorp","Central Corp",school,Schools & Education
"""

class DatasetTestCase(unittest.TestCase):

    def setUp(self):
        self.ds = Dataset(SIMPLE_MODEL)
        engine = create_engine('sqlite:///:memory:')
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.meta.bind = self.engine

    def test_load_model_properties(self):
        assert self.ds.name==SIMPLE_MODEL['dataset']['name'], self.ds.name
        assert self.ds.label==SIMPLE_MODEL['dataset']['label'], self.ds.label

    def test_load_model_dimensions(self):
        assert len(self.ds.dimensions)==4,self.ds.dimensions
        assert isinstance(self.ds['time'], ValueDimension), self.ds['time']
        assert isinstance(self.ds['field'], ValueDimension), self.ds['field']
        assert isinstance(self.ds['to'], ComplexDimension), self.ds['to']
        assert isinstance(self.ds['function'], ComplexDimension), \
            self.ds['function']
        assert len(self.ds.metrics)==1,self.ds.metrics
        assert isinstance(self.ds['amount'], Metric), self.ds['amount']

    def test_value_dimensions_as_attributes(self):
        self.ds.generate(self.meta)
        dim = self.ds['field']
        assert isinstance(dim.column.type, UnicodeText), dim.column
        assert 'field'==dim.column.name, dim.column
        assert dim.name=='field', dim.name
        assert dim.source_column==SIMPLE_MODEL['mapping']['field']['column'], \
                dim.source_column
        assert dim.label==SIMPLE_MODEL['mapping']['field']['label'], \
                dim.label
        assert dim.default==None, dim.default
        assert dim.dataset==self.ds, dim.dataset
        assert dim.datatype=='string', dim.datatype
        assert not hasattr(dim, 'table')
        assert not hasattr(dim, 'alias')

    def test_generate_db_entry_table(self):
        self.ds.generate(self.meta)
        assert self.ds.table.name=='entry', self.ds.table.name
        assert self.ds.alias.name=='entry', self.ds.alias.name
        cols = self.ds.table.c
        assert 'id' in cols
        assert isinstance(cols['id'].type, Integer)
        # TODO: 
        assert 'time' in cols
        assert isinstance(cols['time'].type, UnicodeText)
        assert 'amount' in cols
        assert isinstance(cols['amount'].type, Float)
        assert 'field' in cols
        assert isinstance(cols['field'].type, UnicodeText)
        assert 'to_id' in cols
        assert isinstance(cols['to_id'].type, Integer)
        assert 'function_id' in cols
        assert isinstance(cols['function_id'].type, Integer)
        self.assertRaises(KeyError, cols.__getitem__, 'foo')


from StringIO import StringIO
import csv
class DatasetLoadTestCase(unittest.TestCase):

    def setUp(self):
        self.ds = Dataset(SIMPLE_MODEL)
        engine = create_engine('sqlite:///:memory:')
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.ds.generate(self.meta)
        self.reader = csv.DictReader(StringIO(TEST_DATA))
    
    def test_load_all(self):
        self.ds.load_all(self.engine, self.reader)
        resn = self.engine.execute(self.ds.table.select()).fetchall()
        assert len(resn)==6,resn
        row0 = resn[0]
        assert row0['time']=='2010', row0.items()
        assert row0['amount']==200, row0.items()
        assert row0['field']=='foo', row0.items()

    def test_aggregate_simple(self):
        self.ds.load_all(self.engine, self.reader)
        res = self.ds.aggregate(self.engine)
        assert res['summary']['num_entries']==6, res
        assert res['summary']['amount']==2690.0, res

    def test_aggregate_basic_cut(self):
        self.ds.load_all(self.engine, self.reader)
        res = self.ds.aggregate(self.engine, cuts=[('field', u'foo')])
        assert res['summary']['num_entries']==3, res
        assert res['summary']['amount']==1000, res

    def test_aggregate_or_cut(self):
        self.ds.load_all(self.engine, self.reader)
        res = self.ds.aggregate(self.engine, cuts=[('field', u'foo'), 
                                                   ('field', u'bar')])
        assert res['summary']['num_entries']==4, res
        assert res['summary']['amount']==1190, res
    
    def test_aggregate_dimensions_drilldown(self):
        self.ds.load_all(self.engine, self.reader)
        res = self.ds.aggregate(self.engine, drilldowns=['function'])
        assert res['summary']['num_entries']==6, res
        assert res['summary']['amount']==2690, res
        assert len(res['drilldown'])==2, res['drilldown']
    
    def test_aggregate_two_dimensions_drilldown(self):
        self.ds.load_all(self.engine, self.reader)
        res = self.ds.aggregate(self.engine, drilldowns=['function', 'field'])
        #pprint(res)
        assert res['summary']['num_entries']==6, res
        assert res['summary']['amount']==2690, res
        assert len(res['drilldown'])==5, res['drilldown']
    
    def test_materialize_table(self):
        self.ds.load_all(self.engine, self.reader)
        itr = self.ds.materialize(self.engine)
        tbl = list(itr)
        assert len(tbl)==6, tbl
        row = tbl[0]
        assert isinstance(row['field'], unicode), row
        assert isinstance(row['function'], dict), row
        assert isinstance(row['to'], dict), row

class ComplexDimensionTestCase(unittest.TestCase):

    def setUp(self):
        self.ds = Dataset(SIMPLE_MODEL)
        engine = create_engine('sqlite:///:memory:')
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.entity = self.ds['to']
        self.classifier = self.ds['function']

    def test_basic_properties(self):
        self.ds.generate(self.meta)
        assert self.entity.name=='to', self.entity.name
        assert self.classifier.name=='function', self.classifier.name
        assert self.entity.scheme=='entity', self.entity.scheme
        assert self.classifier.scheme=='funny', self.classifier.scheme
        
    def test_generated_tables(self):
        assert not hasattr(self.entity, 'table'), self.entity
        self.ds.generate(self.meta)
        assert hasattr(self.entity, 'table'), self.entity
        assert self.entity.table.name==self.entity.scheme, self.entity.table.name
        assert hasattr(self.entity, 'alias')
        assert self.entity.alias.name==self.entity.name, self.entity.alias.name
        cols = self.entity.table.c
        assert 'id' in cols
        self.assertRaises(KeyError, cols.__getitem__, 'field')

    def test_attributes_exist_on_object(self):
        assert len(self.entity.attributes)==3, self.entity.attributes
        self.assertRaises(KeyError, self.entity.__getitem__, 'field')
        assert self.entity['name'].name=='name'
        assert self.entity['name'].datatype=='string'
        assert self.entity['const'].default=='true'

    def test_attributes_exist_on_table(self):
        self.ds.generate(self.meta)
        assert hasattr(self.entity, 'table'), self.entity
        assert 'name' in self.entity.table.c, self.entity.table.c
        assert 'label' in self.entity.table.c, self.entity.table.c


if __name__ == '__main__':
    unittest.main()
    #ds = load('example.js', 'sqlite:///example.db')
