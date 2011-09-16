from datetime import datetime
import logging

from spendb.core import db
from spendb.model.dataset import Dataset

class DatasetLogRecord(db.Model):
    __tablename__ = 'log_record'

    id = db.Column(db.Integer, primary_key=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    logger = db.Column(db.Unicode())
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    level = db.Column(db.Unicode())
    file_name = db.Column(db.Unicode())
    module = db.Column(db.Unicode())
    line_number = db.Column(db.Unicode())
    message = db.Column(db.Unicode())
    func = db.Column(db.Unicode())

    dataset = db.relationship(Dataset,
        backref=db.backref('log_events', lazy='dynamic'))

    def __init__(self):
        pass

    def __repr__(self):
        return "<DatasetLogRecord(%s,%s)>" % (self.dataset.name,
                                              self.message)

class DatasetLogger(logging.Logger):

    def __init__(self, dataset, level=logging.DEBUG):
        self.dataset = dataset
        self.name = dataset.name
        self.level = level

    def handle(self, record):
        dlr = DatasetLogRecord()
        dlr.dataset_id = self.dataset.id
        dlr.name = record.name
        dlr.message = record.getMessage()
        dlr.level = record.levelname
        dlr.file_name = record.filename
        dlr.module = record.module
        dlr.line_number = record.lineno
        dlr.func = record.funcName
        
        #session = db.create_scoped_session()
        #session.begin()
        #session = db.Session(bind=db.engine)
        db.session.add(dlr)
        db.session.flush()
        #session.commit()

