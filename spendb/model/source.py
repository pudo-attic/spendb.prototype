import os

from werkzeug import secure_filename

from spendb.core import db
from spendb.model.dataset import Dataset

class Source(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.Unicode())
    name = db.Column(db.Unicode())
    description = db.Column(db.Unicode())

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    dataset = db.relationship(Dataset,
                    backref=db.backref('sources', lazy='dynamic'))

    def __init__(self, dataset, type, name, description=None):
        self.dataset = dataset
        self.type = type
        self.name = secure_filename(os.path.basename(name))
        self.description = description

    @property
    def staging_path(self):
        from flask import current_app
        directory = os.path.join(current_app.config['STAGING_DATA_PATH'], 
                            self.dataset.name)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        return os.path.join(directory, self.name)

    def __repr__(self):
        return "<Source(%s,%s)>" % (self.dataset.name, self.name)



