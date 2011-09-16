import json

from flaskext.script import Manager
from flaskext.celery import install_commands as install_celery_commands

from spendb.core import app, db
from spendb.model import Dataset, DatasetLogger

manager = Manager(app)
install_celery_commands(manager)

@manager.command
def createdb():
    """ Create the SQLAlchemy database. """
    db.create_all()

@manager.command
def dsinit(filename):
    """ Load a JSON file and generate a dataset environment. """
    fh = open(filename, 'r')
    data = json.load(fh)
    fh.close()
    dataset = Dataset(data)
    old = Dataset.query.filter_by(name=dataset.name).first()
    if old is not None:
        raise ValueError("A dataset named %s already exists!" % old.name)
    db.session.add(dataset)
    dataset.generate()
    db.session.flush()
    log = DatasetLogger(dataset)
    log.info("Dataset created: %s", dataset.label)
    db.session.commit()

@manager.command
def dsdrop(dataset):
    """ Drop a dataset and all associated objects. """
    ds = Dataset.query.filter_by(name=dataset).first()
    if ds is None:
        raise ValueError("Dataset does not exist: %s" % dataset)
    ds.generate()
    ds.drop()
    db.session.delete(ds)
    db.session.commit()

@manager.command
def dslist():
    """ List all datasets in the databse. """
    fmt = " %-5s | %-25s | %-45s"
    print fmt % ('id', 'name', 'label')
    print '-' * 80
    for dataset in Dataset.query:
        m = fmt % (dataset.id, dataset.name, dataset.label)
        print m.encode('utf-8')

def spendb():
    manager.run()

if __name__ == '__main__':
    spendb()
