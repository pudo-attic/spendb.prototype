import json
import os

from flaskext.script import Manager
from flaskext.celery import install_commands as install_celery_commands

from spendb.core import app, db
from spendb.model import Dataset, DatasetLogger, Source

manager = Manager(app)
install_celery_commands(manager)

@manager.command
def createdb():
    """ Create the SQLAlchemy database. """
    db.create_all()

def _get_ds(name):
    ds = Dataset.query.filter_by(name=name).first()
    if ds is None:
        raise ValueError("Dataset does not exist: %s" % name)
    return ds

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
    ds = _get_ds(dataset)
    ds.generate()
    ds.drop()
    db.session.delete(ds)
    db.session.commit()

@manager.command
def dsflush(dataset):
    """ Flush all data from a dataset. """
    ds = _get_ds(dataset)
    ds.generate()
    ds.flush()
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

@manager.command
def srcadd(dataset, filename):
    """ Add a source file to a dataset. """
    ds = _get_ds(dataset)
    src = Source(ds, 'file', filename)
    for other in ds.sources:
        if other.name == src.name:
            raise ValueError("Source already exists: %s" % other.name)
    db.session.add(src)
    import shutil
    print "Copying to %s..." % src.staging_path
    shutil.copyfile(filename, src.staging_path)
    db.session.commit()

@manager.command
def srclist(dataset):
    """ List all source files for a dataset. """
    ds = _get_ds(dataset)
    fmt = " %-5s | %-55s | %-20s"
    print fmt % ('id', 'name', 'type')
    print '-' * 80
    for source in ds.sources:
        m = fmt % (source.id, source.name, source.type)
        print m.encode('utf-8')

@manager.command
def srcrm(dataset, source):
    """ Remove a source from a dataset. """
    ds = _get_ds(dataset)
    for src in ds.sources:
        if src.name == source:
            db.session.delete(src)
            if os.path.exists(src.staging_path):
                os.remove(src.staging_path)
            db.session.commit()

def spendb():
    manager.run()

if __name__ == '__main__':
    spendb()
