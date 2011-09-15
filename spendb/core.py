# shut up useless SA warning:
import warnings; warnings.filterwarnings('ignore', 'Unicode type received non-unicode bind param value.')

from flask import Flask
from flaskext.sqlalchemy import SQLAlchemy
from migrate.versioning.util import construct_engine

from spendb import default_settings

import flaskext.sqlalchemy as fsqla 

def mig_create_engine(*a, **kw):
    engine = fsqla.create_engine(*a, **kw)
    return construct_engine(engine)
fsqla.create_engine = mig_create_engine

app = Flask(__name__)
app.config.from_object(default_settings)
app.config.from_envvar('SPENDB_SETTINGS', silent=True)

db = SQLAlchemy(app)

#import ipdb; ipdb.set_trace()

