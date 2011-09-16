from setuptools import setup, find_packages

version = '0.1'

setup(name='spendb',
      version=version,
      description="Light-weight OpenSpending clone",
      long_description="",
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database"],
      keywords='spending finance government aggregation cube sql etl',
      author='Friedrich Lindenberg',
      author_email='frierich.lindenberg@okfn.org',
      url='http://pudo.org',
      license='GPLv3',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          "flask==0.7.2",
          "sqlalchemy==0.7.2",
          "flask-sqlalchemy==0.15",
          "sqlalchemy-migrate==0.7.1",
          "flask-celery=2.2.5",
          "kombu-sqlalchemy=1.1.0",

          "nose>=1.1.2",
          "ipython>=0.11"
      ],
      entry_points="""
      # -*- Entry points: -*-
      [console_scripts]
      spendb = spendb.manage:spendb
      """,
      )
