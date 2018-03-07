from setuptools import setup

setup(name='isc_orm',
      version='1.0',
      description='Object Relational Model for Blue Waters isc Database',
      author='Aaron Saxton',
      author_email='saxton@illinois.edu',
      url='',
      packages=[
        'isc_orm',
        ],
      install_requires=[
        'MySQL-python==1.2.5',
        'SQLAlchemy==1.1.13',
        'tqdm==4.15.0',
        'jupyter==1.0.0',
        ]
     )
