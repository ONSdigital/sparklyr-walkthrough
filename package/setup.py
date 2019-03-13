from distutils.core import setup

setup(
    name='lib',
    version='0.1.0',
    packages=['lib',],
    entry_points = {
        'console_scripts': ['download_mot_data=lib.source:download_mot_data'],
    }
)