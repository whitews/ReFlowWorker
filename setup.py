from setuptools import setup

setup(
    name='ReFlowWorker',
    version='0.2.0',
    author='Scott White',
    author_email='scott.white@duke.edu',
    packages=['reflowworker'],
    url='https://github.com/whitews/ReFlowWorker',
    license='LICENSE.txt',
    description='Client side worker for a ReFlow server for distributed processing',
    install_requires=[
        'requests (>=1.1.0)',
        'numpy (>=1.6)',
        'pycuda (>=2013.1.1)',
        'reflowrestclient (==0.2.0)',
        'flowio (==0.6)',
        'flowutils (==0.3)',
        'flowstats (==0.4)'
    ]
)