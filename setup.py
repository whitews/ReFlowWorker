from setuptools import setup

setup(
    name='ReFlowWorker',
    version='0.3',
    author='Scott White',
    author_email='scott.white@duke.edu',
    packages=['reflowworker'],
    url='https://github.com/whitews/ReFlowWorker',
    license='LICENSE.txt',
    description='Client side worker for a ReFlow server for distributed processing',
    install_requires=[
        'requests (>=1.1.0)',
        'numpy (>=1.8)',
        'pycuda (>=2015.1.3)',
        'reflowrestclient (==0.3)',
        'flowio (==0.6)',
        'flowutils (==0.3)',
        'flowstats (==0.7)'
    ]
)