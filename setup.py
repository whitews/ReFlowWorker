from setuptools import setup

setup(
    name='ReFlowWorker',
    version='0.0.8',
    author='Scott White',
    author_email='scott.white@duke.edu',
    packages=['reflowworker'],
    url='https://github.com/whitews/ReFlowWorker',
    license='LICENSE.txt',
    description='Client side worker for a ReFlow server for distributed processing',
    install_requires=[
        'requests>=1.1.0',
        'numpy>=1.6',
        'reflowrestclient',
        'flowio',
        'flowutils',
        'flowstats'
    ]
)