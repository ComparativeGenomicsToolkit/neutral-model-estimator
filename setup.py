from setuptools import setup

setup(
    name='neutral-model-estimator',
    version='0.1',
    packages=['neutral_model_estimator'],
    install_requires=[
        'luigi>=2.5',
        'toil==3.7',
        'python-dateutil==2.7.5',
        'more-itertools<6',
    ],
    license='MIT'
)
