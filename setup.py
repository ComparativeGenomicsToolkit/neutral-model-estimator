from setuptools import setup

setup(
    name='neutral-model-estimator',
    version='0.1',
    packages=['neutral_model_estimator'],
    install_requires=[
        'luigi>=2.5',
        'toil>=3.7'
    ],
    license='MIT'
)
