#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-okta",
    version="0.0.5",
    description="Singer.io tap for extracting Okta data",
    author="Kandasamy",
    url="http://github.com/singer-io/tap-okta",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_okta"],
    install_requires=[
        'requests==2.20.0',
        'singer-python==5.6.0',
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-okta=tap_okta:main
    """,
    packages=["tap_okta"],
    package_data = {
        "schemas": ["tap_okta/schemas/*.json"]
    },
    include_package_data=True,
)
