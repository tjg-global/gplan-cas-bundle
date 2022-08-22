import os, sys
import glob
import setuptools

setuptools.setup(
    name='gbundle',
    version='1.0',
    description='Global gplan-cas Bundle',
    author='Tim Golden',
    author_email='tim.golden@global.com',
    packages = ["gbundle"],
    install_requires=[
        'gitpython',
        'pyodbc'
    ],
    entry_points = {
        "console_scripts" : [
            "gbundle=gbundle.gbundle:command_line",
        ]
    }
)
