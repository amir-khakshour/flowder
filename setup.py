#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Twisted>=8.0',
    'Scrapy>=0.17',
]

test_requirements = [
    'nose',
]

setup(
    name='flowder',
    version='0.1.0',
    description="A service daemon to download files asynchronously. ",
    long_description=readme + '\n\n' + history,
    author="Amir Khakshour",
    author_email='khakshour.amir@gmail.com',
    url='https://github.com/amir-khakshour/flowder',
    packages=[
        'flowder',
    ],
    package_dir={'flowder':
                 'flowder'},
    entry_points={
        'console_scripts': [
            'flowder=flowder.cli:main'
        ]
    },
    scripts=['bin/flowder'],
    include_package_data=True,
    install_requires=requirements,
    dependency_links=[
        "git+ssh://git@github.com/amir-khakshour/pygear",
    ],
    license="MIT license",
    zip_safe=False,
    keywords='flowder',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='nose.collector',
    tests_require=test_requirements
)
