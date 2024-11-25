from setuptools import setup, find_packages

setup(
    name='external_api_utils',
    version='0.1.0',
    description='A core library to access resources from third party APIs',
    author='Ioannis Tsakmakis, Nikolaos Kokkos',
    author_email='itsakmak@envrio.org, nkokkos@envrio.org',
    packages=find_packages(),
    python_requires='>=3.12',
    install_requires=[  
        'requests>=2.32.3',
        'xmltodict>=0.14.2',
        'pandas>=2.2.3'
    ],
    classifiers=[  
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.12',
        'Framework :: Flask',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
