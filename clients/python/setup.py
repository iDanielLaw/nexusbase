from setuptools import setup, find_packages

setup(
    name='nexusbase-client',
    version='0.1.0',
    author='INLOpen',
    author_email='nexusbase@inl.in.th',
    description='A Python client for NexusBase',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/INLOpen/nexusbase',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'grpcio>=1.60.0',
        'grpcio-tools>=1.60.0',
        'protobuf>=4.25.0',
    ],
)