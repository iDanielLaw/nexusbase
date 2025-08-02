from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nbql-client",
    version="0.1.0",
    author="Gemini Code Assist",
    author_email="example@example.com",
    description="A Python client for the Nexusbase Query Language (NBQL)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="<your-repo-url-here>", # Placeholder
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
	install_requires=[
		"crc32c>=2.2"
	],
    entry_points={
        'console_scripts': [
            'nbql-cli=cli:main',
        ],
    },
)
