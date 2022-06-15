"""Packaging and distribution configuration."""
import os

import setuptools

VERSION_FILE = "./VERSION"


def get_readme():
    with open("README.md", "r") as fh:
        return fh.read()


def get_version():
    """Reads the current version from two potential sources.

    It does so in this order:
    - environment variable `BUILD_VERSION`
    - ./VERSION file

    If neither of them is set, a ValueError is raised.

    Returns:
        str

    Raises:
        ValueError: if the version cannot be determined
    """
    version = os.getenv("BUILD_VERSION")

    if version is not None:
        return version

    if os.path.exists(VERSION_FILE):
        with open(VERSION_FILE, "r") as f:
            return f.read().strip()

    raise ValueError("Could not determine build version")


def read_requirements(file_path):
    """Reads a requirements file and returns a list of strings, each being a requirements.

    Args:
        file_path (str):

    Returns:
        List[str]
    """
    with open(file_path, "r") as f:
        return f.readlines()


setuptools.setup(
    name="dynamicio",
    version=get_version(),
    author="Christos Hadjinikolis, Radu Ghitescu",
    author_email="christos.hadjinikolis@gmail.com, radu.ghitescu@gmail.com",
    description="Panda's wrapper for IO operations",
    long_description=get_readme(),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    license="Apache License 2.0",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements("requirements.txt"),
    package_data={"dynamicio": ["py.typed"]},
    entry_points="""
        [console_scripts]
        dynamicio=dynamicio.cli:run
        """,
)
