"""Python setup.py for pyjuicefs package"""
import io
import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    """Read the contents of a text file safely.
    >>> read("pyjuicefs", "VERSION")
    '0.1.0'
    >>> read("README.md")
    ...
    """

    content = ""
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name="pyjuicefs",
    version=read("pyjuicefs", "VERSION"),
    description="Awesome pyjuicefs created by fzn0268",
    url="https://github.com/fzn0268/pyjuicefs/",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="fzn0268",
    packages=find_packages(exclude=["tests", ".github"]),
    python_requires=">= 3.8",
    install_requires=read_requirements("requirements.txt"),
    entry_points={
        'fsspec.specs': [
            'juicefs=pyjuicefs.JuiceFileSystem',
        ],
        "console_scripts": ["pyjuicefs = pyjuicefs.__main__:main"]
    },
    extras_require={"test": read_requirements("requirements-test.txt")},
)
