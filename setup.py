"""Python package building configuration."""

from glob import glob
from os.path import basename, splitext

from setuptools import find_packages, setup


with open("README.md", "r") as readme_file:
    readme = readme_file.read()

setup(
    name="mecoda-minka",
    version="1.0.1",
    description="Library to download information collected in Minka API.",
    author="Ana Alvarez",
    author_email="anomalia@disroot.org",
    license="GNU General Public License v3",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/pynomaly/mecoda-minka",
    packages=find_packages("src"),
    package_dir={"": "src"},
    package_data={"mecoda_minka": ["py.typed", "data/*.csv"]},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
        "Natural Language :: English",
    ],
    install_requires=["pydantic", "requests", "pandas", "folium"],
)
