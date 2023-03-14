from setuptools import find_packages, setup
import versioneer

setup(
    name="intake_dbdb",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Dougie Squire",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "cftime",
        "dask",
        "ecgtools",
        "fsspec",
        "intake-esm",
        "jsonschema",
        "xarray",
    ],
)