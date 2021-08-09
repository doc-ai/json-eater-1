from setuptools import setup
from setuptools_rust import RustExtension

import os

from json_eater import __version__

# Allow passing a custom version when building a pre-release package
custom_version = os.environ.get('VERSION', None)

if custom_version is not None:
  __version__ = custom_version

setup(
    name="json-eater",
    version=__version__,
    author="Kartik Thakore, Jeffrey Kim",
    author_email="kartik.thakore@sharecare.com, jeffrey.kim@sharecare.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Rust",
        "Operating System :: OS Independent"
    ],
    url='https://github.com/doc-ai/json-eater.git',
    packages=["json_eater"],
    rust_extensions=[RustExtension("json_eater.json_eater", "Cargo.toml", debug=False)],
    include_package_data=True,
    zip_safe=False,
)