from setuptools import setup
from setuptools_rust import RustExtension


setup(
    name="json-eater",
    version="0.0.2",
    author="Kartik Thakore, Jeffrey Kim",
    author_email="kartik.thakore@sharecare.com, jeffrey.kim@sharecare.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Rust",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
    ],
    url='https://github.com/doc-ai/json-eater.git',
    packages=["json_eater"],
    rust_extensions=[RustExtension("json_eater.json_eater", "Cargo.toml", debug=False)],
    include_package_data=True,
    zip_safe=False,
)