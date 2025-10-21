#!/usr/bin/env python
"""Setup script for SignLedger - Immutable Audit Logging Library."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Core dependencies
core_deps = [
    "pydantic>=2.0.0",
    "cryptography>=41.0.0",
    "typing-extensions>=4.0.0",
    "python-dateutil>=2.8.0",
]

# Optional dependencies for different features
extras_require = {
    # Database backends
    "postgresql": ["psycopg2-binary>=2.9.0", "sqlalchemy>=2.0.0"],
    "mongodb": ["pymongo>=4.0.0"],
    "sqlite": ["sqlalchemy>=2.0.0"],

    # Compression algorithms
    "compression": ["zstandard>=0.21.0", "lz4>=4.3.0"],

    # Framework integrations
    "django": ["django>=3.2"],
    "flask": ["flask>=2.0.0"],
    "fastapi": ["fastapi>=0.100.0", "uvicorn>=0.23.0"],

    # Async support
    "async": ["aiofiles>=23.0.0"],

    # Development dependencies
    "dev": [
        "pytest>=7.4.0",
        "pytest-cov>=4.1.0",
        "pytest-asyncio>=0.21.0",
        "pytest-mock>=3.11.0",
        "black>=23.7.0",
        "isort>=5.12.0",
        "flake8>=6.1.0",
        "mypy>=1.5.0",
        "bandit>=1.7.5",
    ],

    # Testing dependencies
    "test": [
        "pytest>=7.4.0",
        "pytest-cov>=4.1.0",
        "pytest-asyncio>=0.21.0",
        "pytest-mock>=3.11.0",
        "faker>=19.3.0",
    ],
}

# All features
extras_require["all"] = list(set(
    extras_require["postgresql"] +
    extras_require["mongodb"] +
    extras_require["compression"] +
    extras_require["django"] +
    extras_require["flask"] +
    extras_require["fastapi"] +
    extras_require["async"]
))

setup(
    name="signledger",
    version="1.0.0",
    author="Vipin Kumar",
    author_email="vipin08@example.com",
    description="Immutable audit logging with cryptographic verification for Python applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vipin08/signledger",
    project_urls={
        "Bug Reports": "https://github.com/vipin08/signledger/issues",
        "Source": "https://github.com/vipin08/signledger",
    },
    packages=find_packages(exclude=["tests", "tests.*", "examples", "docs"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Logging",
        "Topic :: Security :: Cryptography",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Typing :: Typed",
    ],
    keywords="audit logging immutable ledger blockchain cryptography compliance security",
    python_requires=">=3.8",
    install_requires=core_deps,
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "signledger=signledger.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "signledger": ["py.typed"],
    },
    zip_safe=False,
)
