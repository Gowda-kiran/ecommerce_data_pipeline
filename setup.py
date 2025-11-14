from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="ecommerce-data-pipeline",
    version="1.0.0",
    author="Data Engineering Team",
    description="E-commerce Data Pipeline with PySpark, Airflow, and Data Quality Checks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Gowda-kiran/ecommerce_data_pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "black>=23.12.1",
            "flake8>=7.0.0",
        ],
    },
)
