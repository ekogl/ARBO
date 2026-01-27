from setuptools import setup, find_packages

setup(
    name="arbo_lib",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "psycopg2-binary",
        "scikit-learn",
        "apache-airflow",
        "kubernetes"
    ]
)