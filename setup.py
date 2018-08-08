import setuptools

setuptools.setup(
    name="source_postgres",
    version="0.0.1",
    packages=setuptools.find_packages(exclude=["tests"]),
    install_requires=[
        "psycopg2",
    ],
)
