import re
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="PROMINENCE",
    version='1.0.0',
    author="Andrew Lahiff",
    author_email="andrew.lahiff@ukaea.uk",
    description="PROMINENCE",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://prominence-eosc.github.io/docs",
    platforms=["any"],
    install_requires=["uwsgi", "flask", "requests", "boto3", "PyJWT", "elasticsearch", "elasticsearch-dsl", "etcd3", "influxdb-client"],
    package_dir={'': '.'},
    scripts=["prominence-restapi.py"],
    packages=["prominence", "prominence.backend"],
    package_data={"": ["README.md"]},
)
