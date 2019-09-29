import re
import setuptools

with open('prominence/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

setuptools.setup(
    name="prominence-hooks",
    version=version,
    author="Andrew Lahiff",
    author_email="andrew.lahiff@ukaea.uk",
    description="PROMINENCE HTCondor job router hooks",
    url="https://prominence-eosc.github.io/docs",
    platforms=["any"],
    install_requires=["requests", "cryptography", "M2Crypto"],
    package_dir={'': '.'},
    scripts=["bin/prominence"],
    packages=['prominence']
)
