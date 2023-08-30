from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

# get version from __version__ variable in insights_additions/__init__.py
from insights_additions import __version__ as version

setup(
    name="insights_additions",
    version=version,
    description="Insights Additions",
    author="Venco Ltd",
    author_email="dev@venco.co",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
)
