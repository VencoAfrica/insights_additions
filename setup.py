from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in insights_changes/__init__.py
from insights_changes import __version__ as version

setup(
	name="insights_changes",
	version=version,
	description="Insights Changes",
	author="Venco Ltd",
	author_email="dev@venco.co",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
