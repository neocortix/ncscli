import os
import setuptools
import shutil

__version__ = '0.12.10'

with open("README.md", "r") as fh:
    long_description = fh.read()

cacheDirPath = 'ncscli/__pycache__'
if os.path.isdir( cacheDirPath ):
    shutil.rmtree( cacheDirPath )

#print( '>>found packages', setuptools.find_packages( exclude=["examples"] ) )
setuptools.setup(
    name="ncscli", # hopefully doen't need username-suffix
    version=__version__,
    python_requires='>=3.6',
    install_requires=['requests>=2.12.4', 'asyncssh>=1.16.1'],
    scripts=['ncscli/ncs.py', 'ncscli/jsonToInv.py', 'ncscli/purgeKnownHosts.py', 'ncscli/tellInstances.py'],
    packages=["ncscli", 'ncsexamples'],
    package_dir = { 'ncscli': 'ncscli', 'ncsexamples': 'examples' },
    package_data = { '': ['*', '*/*', '*/*/*'] },
    exclude_package_data = { '': ['*_pycache_*', '*.pyc', '*/*.pyc'] },

    description="ncscli is the command-line and python interface for Neocortix Cloud Services.",
    author="Neocortix, Inc.",
    author_email="info@neocortix.com",
    url="https://github.com/neocortix/ncscli",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
