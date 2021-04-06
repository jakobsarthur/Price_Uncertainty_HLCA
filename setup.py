from setuptools import setup
import os

packages = []
root_dir = os.path.dirname(__file__)
if root_dir:
    os.chdir(root_dir)


# TODO
# Change every instance of `Price_Uncertainty_HLCA` to the name of your library.
# Change `author`
# Change `author_email`
# Change `url`
# Change the PyPI classifiers: https://pypi.org/pypi?%3Aaction=list_classifiers
# If not using the standard license, please choose an identifier from https://spdx.org/licenses/

# Probably should be changed, __init__.py is no longer required for Python 3
for dirpath, dirnames, filenames in os.walk('Price_Uncertainty_HLCA'):
    # Ignore dirnames that start with '.'
    if '__init__.py' in filenames:
        pkg = dirpath.replace(os.path.sep, '.')
        if os.path.altsep:
            pkg = pkg.replace(os.path.altsep, '.')
        packages.append(pkg)


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths


setup(
    name='Price_Uncertainty_HLCA',
    version="0.1",
    packages=packages,
    author="Arthur Jakobs",
    author_email="arthur.jakobs@indecol.uni-freiburg.de",
    license="BSD-3-Clause",
    # Only if you have non-python data (CSV, etc.). Might need to change the directory name as well.
    # package_data={'your_name_here': package_files(os.path.join('Price_Uncertainty_HLCA', 'data'))},
    
    install_requires=[
        'appdirs',
    ],
    url="https://github.com/jakobsarthur/Price_Uncertainty_HLCA",
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    description='Arthur Jakobs',
    classifiers=[
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Operating System :: UNIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Mathematics',
    ],
)
