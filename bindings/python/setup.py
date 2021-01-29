from setuptools import find_packages, setup, Extension
from Cython.Build import cythonize

import os


LIBBSON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'pymongoarrow/libbson/lib')


EXT_MODULES = [
    Extension('pymongoarrow.bson_util',
              sources=['pymongoarrow/bson_util.pyx'],
              language='c++',
              include_dirs=['pymongoarrow/libbson/include'],
              libraries=['bson-1.0'],
              library_dirs=[LIBBSON_PATH],
              extra_link_args=['-Wl,-rpath,' + LIBBSON_PATH])
]

# ext_modules = cythonize("pymongoarrow/*.pyx")
# for module in ext_modules:
#     module.include_dirs.append('pymongoarrow/libbson/include')
#     module.libraries.append('bson-1.0')
#     module.library_dirs.append('pymongoarrow/libbson/lib')


# import ipdb; ipdb.set_trace()

setup(
    name='pymongoarrow',
    packages=['pymongoarrow'],
    # ext_modules=ext_modules,
    ext_modules=cythonize(EXT_MODULES),
    setup_requires=['cython >= 0.29'])
