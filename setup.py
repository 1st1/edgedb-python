#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import sys

if sys.version_info < (3, 5):
    raise RuntimeError('edgedb requires Python 3.5 or greater')

import os
import os.path
import platform

# We use vanilla build_ext, to avoid importing Cython via
# the setuptools version.
from distutils import extension as distutils_extension
from distutils.command import build_ext as distutils_build_ext

import setuptools


CYTHON_DEPENDENCY = 'Cython==0.29.0'

# Minimal dependencies required to test edgedb.
TEST_DEPENDENCIES = [
    'flake8~=3.5.0',
    'uvloop>=0.11.3;platform_system!="Windows"',
]

# Dependencies required to build documentation.
DOC_DEPENDENCIES = [
    'Sphinx~=1.7.3',
    'sphinxcontrib-asyncio~=0.2.0',
    'sphinx_rtd_theme~=0.2.4',
]

EXTRA_DEPENDENCIES = {
    'docs': DOC_DEPENDENCIES,
    'test': TEST_DEPENDENCIES,
    # Dependencies required to develop edgedb.
    'dev': [
        CYTHON_DEPENDENCY,
        'pytest>=3.6.0',
    ] + DOC_DEPENDENCIES + TEST_DEPENDENCIES
}


CFLAGS = ['-O2']
LDFLAGS = []

if platform.uname().system != 'Windows':
    CFLAGS.extend(['-fsigned-char', '-Wall', '-Wsign-compare', '-Wconversion'])


class build_ext(distutils_build_ext.build_ext):

    user_options = distutils_build_ext.build_ext.user_options + [
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
        ('cython-directives=', None,
            'Cython compiler directives'),
    ]

    def initialize_options(self):
        # initialize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        super(build_ext, self).initialize_options()

        if os.environ.get('EDGEDB_DEBUG'):
            self.cython_always = True
            self.cython_annotate = True
            self.cython_directives = "linetrace=True"
            self.define = 'PG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL'
            self.debug = True
        else:
            self.cython_always = False
            self.cython_annotate = None
            self.cython_directives = None

    def finalize_options(self):
        # finalize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        import pkg_resources

        # Double check Cython presence in case setup_requires
        # didn't go into effect (most likely because someone
        # imported Cython before setup_requires injected the
        # correct egg into sys.path.
        try:
            import Cython
        except ImportError:
            raise RuntimeError(
                'please install {} to compile edgedb from source'.format(
                    CYTHON_DEPENDENCY))

        cython_dep = pkg_resources.Requirement.parse(CYTHON_DEPENDENCY)
        if Cython.__version__ not in cython_dep:
            raise RuntimeError(
                'edgedb requires {}, got Cython=={}'.format(
                    CYTHON_DEPENDENCY, Cython.__version__
                ))

        from Cython.Build import cythonize

        directives = {
            'language_level': '3'
        }
        if self.cython_directives:
            for directive in self.cython_directives.split(','):
                k, _, v = directive.partition('=')
                if v.lower() == 'false':
                    v = False
                if v.lower() == 'true':
                    v = True

                directives[k] = v

        self.distribution.ext_modules[:] = cythonize(
            self.distribution.ext_modules,
            compiler_directives=directives,
            annotate=self.cython_annotate)

        super(build_ext, self).finalize_options()


setuptools.setup(
    name='edgedb',
    version='0.0.1',
    description='EdgeDB Python driver',
    platforms=['macOS', 'POSIX', 'Windows'],
    author='MagicStack Inc',
    author_email='hello@magic.io',
    url='https://github.com/edgedb/edgedb-python',
    license='Apache License, Version 2.0',
    packages=['edgedb'],
    provides=['edgedb'],
    zip_safe=False,
    include_package_data=True,
    ext_modules=[
        distutils_extension.Extension(
            "edgedb.pgproto.pgproto",
            ["edgedb/pgproto/pgproto.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS),

        distutils_extension.Extension(
            "edgedb.protocol.aprotocol",
            ["edgedb/protocol/datatypes/args.c",
             "edgedb/protocol/datatypes/record_desc.c",
             "edgedb/protocol/datatypes/tuple.c",
             "edgedb/protocol/datatypes/namedtuple.c",
             "edgedb/protocol/datatypes/object.c",
             "edgedb/protocol/datatypes/set.c",
             "edgedb/protocol/datatypes/hash.c",
             "edgedb/protocol/datatypes/array.c",
             "edgedb/protocol/datatypes/repr.c",
             "edgedb/protocol/aprotocol.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS),
    ],
    cmdclass={'build_ext': build_ext},
    test_suite='tests.suite',
    setup_requires=EXTRA_DEPENDENCIES['dev'],
)
