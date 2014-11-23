#!/usr/bin/env python

__author__    = "Andre Merzky, Matteo Turilli"
__copyright__ = "Copyright 2014, RADICAL"
__email__     = "matteo.turilli@rutgers.edu"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess as sp

from setuptools import setup, Command

name     = 'aimes.emanager'
mod_root = ''

#-----------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - version:          1.2.3            - is used for installation
#   - version_detail:  v1.2.3-9-g0684b06 - is used for debugging
#   - version is read from VERSION file in src root, which is on installation
#     copied into the module dir.
#   - version_detail is derived from the git tag, and only available when
#     installed from git -- this is stored in VERSION.git, in the same
#     locations, on install.
#   - both files, VERSION and VERSION.git are used to provide the runtime
#     version information.
#
def get_version (mod_root):
    """
    mod_root
        a VERSION and VERSION.git file containing the version strings is
        created in mod_root, during installation.  Those files are used
        at runtime to get the version information.

    """

    try:

        version        = None
        version_detail = None

        # get version from './VERSION'
        src_root = os.path.dirname (__file__)
        if  not src_root :
            src_root = '.'

        with open (src_root + "/VERSION", "r") as f :
            version = f.readline ().strip()


        # attempt to get version detail information from git
        p   = sp.Popen ('cd %s ; '\
                        'tag=`git describe --tags --always` 2>/dev/null ; '\
                        'branch=`git branch | grep -e "^*" | cut -f 2 -d " "` 2>/dev/null ; '\
                        'echo $tag@$branch'  % src_root,
                        stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
        version_detail = p.communicate()[0].strip()

        if  p.returncode   !=  0  or \
            version_detail == '@' or \
            'fatal'        in version_detail :
            version_detail =  "v%s" % version

        print 'version: %s (%s)'  % (version, version_detail)


        # make sure the version files exist for the runtime version inspection
        path = "%s/%s" % (src_root, mod_root)
        print 'creating %s/VERSION' % path

        with open (path + "/VERSION",     "w") as f : f.write (version        + "\n")
        with open (path + "/VERSION.git", "w") as f : f.write (version_detail + "\n")

        return version, version_detail

    except Exception as e :
        raise RuntimeError ("Could not extract/set version: %s" % e)


#------------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_detail = get_version (mod_root)


#------------------------------------------------------------------------------
# check python version. we need > 2.6, <3.x
if  sys.hexversion < 0x02060000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("%s requires Python 2.x (2.6 or higher)" % name)


#------------------------------------------------------------------------------
class our_test(Command):
    user_options = []
    def initialize_options (self) : pass
    def finalize_options   (self) : pass
    def run (self) :
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        retval  = sp.call([sys.executable,
                          '%s/run_tests.py'               % testdir,
                          '%s/configs/default.cfg'        % testdir])
        raise SystemExit(retval)


#------------------------------------------------------------------------------
#
def read(*rnames):
    try :
        return open(os.path.join(os.path.dirname(__file__), *rnames)).read()
    except Exception :
        return ""


#------------------------------------------------------------------------------
setup_args = {
    'name'               : 'emanager',
    'version'            : version,
    'description'        : 'AIMES execution manager module',
    'long_description'   : (read('README.rst') + '\n\n' + read('CHANGES.rst')),
    'author'             : 'Matteo Turilli, Andre Merzky',
    'author_email'       : 'matteo.turilli@rutgers.edu',
    'maintainer'         : 'Matteo Turilli, Andre Merzky',
    'maintainer_email'   : 'matteo.turilli@rutgers.edu',
    'url'                : 'https://bitbucket.org/shantenujha/aimes',
    'license'            : 'MIT',
    'keywords'           : 'AIMES execution_manager',
    'classifiers'        : [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'packages'           : [
        'aimes.emanager'
    ],
    'scripts'            : [
        'bin/demo_SC2014.sh',
        'bin/demo_SC2014_ve_setup.sh',
        'bin/demo_SC2014_env_setup.sh',
        'bin/demo_SC2014_script.py'
    ],
    'package_data'       : {'' : ['etc/*.conf', 'VERSION', 'VERSION.git']},
    'data_files'         : [
        ("%s/etc/"% sys.prefix, ['etc/bundle_demo_SC2014.conf']),
        ("%s/etc/"% sys.prefix, ['etc/emanager.conf']),
        ("%s/etc/"% sys.prefix, ['etc/skeleton_demo_SC2014.conf'])
    ],
    'cmdclass'           : {
        'test'           : our_test,
    },
    'install_requires'   : [
        'radical.pilot'
    ],
    'extras_require'     : {
        'bundles'        : ['aimes.bundle'],
        'skeleton'       : ['aimes.skeleton']
    },
    'tests_require'      : [],
    'zip_safe'           : False
}

#------------------------------------------------------------------------------

setup (**setup_args)

#------------------------------------------------------------------------------

