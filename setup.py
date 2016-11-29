#!/usr/bin/env python
#
# Setup prog for vc3-master
#
#

release_version='0.9.1'

import commands
import os
import re
import sys

from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org


# ===========================================================
#                D A T A     F I L E S 
# ===========================================================


# etc files are handled by setup.cfg
etc_files = ['etc/applications.conf',
             'etc/clusters.conf',
             'etc/sites.conf',
            ]


sbin_scripts = ['sbin/vc3-master',
               ]

# -----------------------------------------------------------

rpm_data_files=[('/usr/sbin', sbin_scripts),
                ('/etc/vc3master', etc_files),
               ]



# -----------------------------------------------------------

def choose_data_files():
    rpminstall = True
    userinstall = False
     
    if 'bdist_rpm' in sys.argv:
        rpminstall = True

    elif 'install' in sys.argv:
        for a in sys.argv:
            if a.lower().startswith('--home'):
                rpminstall = False
                userinstall = True
                
    if rpminstall:
        return rpm_data_files
    elif userinstall:
        return home_data_files
    else:
        # Something probably went wrong, so punt
        return rpm_data_files
       
# ===========================================================

# setup for distutils
setup(
    name="vc3-master",
    version=release_version,
    description='vc3-master package',
    long_description='''This package contains vc3 master''',
    license='GPL',
    author='VC3 Team',
    author_email='vc3-project@googlegroups.com',
    maintainer='VC3 team',
    maintainer_email='vc3-project@googlegroups.com',
    url='http://virtualclusters.org/',
    packages=['vc3master',
              'vc3master.bin',
              'vc3master.lib',
              'vc3master.plugins',
             ],

    
    data_files = choose_data_files()
)
