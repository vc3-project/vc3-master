#!/usr/bin/env python
#
# Setup prog for vc3-master
#
#

import vc3.master
release_version=vc3.master.__version__

import commands
import os
import re
import sys

from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

systemd_files = [ 'etc/vc3-service-info.service' ]

etc_files = ['etc/vc3-master.conf',
             'etc/vc3-master.service',
            ]

sysconfig_files = [
             'etc/sysconfig/vc3-master',
             ]

logrotate_files = [
             'etc/logrotate/vc3-master',
                  ]

initd_files = ['etc/vc3-master.init', ]

rpm_data_files=[
                ('/etc/vc3', etc_files),
                ('/etc/sysconfig', sysconfig_files),
                ('/etc/logrotate.d', logrotate_files),                                        
                ('/etc/init.d', initd_files),
                ('/usr/lib/systemd/system', systemd_files),                                     
               ]

home_data_files=[#('etc', libexec_files),
                 ('etc', etc_files),
                 ('etc', initd_files),
                 ('etc', sysconfig_files),
                ]

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
    packages=['vc3',
              'vc3.plugins',
             ],
    scripts=['scripts/vc3-master'],
    
    data_files = choose_data_files()
)
