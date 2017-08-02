#!/usr/bin/env python
#
# Setup prog for vc3-master
#
#
import sys
import re
from setuptools import setup
import time


def choose_data_file_locations():
    local_install = False

    if '--user' in sys.argv:
        local_install = True
    elif any([re.match('--home(=|\s)', arg) for arg in sys.argv]):
        local_install = True
    elif any([re.match('--prefix(=|\s)', arg) for arg in sys.argv]):
        local_install = True

    if local_install:
        return home_data_files
    else:
        return rpm_data_files

# commenting, as it creates dependency on vc3 prefix:
current_time = time.gmtime()
release_version = "{0}.{1:0>2}.{2:0>2}".format(current_time.tm_year, current_time.tm_mon, current_time.tm_day)


systemd_files = ['etc/vc3-master.service']

etc_files = ['etc/vc3-master.conf',
             'etc/vc3-master.conf.sample',
             'etc/tasks.conf']

sysconfig_files = ['etc/sysconfig/vc3-master',]

logrotate_files = ['etc/logrotate/vc3-master',]

initd_files = []

rpm_data_files=[
                ('/etc/vc3', etc_files),
                ('/etc/sysconfig', sysconfig_files),
                ('/etc/logrotate.d', logrotate_files),                                        
                ('/usr/lib/systemd/system', systemd_files),
               ]

home_data_files=[('etc', etc_files),
                 ('etc', initd_files),
                 ('etc', sysconfig_files)]

data_files = choose_data_file_locations()

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
              'vc3master.plugins',
              'vc3master.plugins.task'
             ],
    scripts=['scripts/vc3-master'],
    data_files=data_files
)
