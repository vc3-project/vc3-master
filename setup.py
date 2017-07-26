#!/usr/bin/env python
#
# Setup prog for vc3-master
#
#

# commenting, as it creates dependency on vc3 prefix:
release_version='0.0.1'

import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

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


def choose_data_file_locations():
    rpm_install = True

    if 'bdist_rpm' in sys.argv:
        rpm_install = True

    elif '--user' in sys.argv:
        rpm_install = False

    elif any( [ re.match('--home(=|\s)', arg) for arg in sys.argv] ):
        rpm_install = False

    if rpm_install:
        return rpm_data_files
    else:
    return home_data_files

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
    data_files=choose_data_file_locations()
)
