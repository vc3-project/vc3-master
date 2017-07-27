#!/bin/bash
if id vc3 > /dev/null 2>&1; then
           : # do nothing
else
    /usr/sbin/useradd --comment "VC3 service account" --shell /bin/bash vc3
fi
