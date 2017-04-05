#!/bin/env python
#
#  Poll for resources in state 'new'
#  IF SSH or MFA:
#      -- Create SSH keypair
#      -- generate pairing code
#      -- Create resource tool command line.  
#      -- Store Resrouce in state 'ready-for-init'
#
#