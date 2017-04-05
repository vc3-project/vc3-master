#!/bin/env python
# 
#  This Task plugin searches for new user vcluster Requests
#
#  - poll for requests in state 'new'
#  --  determine need for core
#  if CORE:
#     -- generate factory config  
#     -- launch core instance
#     -- confirm core launched 
#     -- store request in state 'initialized'
#
#