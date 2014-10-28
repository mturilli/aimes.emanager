#!/usr/bin/env python

import sys
import emanager.interface as emis

s = emis.Skeleton (sys.argv[1])

print s

cuds = emis.task_to_cud (s.tasks)

for cud in cuds :
    print cud

