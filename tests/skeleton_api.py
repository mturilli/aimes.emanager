#!/usr/bin/env python

# pylint: disable-msg=C0103

"""Test the Skeleton API.
"""

__author__ = "Matteo Turilli, Andre Merzky"
__copyright__ = "Copyright 2014, RADICAL"
__license__ = "MIT"

import os
import radical.utils as ru
import aimes.skeleton

# Set environment variables.
CONF = os.getenv("SKELETON_CONF")
MODE = os.getenv("SKELETON_MODE")

# Create a reporter for the test. Takes care of colors and font attributes.
report = ru.Reporter(title='Skeleton API test')

skeleton = aimes.skeleton.Skeleton(CONF)
skeleton.generate(mode=MODE)

commands = skeleton.setup()

for cmd in commands:
    print cmd

for stage in skeleton.stages:
    report.info("stage.name       : %s" % stage.name)
    print "stage.tasks      : %s" % stage.tasks
    print "stage.inputdir[0]: %s" % stage.inputdir[0]

    # Derive stage size
    print "len(stage.tasks) : %s" % len(stage.tasks)

for task in skeleton.tasks:
    report.info("task.name        : %s" % task.name)
  # print "  task.description : %s" % task.description
    print "  task.stage       : %s" % task.stage
    print "  task.length      : %s" % task.length
    print "  task.cores       : %s" % task.cores
    print "  task.task_type   : %s" % task.task_type
    print "  task.command     : %s" % task.command
  # print "  task.executable  : %s" % task.executable
  # print "  task.arguments   : %s" % task.arguments

    print "  task.inputs"
    for i in task.inputs:
        print "    i['name']      : %s" % i['name']
        print "    i['size']      : %s" % i['size']

    print "  task.outputs"
    for o in task.outputs:
        print "    o['name']      : %s" % o['name']
        print "    o['size']      : %s" % o['size']
