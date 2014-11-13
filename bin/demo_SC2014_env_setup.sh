#!/bin/sh

# Setup the environment variables for the execution of the AIMES SC2014 demo.
#
# This script is sources by demo_SC2014.sh. As such, the user is not required
# to source this file before executing the demo.
#
# Author: Matteo Turilli, Andre Merzky
# copyright: Copyright 2014, RADICAL
# license: MIT

# Set up Radical Pilot execution environment
export RADICAL_PILOT_DBURL='mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/radicalpilot'
export RADICAL_PILOT_BENCHMARK=
export SAGA_VERBOSE=debug            # To be unset for the demo
export RADICAL_PILOT_VERBOSE=debug   # To be unset for the demo
#export EMANAGER_DEBUG

# Set up eManager execution environment
export BUNDLE_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
export SKELETON_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf
export ORIGIN='54.196.51.239'

export XSEDE_PROJECT_ID_STAMPEDE='TG-MCB090174'
export XSEDE_PROJECT_ID_TRESTLES='unc102'
export XSEDE_PROJECT_ID_GORDON='unc102'
export XSEDE_PROJECT_ID_BLACKLIGHT='unc102'
