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
export SAGA_VERBOSE=debug
export RADICAL_PILOT_VERBOSE=debug
export RADICAL_UTILS_VERBOSE=debug
export RADICAL_DEBUG_FILE=/tmp/aimes_demo_sc2014_debug.log
export RADICAL_PILOT_LOG_TARGETS=\>$RADICAL_DEBUG_FILE
export SAGA_LOG_TARGETS=\>\>$RADICAL_DEBUG_FILE
export RADICAL_UTILS_LOG_TARGETS=\>\>$RADICAL_DEBUG_FILE
#export EMANAGER_DEBUG

# Set up eManager execution environment
export DEMO_FOLDER=/home/mturilli/AIMES_demo_SC2014

export BUNDLE_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
export BUNDLE_DBURL='mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/bundle_v0_1/'
export SKELETON_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf
export ORIGIN='54.196.51.239'

export XSEDE_PROJECT_ID_STAMPEDE='TG-MCB090174'
export XSEDE_PROJECT_ID_TRESTLES='unc102'
export XSEDE_PROJECT_ID_GORDON='unc102'
export XSEDE_PROJECT_ID_BLACKLIGHT='unc102'

# Manage multiple identities when running the script
username=`id -un`
if test "$username" = "merzky"
then
    echo "hi Andre"
    export DEMO_FOLDER=/home/merzky/saga/aimes.emanager
    export AIMES_USER_ID=merzky
fi

