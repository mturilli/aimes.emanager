#!/bin/sh

# Setup the environment variables for the execution of the AIMES SC2014 demo.
#
# This script is sources by demo_SC2014.sh. As such, the user is not required
# to source this file before executing the demo.
#
# Author: Matteo Turilli, Andre Merzky
# copyright: Copyright 2014, RADICAL
# license: MIT

# Manage multiple identities when running the script
username=`id -un`

if test "$username" = "merzky"
then
    echo "Hi Andre"
    export EMANAGER_DEBUG=TRUE
    export AIMES_USER_ID=merzky
    export DEMO_FOLDER=/home/merzky/saga/aimes.emanager
    export BUNDLE_CONF=/home/merzky/saga/aimes.emanager/etc/bundle_demo_SC2014.conf
    export SKELETON_CONF=/home/merzky/saga/aimes.emanager/etc/skeleton_demo_SC2014.conf
    export XSEDE_PROJECT_ID_STAMPEDE='TG-MCB090174'
    export XSEDE_PROJECT_ID_TRESTLES='unc102'
    export XSEDE_PROJECT_ID_GORDON='unc102'
    export XSEDE_PROJECT_ID_BLACKLIGHT='unc102'
    export RECIPIENTS=matteo.turilli@gmail.com,andre@merzky.net,shantenu.jha@rutgers.edu
fi

if test "$username" = "mturilli"
then
    #export EMANAGER_DEBUG
    export AIMES_USER_ID=mturilli
    export DEMO_FOLDER=/home/mturilli/AIMES_demo_SC2014
    export BUNDLE_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
    export SKELETON_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf
    export XSEDE_PROJECT_ID_STAMPEDE='TG-MCB090174'
    export XSEDE_PROJECT_ID_TRESTLES='unc100'
    export XSEDE_PROJECT_ID_GORDON='unc101'
    export XSEDE_PROJECT_ID_BLACKLIGHT='unc102'
    export RECIPIENTS=matteo.turilli@gmail.com,andre@merzky.net,shantenu.jha@rutgers.edu
fi

# if test "$username" = "<INSERT_YOUR_USERNAME>"
# then
#     #export EMANAGER_DEBUG
#     export AIMES_USER_ID=<INSERT_YOUR_USERNAME>
#     export AIMES_USER_KEY=<INSERT_PATH_TO_YOUR_SSH_PRIVATE_KEY>
#     export DEMO_FOLDER=/home/<INSERT_YOUR_USERNAME>/AIMES_demo_SC2014
#     export BUNDLE_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
#     export SKELETON_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf
#     export XSEDE_PROJECT_ID_STAMPEDE=<INSERT_YOUR_STAMPEDE_ALLOCATION>
#     export XSEDE_PROJECT_ID_TRESTLES=<INSERT_YOUR_TRESTLES_ALLOCATION>
#     export XSEDE_PROJECT_ID_GORDON=<INSERT_YOUR_GORDON_ALLOCATION>
#     export XSEDE_PROJECT_ID_BLACKLIGHT=<INSERT_YOUR_BLACKLIGHT_ALLOCATION>
#     export RECIPIENTS=<INSERT_RECIPIENT_EMAIL_ADDRESS>,<INSERT_RECIPIENT_EMAIL_ADDRESS>
# fi

# Set up Radical Pilot execution environment
export RADICAL_PILOT_DBURL='mongodb://54.221.194.147:24242/radicalpilot'
export RADICAL_PILOT_BENCHMARK=
export SAGA_VERBOSE=debug
export RADICAL_PILOT_VERBOSE=debug
export RADICAL_UTILS_VERBOSE=debug
export RADICAL_DEBUG_FILE=/tmp/aimes_demo_sc2014_debug.log
export RADICAL_PILOT_LOG_TARGETS=$RADICAL_DEBUG_FILE
export SAGA_LOG_TARGETS=$RADICAL_DEBUG_FILE
export RADICAL_UTILS_LOG_TARGETS=$RADICAL_DEBUG_FILE

# Set up eManager execution environment
export ORIGIN='54.196.51.239'
export BUNDLE_DBURL='mongodb://54.221.194.147:24242/AIMES_bundle/'

# Setup report
export RUN_TAG="AIMES demo SC2014"
