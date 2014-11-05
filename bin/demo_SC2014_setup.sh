#!/bin/sh

# Setup the execution environment for the AIMES SC2014 demo.
#
# Run this script once before running the demo. The script should be run every
# time a new ssh connection is established with the VM from which the demo has
# to be run.
#
# Author: Matteo Turilli <matteo.turilli@rutgers.edu>
# copyright: Copyright 2014, RADICAL
# license: MIT

# Set up a clean Virtual Environment
deactivate
rm -rf ~/Virtualenvs/AIMES-DEMO-SC2014
mkdir -p ~/Virtualenvs/
virtualenv ~/Virtualenvs/AIMES-DEMO-SC2014
. ~/Virtualenvs/AIMES-DEMO-SC2014/bin/activate

# Install RADICAL software stack
pip install --upgrade -e git://github.com/radical-cybertools/radical.utils.git@devel#egg=radical.utils
pip install --upgrade -e git://github.com/radical-cybertools/saga-python.git@devel#egg=saga-python
pip install --upgrade -e git://github.com/radical-cybertools/radical.pilot.git@devel#egg=radical.pilot
pip install pandas
pip install Pyro4

# Install AIMES software stack
pip install --upgrade -e git://github.com/applicationskeleton/Skeleton.git@experimental#egg=Skeleton
pip install --upgrade -e git://bitbucket.org/shantenujha/aimes/modules/bundle@devel-bundles#egg=aimes.bundle
pip install --upgrade -e git://github.com/mturilli/aimes.emanager.git@devel#egg=aimes.emanager

# Set up Radical Pilot execution environment
export RADICAL_PILOT_DBURL='mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/radicalpilot'
export RADICAL_PILOT_BENCHMARK=
export SAGA_VERBOSE=debug            # To be unset for the demo
export RADICAL_PILOT_VERBOSE=debug   # To be unset for the demo

# Set up eManager execution environment
export BUNDLE_CONF='~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf'
export SKELETON_CONF='~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf'
export ORIGIN='54.196.51.239'

export XSEDE_PROJECT_ID_STAMPEDE=TG-MCB090174
export XSEDE_PROJECT_ID_TRESTLES=TG-MCB090174

