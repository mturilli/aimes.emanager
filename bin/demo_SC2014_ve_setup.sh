#!/bin/sh

# Setup the execution environment for the AIMES SC2014 demo.
#
# Run this script once before running the demo. The script should be run every
# time a new ssh connection is established with the VM from which the demo has
# to be run.
#
# NOTE: this script assumes to be run within the root directory of the
# aimes.emanager repository.
#
# Author: Matteo Turilli, Andre Merzky
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

# Install AIMES software stack
pip install --upgrade -e git://github.com/applicationskeleton/Skeleton.git@experimental#egg=Skeleton
git clone git@bitbucket.org:shantenujha/aimes aimes.bundle
cd aimes.bundle
git checkout devel-bundles
git pull
cd modules/bundle
pip install --upgrade .
cd ../../../
# pip install --upgrade -e git://bitbucket.org/shantenujha/aimes/modules/emanager.git@devel#egg=emanager
pip install --upgrade .
pip install pandas
pip install Pyro4
