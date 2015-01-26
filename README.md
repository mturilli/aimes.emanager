# aimes.emanager

EManager stays for Execution Manager and it is a module developed within the
AIMES project. The EManager takes a Skeleton as an input and executes its tasks by means of the RADICAL pilot framework. Tasks are first described in terms of Compute Units and then are scheduled on one or more pilots. The number of pilots required to execute the given tasks is determined dynamically on the base of the duration and number of cores required by each task. Currently, EManager supports:

* RADICAL pilot as PilotJob framework; and
* AIMES skeletons to execute synthetic workloads;
* AIMES bundles as information system about resource properties.

EManager is still under development. At the moment, a proof of concept is
available from this repository in the form of a demo script. The script has
used at [Super Computing 2014](http://sc14.supercomputing.org/) to
illustrate the progress and current state of the art of the AIMES project.

## Usage

The demo script requires:

* either a Linux or OSX operating system. 
* applications and python modules.
* Accounts and allocations on [XSEDE](https://www.xsede.org/) and [NeRSC](https://www.nersc.gov/).

### Applications

* [git](http://git-scm.com/)
* [virtualenv](https://pypi.python.org/pypi/virtualenv)
* [pip](https://pypi.python.org/pypi/pip)
* [gnuplot >= 4.6](http://www.gnuplot.info/)
* [mutt](http://www.mutt.org/)

### Python modules

* [radical.pilot](https://github.com/radical-cybertools/radical.pilot): installed from pipi
* [radical.utils](https://github.com/radical-cybertools/radical.utils): installed by radical.pilot from pipi
* [saga-python](https://github.com/radical-cybertools/saga-python): installed by radical.pilot from pipi
* [aimes.skeleton](https://github.com/applicationskeleton/Skeleton): installed from github
* [aimes.bundle](https://github.com/Francis-Liu/aimes.bundle): installed from github
* [pandas](http://pandas.pydata.org/): installed from pipi

### Allocations

* [stampede](https://portal.xsede.org/tacc-stampede)
* [trestles](http://www.sdsc.edu/us/resources/trestles/)
* [gordon](https://portal.xsede.org/sdsc-gordon)
* [blacklight](https://portal.xsede.org/psc-blacklight)
* [Hopper](https://www.nersc.gov/systems/hopper-cray-xe6/)


## Installation

To run the demo script, prepare a dedicated python environment:

```
virtualenv ~/Virtualenvs/AIMES-DEMO-SC2014
. ~/Virtualenvs/AIMES-DEMO-SC2014/bin/activate
```

Install the required modules:

```
pip install radical.pilot
pip install --upgrade git+https://github.com/applicationskeleton/Skeleton.git@$master#egg=Skeleton
pip install --upgrade git+https://github.com/Francis-Liu/aimes.bundle.git@$master#egg=aimes.bundle
pip install pandas
pip install Pyro4
```

Install the execution manager:

```
pip install --upgrade git+https://github.com/mturilli/aimes.emanager.git@master#egg=aimes.emanager
```

## Configuration

This demo has **not** been designed to be portable or to be shared among
multiple users. As such, the demo requires an extensive and rigid configuration of the running environment. The following programs need to be installed and made available within the OS:

* gnuplot >= 4.6
* mutt

Gnuplot is used to generate a diagrammatic representation of the demo run. This diagram is mailed alongside run statistics and logs to a configurable list of recipients via mutt.

Edit the following file in your preferred editor:

```
~/Virtualenvs/AIMES-DEMO-SC2014/bin/demo_SC2014_env_setup.sh
```

uncomment the following block of text:

```
# if test "$username" = "<INSERT_YOUR_USERNAME>"
# then
#     #export EMANAGER_DEBUG
#     export AIMES_USER_ID=<INSERT_YOUR_USERNAME>
#     export DEMO_FOLDER=/home/<INSERT_YOUR_USERNAME>/AIMES_demo_SC2014
#     export BUNDLE_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
#     export SKELETON_CONF=~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf
#     export XSEDE_PROJECT_ID_STAMPEDE=<INSERT_YOUR_STAMPEDE_ALLOCATION>
#     export XSEDE_PROJECT_ID_TRESTLES=<INSERT_YOUR_TRESTLES_ALLOCATION>
#     export XSEDE_PROJECT_ID_GORDON=<INSERT_YOUR_GORDON_ALLOCATION>
#     export XSEDE_PROJECT_ID_BLACKLIGHT=<INSERT_YOUR_BLACKLIGHT_ALLOCATION>
#     export RECIPIENTS=<INSERT_RECIPIENT_EMAIL_ADDRESS>,<INSERT_RECIPIENT_EMAIL_ADDRESS>
# fi
```

and replace:

* `<INSERT_YOUR_USERNAME>` with the name of the account from which you will run the demo. The command `id -un` can be used to find out the name of the account to be used.
* `<INSERT_YOUR_STAMPEDE_ALLOCATION>` with the allocation you want to use on stampede.
* `<INSERT_YOUR_TRESTLES_ALLOCATION>` with the allocation you want to use on trestles.
* `<INSERT_YOUR_GORDON_ALLOCATION>` with the allocation you want to use on gordon.
* `<INSERT_YOUR_BLACKLIGHT_ALLOCATION>` with the allocation you want to use on blacklight.
* `<INSERT_RECIPIENT_EMAIL_ADDRESS>` with one or more comma-delimited e-mail address to which you want to send the report email once the demo as finished to run.

Edit the following file in your preferred editor:

```
~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
```

and replace `<INSERT_STAMPEDE_USERNAME>`, `<INSERT_TRESTLES_USERNAME>`, `<INSERT_GORDON_USERNAME>`, `<INSERT_BLACKLIGHT_USERNAME>`, and `<INSERT_HOPPER_USERNAME>` with your username on the named resources.

## Initialization

The bundle module needs to be initialized before running the demo. Execute the following command:

```
aimes-bundle-manager -c ~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf -m mongodb -u mongodb://54.221.194.147:24242/AIMES-bundle/ -v
```

and wait 5 minutes to allow for all the resource information to be loaded into the bundle database.

## Execution

