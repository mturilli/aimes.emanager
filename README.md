# aimes.emanager

EManager stays for Execution Manager and it is a module developed within the
AIMES project. The EManager takes a workflow description as an input and executes its tasks by means of a pilot framework on a dynamically chosen set of resources. The number of pilots required to execute the given tasks is determined dynamically on the base of the duration and number of cores required by each task and on the information acquired about target resources.

Currently, EManager supports:

* [RADICAL-pilot](https://radical-cybertools.github.io/radical-pilot/index.html) as PilotJob framework;
* [AIMES skeletons](http://dx.doi.org/10.1109/eScience.2014.9) as a synthetic workflow descriptor; and
* [AIMES bundles](https://github.com/Francis-Liu/aimes.bundle) as information system about resource properties.

EManager is still under development. At the moment, a proof of concept is
available from this repository in the form of a demo script. The script has been
used at [Super Computing 2014](http://sc14.supercomputing.org/) to
illustrate the progress and current state of the art of the AIMES project.

## Usage

The demo script requires:

* A Linux operating system. Apple OS X should work too but it has not been tested.
* OS-level applications
* A selected number of python modules.
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
* [Pyro4](https://pythonhosted.org/Pyro4/): installed from pipi

### Allocations

* [stampede](https://portal.xsede.org/tacc-stampede)
* [trestles](http://www.sdsc.edu/us/resources/trestles/)
* [gordon](https://portal.xsede.org/sdsc-gordon)
* [blacklight](https://portal.xsede.org/psc-blacklight)
* [Hopper](https://www.nersc.gov/systems/hopper-cray-xe6/)

The demo can be run on a reduced set of resources but all the above resources are required to get a run consistent with the one demoed at SC2014.

## Installation

To run the demo script, prepare a dedicated python environment:

```
virtualenv ~/Virtualenvs/AIMES-DEMO-SC2014
. ~/Virtualenvs/AIMES-DEMO-SC2014/bin/activate
```

Install the required python modules:

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
multiple users. As such, the demo requires an extensive and fairly rigid configuration of its running environment.

### Supporting applications

The following programs need to be installed and made available within the OS:

* gnuplot >= 4.6
* mutt

Gnuplot is used to generate a diagrammatic representation of the demo run. This diagram is mailed alongside run statistics and logs to a configurable list of recipients via mutt. A mail agent/server is assumed to be configured and available on the host on which the demo is executed. Without one, mutt will not be able to send the e-mail.

### Configuration files

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
#     export AIMES_USER_KEY=<INSERT_PATH_TO_YOUR_SSH_PUBLIC_KEY>
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
* `<INSERT_PATH_TO_YOUR_SSH_PUBLIC_KEY>` with the path of the ssh public key that will be used to authenticate on _all_ the target resources. This parameter needs to be specified only when more than one private key is present in `~/.ssh`. Example of a valid parameter: `/home/test/.ssh/test_rsa.pub`.
* `<INSERT_YOUR_STAMPEDE_ALLOCATION>` with the allocation you want to use on stampede.
* `<INSERT_YOUR_TRESTLES_ALLOCATION>` with the allocation you want to use on trestles.
* `<INSERT_YOUR_GORDON_ALLOCATION>` with the allocation you want to use on gordon.
* `<INSERT_YOUR_BLACKLIGHT_ALLOCATION>` with the allocation you want to use on blacklight.
* `<INSERT_RECIPIENT_EMAIL_ADDRESS>` with one or more comma-delimited e-mail address(es) to which you want to send the report email once a demo run has terminated.

Edit the following file in your preferred editor:

```
~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf
```

and replace `<INSERT_STAMPEDE_USERNAME>`, `<INSERT_TRESTLES_USERNAME>`, `<INSERT_GORDON_USERNAME>`, `<INSERT_BLACKLIGHT_USERNAME>`, and `<INSERT_HOPPER_USERNAME>` with your username on the named resources.

If the run needs to be run on a reduced set of resources, all the unneeded resources should be commented out in this file.

### Execution environment

Create the directory from which to run the demo:

```
mkdir ~/AIMES_demo_SC2014
```

### Authentication

Bundles and radical.pilot require key-based ssh authentication and **do not handle** password requests for password-protected private keys. You have the choice to create a password-less private key or, more securely, use a ssh-agent to manage password requests for your keys. In order to run this demo you will need to setup key-based ssh authentication on: stampede, trestles, gordon, blacklight, and hopper.


## Initialization

The bundle module needs to be initialized before running the demo. Execute the following command:

```
aimes-bundle-manager -c ~/Virtualenvs/AIMES-DEMO-SC2014/etc/bundle_demo_SC2014.conf -m mongodb -u mongodb://54.221.194.147:24242/AIMES-bundle/ -v
```

and wait few minutes to allow for all the resource information to be loaded into the bundle database.

## Execution

Execute the AIMES SC2014 demo as follows:

```
cd ~/AIMES_demo_SC2014
demo_SC2014.sh
```

The script will output all the steps of the demo on the console and, once completed, will send an e-mail with the summary of the run and its diagrammatic representation to the e-mail address(es) indicated in the demo configuration file. Here an example of a diagram produced for a successful run of the demo:

![Diagrammatic representation of a demo run](https://raw.githubusercontent.com/mturilli/aimes.emanager/master/doc/54c64b2323769c240b19d396.png)

**Note that the pilot on blacklight is supposed to fail. This illustrates the fault tolerant properties of the scheduler used to late-bind the tasks of the given skeleton on a dynamic number of pilots.**

The following directories will be written into the demo directory:

* `run-21-<SID>`: directory containing all the files relative to the demo run, including diagrams, logs, and statistics. If the e-mail fails to be delivered, all the files are still available within this directory. Multiple runs create individual directories.
* `Stage_1_Input`: directory with the input files for the tasks of the first stage of the skeleton.
* `Stage_1_Output`: directory with the output files of the tasks of the first stage of the skeleton. These files are transferred from the remote resource back to the host from which the demo has been run.
* `Stage_2_Output`: directory with the output files of the tasks of the second stage of the skeleton. These files are transferred from the remote resource back to the host from which the demo has been run.

The skeleton executed by the demo is limited to 21 tasks due to the time constraints imposed by a live execution. The skeleton can be modified by editing the file:

```
~/Virtualenvs/AIMES-DEMO-SC2014/etc/skeleton_demo_SC2014.conf
```
The current code should support runs up but not above 4096 tasks per stage.

