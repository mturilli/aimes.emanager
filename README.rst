========
EManager
========

EManager stays for Execution Manager and it is a module developed within the
AIMES project. The EManager takes a Skeleton as an input and executes its tasks by
means of the RADICAL pilot framework. Tasks are first described in terms of
Compute Units and then are scheduled on one or more pilots. The number of
pilots required to execute the given tasks is determined dynamically on the
base of the duration and number of cores required by each task. Currently,
WManager supports:

#. RADICAL pilot as PilotJob framework; and
#. AIMES skeletons to execute synthetic workloads;
#. AIMES bundles as information system about resource properties.

========
Usage
========

The AIMES execution manager requires either a Linux or OSX operating system.
Furthermore, the following applications are required in order to run the the
execution  manager:

* `git <http://git-scm.com/>`_.
* `virtualenv <https://pypi.python.org/pypi/virtualenv>`_.
* `pip <https://pypi.python.org/pypi/pip>`_.

**Dependences**

* radical.pilot: installed from pipi.
* radical.utils: installed by radical.pilot from pipi.
* saga-python: installed by radical.pilot from pipi.
* aimes.emanager: isntalled from github.
* aimes.skeleton: installed from github.
* aimes.bundle: installed from github.
* gnuplot >= 4.6

**Installation**

To run the execution manager, prepare the environment for the workload manager:

#. ``$ virtualenv ~/Virtualenvs/AIMES-DEMO-SC2014``
#. ``$ . ~/Virtualenvs/AIMES-DEMO-SC2014/bin/activate``

Install radical.pilot:

#. ``$ pip install radical.pilot``

The installation of radical.pilot will take care of installing also radical.utils and saga-python.

.. Install the required modules:

.. #. ``$ ...``

.. Install the execution manager:

.. #. ``$ ...``
.. #. ``$ pip install .``

.. To use a local instance of the MongDB server instead of the one provided and
.. maintained by the RADICAL group at Rutgers, download the MongoDB server from:

.. * `...`_.

.. Install the MongoDB server locally:

.. # E.g. OSX: `brew ...`_ installs MongoDB

.. Start the MongoDB server on a dedicated console:

.. #. ``$ ...``

.. ========
.. Example
.. ========

.. The execution manager is shipped with a pre-configured example of how to run a
.. bag of tasks on the XSEDE resources.

.. **Prerequisites**

.. * **A valid account on FutureGrid**. See
..   `FutureGrid - Getting Started <https://portal.futuregrid.org/manual/gettingstarted>`_
..   for further instructions.
.. * **A valid ssh public key uploaded to FutureGrid**. See `Upload a SSH Public Key
..   <http://manual.futuregrid.org/account.html#upload-a-ssh-public-key>`_ for
..   further instructions. Note that while the workload manager supports
..   password-protected ssh keys, the example included with the module assumes a
..   password-less key. Before running the example, please test your connectivity
..   to FutureGrid with the following command: ``ssh
..   <user_name>@sierra.futuregrid.org``. This command should provide you with a
..   shell on a FutureGrid resource without asking for a password.
.. * **The password for the Redis server**. Please contact the RADICAL group at
..   Rutgers for further information about how to access the Redis server or
..   install a local instance and modify accordingly the file ``pilot.input``
..   located in the ``modules/wmanager/examples`` directory of the AIMES git
..   repository. Usually, a password is not needed when connecting to a Redis
..   server installed locally.

.. **Running the example**

.. From a terminal, within the directory ``modules/wmanager`` of the AIMES
..    repository, execute the following commands:

.. #. ``cd examples``

.. Edit the file ``bundle.input`` adding your FutureGrid user name and the
..    path to your FutureGrid private ssh key. E.g.:

.. ``cluster_type=moab  hostname=alamo.futuregrid.org  username=mturilli  key_filename=/Users/matteo/.ssh/futuregrid_rsa``

.. Use the AIMES OWM client for a remote execution:

.. #. ``$ nestor.py skeleton BigJob pilot_remote.input -b bundle_aimes.input bag.input Shell -c ILikeBigJob_wITH-REdIS -u mturilli``

.. Where:

.. * ``nestor.py``: The name of the script that coordinates the components of the workload manager.
.. * ``skeleton``: The type of application used to describe the workload.
.. * ``BigJob``: The pilot framework used to execute the given workload.
.. * ``pilot.input``: The pilot configuration file.
.. * ``bundle.input``: The bundle configuration file.
.. * ``bag.input``: The skeleton configuration file.
.. * ``Shell``: The output mode of the skeleton module.
.. * ``-c <redis_password>``: The password to connect to the Redis server.
.. * ``-u <user_name_on_FutureGrid``: The user name registered with FutureGrid.

.. or locally:

.. #. ``$ nestor.py skeleton BigJob pilot_localhost.input bag.input Shell``

.. **Output**

.. nestor.py will create several files and directories within the directory
.. ``examples`` equal or similar to the following:

.. * ``Stage_1.sh``: A script created by the skeleton. It is the executable run by
..   each Task of the synthetic workload.

.. * ``Stage_1_prepare.sh``: A script created by the skeleton. It creates the input
..   and output directories for each stage of the skeleton and writes the input
..   files for all the tasks of the workload.

.. * ``Stage_1_input/``: It contains the input files for each task of the skeleton
..   workload.

.. * ``Stage_1_output/``: The directory where Stage_1.sh writes its output, one
..   file for each Task.

.. * ``bj-422bdf14-0d2f-11e3-84c0-00254bd30806/``: The working directory created by
..   the BigJob pilot system on the FutureGrid resource and copied back on the
..   local filesystem once the bag of task has been executed. This directory
..   contains one directory for each Compute Unit (CU) executed by the PilotJob
..   system. Currently, there is a 1 to 1 mapping between the Tasks of a Skeleton
..   and the CUs executed by the PilotJob system.


.. =============
.. Documentation
.. =============

.. ``nestor.py -h`` and ``nestor.py skeleton -h`` will offer some more detail. The
.. complete library documentation can be found in HTML at:

.. ``AIMES/modules/wmanager/docs/source/build/html/library/``

.. The HTML documentation can be browsed by cloning the AIMES git repository and
.. opening the file ``index.html`` in the directory
.. ``modules/wmanager/docs/source/_build/html`` of the AIMES git repository.


.. ========
.. Notes
.. ========

.. - The standard output of ``Stage_1.sh`` is written in the cu ``stderr`` file.
..   This does not indicate that the script runs with errors.
