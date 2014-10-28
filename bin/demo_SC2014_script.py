#!/usr/bin/env python

# pylint: disable-msg=C0103

"""Implements an Execution Manager for the AIMES demo.
"""

__author__ = "Matteo Turilli, Andre Merzky"
__copyright__ = "Copyright 2014, RADICAL"
__license__ = "MIT"

import os
import sys
import math
import operator

import radical.utils as ru
import radical.pilot
import emanager
import emanager.interface

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# TODO: use radical.utils configuration module instead of environmental
# variables.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
else:
    print "Session database: %s" % DBURL


# Ref document "AIMES Demo SC2014", Execution Manager: #1
# ----------------------------------------------------------------------
# The bundle configuration file.
BUNDLE_CONF = os.getenv("BUNDLE_CONF")
if BUNDLE_CONF is None:
    print "ERROR: BUNDLE_CONF (i.e. bundle configuration file) not defined."
    sys.exit(1)
else:
    print "Bundle configuration file: %s" % BUNDLE_CONF

# The IP from which the submission originates. From example, the IP of the
# virtual machine from which an AIMES demo is executed.
ORIGIN = os.getenv("ORIGIN")
if ORIGIN is None:
    print "ERROR: ORIGIN (i.e. your current IP address) not defined."
    sys.exit(1)
else:
    print "IP address: %s" % BUNDLE_CONF


# Ref document "AIMES Demo SC2014", Execution Manager: #2
# ----------------------------------------------------------------------
SKELETON_CONF = os.getenv("SKELETON_CONF")
if SKELETON_CONF is None:
    print "ERROR: SKELETON_CONF (i.e. skeleton description file) not defined."
    sys.exit(1)
else:
    print "Skeleton description file: %s" % SKELETON_CONF


# -----------------------------------------------------------------------------
# bundle
# -----------------------------------------------------------------------------
bundle = emanager.interface.Bundle(BUNDLE_CONF, ORIGIN)

# Collect information about the resources to plan the execution strategy.
bandwidth_in = dict()
bandwidth_out = dict()

# Get network bandwidth for each resource.
for resource in bundle.resources:
    bandwidth_in[resource.ID] = resource.get_bandwidth(resource.ID, ORIGIN,
                                                       'in')
    bandwidth_out[resource.ID] = resource.get_bandwidth(resource.ID, ORIGIN,
                                                        'out')

# Set allocation for each given resource
XSEDE_PROJECT_ID_STAMPEDE = os.getenv("XSEDE_PROJECT_ID_STAMPEDE")
if 'stampede.tacc.utexas.edu' in bundle.resources and \
        XSEDE_PROJECT_ID_STAMPEDE is None:
    print "ERROR: XSEDE_PROJECT_ID_STAMPEDE undefined for any stampede."
    sys.exit(1)
else:
    print "XSEDE Stampede project ID: %s" % XSEDE_PROJECT_ID_STAMPEDE

XSEDE_PROJECT_ID_TRESTLES = os.getenv("XSEDE_PROJECT_ID_TRESTLES")
if 'trestles.sdsc.xsede.org' in bundle.resources and \
        XSEDE_PROJECT_ID_TRESTLES is None:
    print "ERROR: XSEDE_PROJECT_ID_TRESTLES undefined for any trestles."
    sys.exit(1)
else:
    print "XSEDE project ID: %s" % XSEDE_PROJECT_ID_TRESTLES

XSEDE_PROJECT_ID_GORDON = os.getenv("XSEDE_PROJECT_ID_GORDON")
if 'gordon.sdsc.xsede.org' in bundle.resources and \
        XSEDE_PROJECT_ID_GORDON is None:
    print "ERROR: XSEDE_PROJECT_ID_GORDON undefined for any gordon."
    sys.exit(1)
else:
    print "XSEDE project ID: %s" % XSEDE_PROJECT_ID_GORDON

XSEDE_PROJECT_ID_BLACKLIGHT = os.getenv("XSEDE_PROJECT_ID_BLACKLIGHT")
if 'blacklight.psc.xsede.org' in bundle.resources and \
        XSEDE_PROJECT_ID_BLACKLIGHT is None:
    print "ERROR: XSEDE_PROJECT_ID_BLACKLIGHT undefined for any blacklight."
    sys.exit(1)
else:
    print "XSEDE project ID: %s" % XSEDE_PROJECT_ID_BLACKLIGHT


# -----------------------------------------------------------------------------
# skeleton
# -----------------------------------------------------------------------------
skeleton = emanager.interface.Skeleton(SKELETON_CONF)

# DEFINE EXECUTION BOUNDARIES
#
# Calculate the min/max time taken by each stage to execute and the
# mix/max amount of cores needed. Factor data transfer time into min/max
# time. Note: Max(compute) <=> Min(time) & Min(compute) <=> Max(time)

stages_compute_limits = dict()
stages_time_limits = dict()

stages_compute_limits['min'] = 0
stages_compute_limits['max'] = 0
stages_time_limits['min'] = 0
stages_time_limits['max'] = 0

for stage in skeleton.stages:
    max_task_duration = 0
    total_tasks_duration = 0

    stages_compute_limits['min'] += 1

    for task in skeleton.tasks:
        stages_compute_limits['max'] += task.cores
        total_tasks_duration += task.duration
        if task.duration > max_task_duration:
            max_task_duration = task.duration

    stages_time_limits['min'] += max_task_duration
    stages_time_limits['max'] += total_tasks_duration

#------------------------------------------------------------------------------
# Execution Strategy
# -----------------------------------------------------------------------------

# DEFINE EURISTICS
#
# Degree of concurrency. Question: what amount of concurrent execution
# minimizes TTC?
eur_concurrency = 100

# Number of resources. Question: what is the number of resources that when used
# to execute the tasks of the workload minimize the TTC?
eur_resources = 100

# Cutoff data transfer. Question: what metric should be used to evaluate the
# impact of time spent transferring data on the TTC?
eur_data_cutoff = 100

# CHOOSE NUMBER OF PILOTS
#
# Adopt an heuristics that tells us how many concurrent resources we
# should choose given the execution time boundaries. We assume that task
# concurrency should always be maximized we may decide that we want to
# start with #pilots = #resources to which we have access.

if eur_resources == 100:
    max_number_pilots = len(bundle.resources)

# Account for the time taken by the data staging and drop all the resources
# that are above the data cutoff.

# Sort the resources by bandwidth
sorted_bandwidth_in = sorted(bandwidth_in.items(),
                             key=operator.itemgetter(1))
sorted_bandwidth_out = sorted(bandwidth_out.items(),
                              key=operator.itemgetter(1))

sorted_bandwidth_in.reverse()
sorted_bandwidth_out.reverse()

if eur_data_cutoff == 100:
    for x, y in ru.misc.window(sorted_bandwidth_in):
        if x[1] - y[1] >= y[1]:
            pass


# CHOOSE THE TYPE OF CONTAINER
#
# If no container is specified go with the container of the chosen resources.

# SORT RESOURCES
#
# Generate a resource matrix with all the properties that are relevant
# to choose resources.
# Resource ID | load | queue_length | bandwidth in | bandwidth out

# CHOOSE RESOURCES
#
# Get the first n resources from the sorted list that, in case, use the
# required container.

# CHOOSE THE SCHEDULER FOR THE CUs
#
# Depending on whether we have multiple pilot and on what metric needs
# to bo min/maximized. In this demo we minimize TTC so we choose backfilling.

# WE CANNOT CHOOSE ABOUT:
#
# - Early/late binding. We use late by default.
# - Data staging strategies. We do not have this kind of capabilities in RP.

#------------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
def pilot_state_cb(p, state):
    """Called very time a ComputePilot changes its state.
    """

    if state == radical.pilot.states.FAILED:
        print "Compute Pilot '%s' failed, exiting..." % p.uid
        sys.exit(1)

    elif state == radical.pilot.states.CANCELED:
        print "Compute Pilot '%s' canceled..." % p.uid

    elif state == radical.pilot.states.ACTIVE:
        print "Compute Pilot '%s' became active..." % p.uid

    elif state == radical.pilot.states.DONE:
        print "Compute Pilot '%s' has done!" % p.uid


def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called
    very time a ComputeUnit changes its state.
    """
    if state == radical.pilot.states.FAILED:
        print "Compute Unit '%s' failed..." % unit.uid

    elif state == radical.pilot.states.CANCELED:
        print "Compute Unit '%s' canceled..." % unit.uid

    elif state == radical.pilot.states.EXECUTING:
        print "Compute Unit '%s' executing..." % unit.uid

    elif state == radical.pilot.states.DONE:
        print "Compute Unit '%s' finished with output:" % unit.uid


def wait_queue_size_cb(umgr, wait_queue_size):
    """Called when the size of the unit managers wait_queue changes.
    """
    print "[Callback]: UnitManager '%s' wait_queue_size changed to %s." \
        % (umgr.uid, wait_queue_size)

    """Called when the size of the unit managers wait_queue changes.
    """
    print "[Callback]: UnitManager '%s' wait_queue_size changed to %s." \
        % (umgr.uid, wait_queue_size)


# -----------------------------------------------------------------------------
# Experiment
# -----------------------------------------------------------------------------
if __name__ == "__main__":

    # SESSION ------------------------------------------------------
    print "Creating session..."
    session = radical.pilot.Session(database_url=DBURL)
    context = radical.pilot.Context('ssh')
    context.user_id = 'mturilli'
    session.add_context(context)

    print "\tSession UID: {0}".format(session.uid)

    try:
        # PILOT MANAGER ------------------------------------------------
        # One pilot manager is used for all pilots.
        print "Initializing Pilot Manager..."
        pmgr = radical.pilot.PilotManager(session=session)

        print "\tPilot Manager UID: {0}".format(pmgr.uid)

        # Register the pilot callback with the pilot manager. Called
        # every time any of the pilots managed by the pilot manager
        # changes its state.
        pmgr.register_callback(pilot_state_cb)


        # PILOTS -------------------------------------------------------
        # Number of cored for each pilot. NOTE: here there is space for
        # optimizations/decisions. Do we assign n cores to each pilot
        # with n = BAG_SIZE so that each single pilot can execute the
        # whole bag with maximal concurrency? Do we maximize the number
        # of resource asked esking BAG_SIZE/len(RESOURCES) cores for
        # each pilot? We might have to collect data about each option.
        print "Creating %i pilot descriptions..." % len(RESOURCES)

        cores = math.ceil(BAG_SIZE/len(RESOURCES))

        # List of all the pilot descriptions
        pdescs = []

        # Create a pilot description for each resource
        for resource in RESOURCES:
            pdesc = radical.pilot.ComputePilotDescription()

            if 'stampede' in resource:
                pdesc.project  = XSEDE_PROJECT_ID_STAMPEDE
            elif 'trestles' in resource:
                pdesc.project  = XSEDE_PROJECT_ID_TRESTLES
            elif 'gordon' in resource:
                pdesc.project  = XSEDE_PROJECT_ID_GORDON
            elif 'blacklight' in resource:
                pdesc.project  = XSEDE_PROJECT_ID_BLACKLIGHT
            else:
                print "ERROR: No XSEDE_PROJECT_ID given for resource %s." % resource
                sys.exit(1)

            pdesc.resource = resource  # label
            pdesc.runtime  = RUNTIME   # minutes
            pdesc.cores    = cores
            pdesc.cleanup  = True
            pdescs.append(pdesc)
            print "\tPilot description for %s created with %i cores." % (resource, cores)

        # Submit the pilots just described.
        print "Submit pilots..."
        pilots = pmgr.submit_pilots(pdescs)


        # DATA STAGING -------------------------------------------------
        # Input file shared by all the CUs. Where is the file? Define
        # the url of the local file in the local directory.
        print "Staging data..."
        shared_input_file_url = saga.Url('file://%s' % (SHARED_INPUT_FILE))

        # For each pilot described: define and open a staging directory
        # on the remote machine and then copy the input file into it.
        # All this is likely to be hidded soon from the user within the
        # scheduler (?).
        if hasattr(pilots, '__iter__'):
            for pilot in pilots:
                remote_dir_url = saga.Url(os.path.join(pilot.sandbox,
                    'staging_area'))
                remote_dir = saga.filesystem.Directory(remote_dir_url,
                    flags=saga.filesystem.CREATE_PARENTS)
                print "\tCreating staging area on %s" % remote_dir_url
                print "\tCopying %s to %s" % (shared_input_file_url, remote_dir_url)
                remote_dir.copy(shared_input_file_url, '.')
        else:
            remote_dir_url = saga.Url(os.path.join(pilots.sandbox,
                'staging_area'))
            remote_dir = saga.filesystem.Directory(remote_dir_url,
                flags=saga.filesystem.CREATE_PARENTS)
            print "\tCreating staging area on %s" % remote_dir_url
            print "\tCopying %s to %s" % (shared_input_file_url, remote_dir_url)
            remote_dir.copy(shared_input_file_url, '.')

        # Now that we have a staging area for each pilot, we specify an
        # action to link the input file that we have transferred into
        # the staging areas within the pilot sandbox. Basically, we need
        # to make the same file available to every task we will execute
        # in a pilot. Instead of copying the file to each task location,
        # we link it (ln -s) within the sandbox they share. This action
        # is not performed immediately, here it is just specified so to
        # be used later when specifying the tasks. Note the triple
        # slash, needed by saga.python.
        sd_shared = {'source': 'staging:///%s' % os.path.basename(SHARED_INPUT_FILE),
                     'action':  radical.pilot.LINK}


        # WORKLOAD -----------------------------------------------------
        # BAG_SIZE number of tasks to be executed on len(RESOURCES)
        print ("Creating %i Compute Units: " % BAG_SIZE),
        cuds = []

        for unit_count in range(0, BAG_SIZE):
            # OUPUT - Configure the staging action for output files.
            sd_outputs = {'source': ['state.cpt'   ,
                                     'confout.gro' ,
                                     'ener.edr'    ,
                                     'traj.trr'    ,
                                     'md.log'      ],
                          'target': ['outputs/state-%d.cpt'   % unit_count,
                                     'outputs/confout-%d.gro' % unit_count,
                                     'outputs/ener-%d.edr'    % unit_count,
                                     'outputs/traj-%d.trr'    % unit_count,
                                     'outputs/md-%d.log'      % unit_count],
                          'action':  radical.pilot.TRANSFER,
                          'flags' :  radical.pilot.CREATE_PARENTS}

            cud = radical.pilot.ComputeUnitDescription()
            cud.kernel         = 'GROMACS'
            cud.arguments      = ['-s', os.path.basename(SHARED_INPUT_FILE)]
            cud.cores          = 1
            cud.input_staging  = sd_shared
            cud.output_staging = sd_outputs

            cuds.append(cud)
            print ('%i,' % unit_count),

        # UNIT MANAGERS ------------------------------------------------
        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object. One unit manager will be used for all
        # the pilots. 'Late binding' scheduler is used to backfill (a
        # type of load balance) conpute units to pilots when they become
        # available.
        print "\nInitializing Unit Manager..."
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_BACKFILLING)

        print "\tUnit Manager UIDs: {0}".format(session.list_unit_managers())

        # Add pilots to the unit manager.
        print "Adding pilots to Unit Manager..."
        umgr.add_pilots(pilots)

        # Register the compute unit callback with the UnitManager.
        # Called every time any of the unit managed by the
        # UnittManager changes its state.
        umgr.register_callback(unit_state_change_cb)


        # EXECUTION ----------------------------------------------------
        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to
        # start assigning ComputeUnits to the ComputePilots.
        print "Submit units to the Unit Manager..."
        umgr.submit_units(cuds)

        # Wait for all compute units to finish.
        print "Wating for the execution of the CUs..."
        umgr.wait_units()
        print "CU execution done."


        # CLEAN UP AND SHUT DOWN ---------------------------------------
        # Close the session so to shutdown all the pilots cleanly
        session.close(cleanup=False, terminate=True)
        sys.exit(0)

    except Exception as e:
        # this catches all RP and system exceptions
        print "Caught exception: %s" % e

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), we catch the corresponding
        # KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit which gets raised if the main threads exits for
        # some other reason.
        print "Caught exception, exit now: %s" % e

    finally:
        # always clean up the session, no matter whether we caught an exception
        print 'session cleanup on exit'
        session.close(cleanup=False, terminate=True)
