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
import pandas as pd

import radical.utils as ru

import radical.pilot
import aimes.bundle
import aimes.emanager
import aimes.emanager.interface

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# TODO: use radical.utils configuration module instead of environmental
# variables.
EMANAGER_DEBUG = os.getenv("EMANAGER_DEBUG")

DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Session database: %s" % DBURL

# The Skeleton configuration file.
SKELETON_CONF = os.getenv("SKELETON_CONF")
if SKELETON_CONF is None:
    print "ERROR: SKELETON_CONF (i.e. skeleton description file) not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Skeleton description file: %s" % SKELETON_CONF

# The bundle configuration file.
BUNDLE_CONF = os.getenv("BUNDLE_CONF")
if BUNDLE_CONF is None:
    print "ERROR: BUNDLE_CONF (i.e. bundle configuration file) not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Bundle configuration file: %s" % BUNDLE_CONF

# The IP from which the submission originates. From example, the IP of the
# virtual machine from which an AIMES demo is executed.
ORIGIN = os.getenv("ORIGIN")
if ORIGIN is None:
    print "ERROR: ORIGIN (i.e. your current IP address) not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - IP address: %s" % BUNDLE_CONF

# -----------------------------------------------------------------------------
# Reporter
# -----------------------------------------------------------------------------
# Create a reporter for the demo. Takes care of colors and font attributes.
report = ru.Reporter(title='AIMES Demo SC2014')

pd.set_option('display.width', 1000)

# -----------------------------------------------------------------------------
# skeleton
# -----------------------------------------------------------------------------
skeleton = aimes.emanager.interface.Skeleton(SKELETON_CONF)
skeleton.setup ()

# Test skeleton API
report.header("Skeleton Workflow S01")

report.info("Stages")
print "Type of workflow       : pipeline"
print "Total number of stages : %d" % len(skeleton.stages)
print "Total number of tasks  : %d" % len(skeleton.tasks)
print "Total input data       : 20.38MB"
print "Total output data      : 1.38MB"

report.info("Stage 1")

for stage in skeleton.stages:
    print "Number of tasks : %d" % len(skeleton.stages[0].tasks)

# I could derive this but no point doing it for the demo
print "Type of tasks   : homogeneous"
print "Input files     : 1 1MB input file for each task"
print "Output files    : 1 20KB output file for each task"

report.info("Stage 2")

for stage in skeleton.stages:
    print "Number of tasks : %d" % len(skeleton.stages[1].tasks)

# I could derive this but no point doing it for the demo
print "Type of tasks   : homogeneous"
print "Input files     : 20 1MB input files for a single task"
print "Output files    : 1 1MB output file for a single task"


if EMANAGER_DEBUG:
    for stage in skeleton.stages:
        report.info("stage.name       : %s" % stage.name)
        print "stage.tasks      : %s" % stage.tasks
        print "stage.inputdir[0]: %s" % stage.inputdir[0]

        # Derive stage size
        print "len(stage.tasks) : %s" % len(stage.tasks)

        # Derive stage duration
        print "sum(task.length for task in stage.tasks): %s" % \
            sum(task.length for task in stage.tasks)

        # Derive stage staged-in data
        print "sum(task.cores for task in stage.tasks) : %s" % \
            sum(task.cores for task in stage.tasks)


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


# -----------------------------------------------------------------------------
# bundle
# -----------------------------------------------------------------------------
report.header("Resource Bundle B01")

bundle = aimes.emanager.interface.Bundle(BUNDLE_CONF, ORIGIN)

# Set allocation for each given resource
XSEDE_PROJECT_ID_STAMPEDE = os.getenv("XSEDE_PROJECT_ID_STAMPEDE")
if 'stampede.tacc.utexas.edu' in bundle.resources and \
        XSEDE_PROJECT_ID_STAMPEDE is None:
    print "ERROR: XSEDE_PROJECT_ID_STAMPEDE undefined for any stampede."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "XSEDE Stampede project ID: %s" % XSEDE_PROJECT_ID_STAMPEDE

XSEDE_PROJECT_ID_TRESTLES = os.getenv("XSEDE_PROJECT_ID_TRESTLES")
if 'trestles.sdsc.xsede.org' in bundle.resources and \
        XSEDE_PROJECT_ID_TRESTLES is None:
    print "ERROR: XSEDE_PROJECT_ID_TRESTLES undefined for any trestles."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "XSEDE project ID: %s" % XSEDE_PROJECT_ID_TRESTLES

XSEDE_PROJECT_ID_GORDON = os.getenv("XSEDE_PROJECT_ID_GORDON")
if 'gordon.sdsc.xsede.org' in bundle.resources and \
        XSEDE_PROJECT_ID_GORDON is None:
    print "ERROR: XSEDE_PROJECT_ID_GORDON undefined for any gordon."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "XSEDE project ID: %s" % XSEDE_PROJECT_ID_GORDON

XSEDE_PROJECT_ID_BLACKLIGHT = os.getenv("XSEDE_PROJECT_ID_BLACKLIGHT")
if 'blacklight.psc.xsede.org' in bundle.resources and \
        XSEDE_PROJECT_ID_BLACKLIGHT is None:
    print "ERROR: XSEDE_PROJECT_ID_BLACKLIGHT undefined for any blacklight."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "XSEDE project ID: %s" % XSEDE_PROJECT_ID_BLACKLIGHT


# Collect information about the resources to plan the execution strategy.
bandwidth_in = dict()
bandwidth_out = dict()

# Get network bandwidth for each resource.
for resource_name in bundle.resources:
    resource = bundle.resources[resource_name]
    bandwidth_in[resource.name] = resource.get_bandwidth(ORIGIN, 'in')
    bandwidth_out[resource.name] = resource.get_bandwidth(ORIGIN, 'out')

# Report back to the demo about the available resource bundle.
report.info("Target Resources")
print "IDs: %s" % \
    [bundle.resources[resource].name for resource in bundle.resources ]

# I could derive this but no point doing it for the demo.
print "Total core capacity: 7168"
print
print "Acquiring real time-resource information:"
print "  Number of nodes ............................. OK"
print "  Type of container ........................... OK"
print "  Bandwidth from origin to resource  .......... OK"
print "  Bandwidth from resource to origin  .......... OK"
print "  Queue names ................................. OK"
print "  Queue max walltime .......................... OK"
print "  Queue max number of jobs .................... OK"
print "  Queue core capacity ......................... OK"
print "  Queue available capacity .................... OK"
print "  Queue length ................................ OK"

if EMANAGER_DEBUG:
    # Test bundle API
    for resource_name in bundle.resources:
        resource = bundle.resources[resource_name]

        report.info("resource.name     : %s" % resource.name)
        print "resource.num_nodes: %s" % resource.num_nodes
        print "resource.container: %s" % resource.container
        print "resource.get_bandwidth(IP, 'in') : %s" % \
            resource.get_bandwidth(ORIGIN, 'in')
        print "resource.get_bandwidth(IP, 'out'): %s" % \
            resource.get_bandwidth(ORIGIN, 'out')
        print "resource.queues   : %s" % resource.queues.keys()

        for queue_name in resource.queues:
            queue = resource.queues[queue_name]
            print
            print "  queue.name             : %s" % queue.name
            print "  queue.resource_name    : %s" % queue.resource_name
            print "  queue.max_walltime     : %s" % queue.max_walltime
            print "  queue.num_procs_limit  : %s" % queue.num_procs_limit
            print "  queue.alive_nodes      : %s" % queue.alive_nodes
            print "  queue.alive_procs      : %s" % queue.alive_procs
            print "  queue.busy_nodes       : %s" % queue.busy_nodes
            print "  queue.busy_procs       : %s" % queue.busy_procs
            print "  queue.free_nodes       : %s" % queue.free_nodes
            print "  queue.free_procs       : %s" % queue.free_procs
            print "  queue.num_queueing_jobs: %s" % queue.num_queueing_jobs
            print "  queue.num_running_jobs : %s" % queue.num_running_jobs

# sys.exit()

#------------------------------------------------------------------------------
# Execution Boundaries
# -----------------------------------------------------------------------------
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
        total_tasks_duration += task.length

        # We assume tasks with heterogeneous runtime.
        if task.length > max_task_duration:
            max_task_duration = task.length

    stages_time_limits['min'] += max_task_duration
    stages_time_limits['max'] += total_tasks_duration

report.header("Execution Strategy")

report.info("Goal for the execution of S01")
print "G01 - Minimize time to completion (MinTTC)"

report.info("Derive execution boundaries for S01")
print "B01 - lowest number of cores  : %s" % stages_compute_limits['min']
print "B02 - highest number of cores : %s" % stages_compute_limits['max']
print "B03 - shortest execution time : %s minutes" % stages_time_limits['min']
print "B04 - longest execution time  : %s minutes" % stages_time_limits['max']


#------------------------------------------------------------------------------
# Execution Strategy
# -----------------------------------------------------------------------------

# DEFINE EURISTICS
report.info("Derive heuristics to satisfy G01 for the workflow S01")

# Degree of concurrency. Question: what amount of concurrent execution
# minimizes TTC?
eur_concurrency = 100
print "E01 - Degree of concurrency for task execution : %s%%" % eur_concurrency

# Number of resources. Question: what is the number of resources that when used
# to execute the tasks of the workload minimize the TTC?
eur_resources_number = 100
print "E02 - Percentage of bundle resources targeted  : %s%%" % \
    eur_resources_number

# Cutoff data transfer. Question: what metric should be used to evaluate the
# impact of time spent transferring data on the TTC?
eur_data_cutoff = 100

# Relevance of available information about resources. We have a set of types of
# information available for each resource. We need a criterium to define the
# importance/priority of each type of information on the other. Question: how
# do we quantify the impact that each dimension has of TTC?
eur_resources_information_order = ["Queue num_cores",
                                   "Queue length",
                                   "Load",
                                   "Bandwidth in",
                                   "Bandwidth out"]
print "E03 - Prioritize resource information          : %s" % \
    eur_resources_information_order

# CHOOSE NUMBER OF PILOTS
report.info("Derive decision points for the execution of S01")

# Adopt an heuristics that tells us how many concurrent resources we
# should choose given the execution time boundaries. We assume that task
# concurrency should always be maximized we may decide that we want to
# start with #pilots = #resources to which we have access.
if eur_resources_number == 100:
    number_pilots = len(bundle.resources)
    print "Decision D01 based on E02 - Home many pilots? %d" % number_pilots

# Account for the time taken by the data staging and drop all the resources
# that are above the data cutoff. NOTE: irrelevant with the workload we use
# for the demo.

# SORT RESOURCES
#
# Generate a resource matrix with all the properties that are relevant to
# choose resources.
#
# Resource ID | Queue num_cores | Queue length | Load | band* in | band* out
data = dict()
colums_labels = ["Name"]+eur_resources_information_order

for label in colums_labels:
    data[label] = list()

    for resource_name in bundle.resources:
        resource = bundle.resources[resource_name]

        if label == 'Name':
            data[label].append(resource.name)

        elif label == 'Queue num_cores':
            for queue in resource.queues:
                if queue == 'normal' or queue == 'batch' or queue == 'default':
                    data[label].append(resource.queues[queue].num_procs_limit)
                    break

        elif label == 'Queue length':
            for queue in resource.queues:
                if queue == 'normal' or queue == 'batch' or queue == 'default':
                    data[label].append(resource.queues[queue].num_queueing_jobs)
                    break

        elif label == 'Load':
            for queue in resource.queues:
                if queue == 'normal' or queue == 'batch' or queue == 'default':
                    total = resource.queues[queue].alive_nodes
                    busy = float(resource.queues[queue].busy_nodes)
                    data[label].append((busy*100)/total)
                    break

        elif label == 'Bandwidth in':
            data[label].append(bandwidth_in[resource.name])

        elif label == 'Bandwidth out':
            data[label].append(bandwidth_out[resource.name])

resource_matrix = pd.DataFrame(data, columns=colums_labels)

# Sort the resource matrix so to define the priority among target resources.
resource_priority = resource_matrix.sort(['Queue num_cores',
                                          'Queue length',
                                          'Load',
                                          'Bandwidth in',
                                          'Bandwidth out'],
                                         ascending=[False,
                                                    True,
                                                    True,
                                                    False,
                                                    False])

print "Decision D02 based on E03 - What is the resource priority?\n"
print "%s \n" % resource_priority



# CHOOSE RESOURCES
#
# Get the first n resources from the sorted list that, in case, use the
# required container.
def uri_to_tag(resource):

    tag = ''

    if resource == 'blacklight.psc.xsede.org':
        tag = 'xsede.blacklight'

    elif resource == 'gordon.sdsc.xsede.org':
        tag = 'xsede.gordon'

    elif resource == 'stampede.tacc.utexas.edu':
        tag = 'xsede.stampede'

    elif resource == 'stampede.tacc.xsede.org':
        tag = 'xsede.stampede'

    elif resource == 'stampede.xsede.org':
        tag = 'xsede.stampede'

    elif resource == 'trestles.sdsc.xsede.org':
        tag = 'xsede.trestles'

    else:
        sys.exit("Unknown resource specified in bundle: %s" % resource)

    return tag

resource_avail = resource_priority['Name'].tolist()
resources = list()

while len(resources) < eur_resources_number and \
    len(resources) < len(resource_avail):
    resources.append(uri_to_tag(resource_avail[len(resources)]))

print "Decision D03 based on D02, E02 - How many resource should be used?"

for resource in resources:
    print "  %s Selected" % resource.ljust(20, '.')
print

# CHOOSE THE TYPE OF CONTAINER
#
# If no container is specified go with the container of the chosen resources.
# NOTE: irrelevant for this demo, we use only 'job' containers.
print "Decision D04 based on B01 - What resource container should be used?"
for resource in resources:
    print "  %s: Pilot" % resource.ljust(17)
print

# CHOOSE THE SCHEDULER FOR THE CUs
#
# Depending on whether we have multiple pilot and on what metric needs to bo
# min/maximized. In this demo we minimize TTC so we choose backfilling. Do we
# have a default scheduler? If so, an else is superfluous.
if len(resources) > 1:
    rp_scheduler = 'SCHED_BACKFILLING'

print "Decision D05 based on D03, D04 - What pilot scheduler should be used? %s" % \
    rp_scheduler

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
# RUN DEMO
# -----------------------------------------------------------------------------
if __name__ == "__main__":

    report.header("Enacting Execution Strategy")

    # SESSION
    print "Creating session..."
    session = radical.pilot.Session(database_url=DBURL)
    context = radical.pilot.Context('ssh')
    context.user_id = 'mturilli'
    session.add_context(context)

    print "Session UID: {0}".format(session.uid)

    try:
        # PILOT MANAGER
        # One pilot manager is used for all pilots.
        print "Initializing Pilot Manager..."
        pmgr = radical.pilot.PilotManager(session=session)

        print "Pilot Manager UID: {0}".format(pmgr.uid)

        # Register the pilot callback with the pilot manager. Called
        # every time any of the pilots managed by the pilot manager
        # changes its state.
        pmgr.register_callback(pilot_state_cb)


        # PILOTS
        # Number of cored for each pilot. NOTE: here there is space for
        # optimizations/decisions. Do we assign n cores to each pilot with n =
        # BAG_SIZE so that each single pilot can execute the whole bag with
        # maximal concurrency? Do we maximize the number of resource asked
        # BAG_SIZE/len(resources) cores for each pilot? We might have to
        # collect data about each option.
        print "Creating %i pilot descriptions..." % len(resources)

        cores = math.ceil((stages_compute_limits['max']*eur_concurrency)/100.0)
        print "DEBUG: number of cores to execute all the skeleton: %d" % cores

        # TODO: derive overhead dynamically from stage_in time + agent
        # bootstrap + agent task queue management overheads
        rp_overhead = 10

        # List of all the pilot descriptions
        pdescs = []

        # Create a pilot description for each resource
        for resource in resources:
            pdesc = radical.pilot.ComputePilotDescription()

            if 'stampede' in resource:
                pdesc.project = XSEDE_PROJECT_ID_STAMPEDE
            elif 'trestles' in resource:
                pdesc.project = XSEDE_PROJECT_ID_TRESTLES
            elif 'gordon' in resource:
                pdesc.project = XSEDE_PROJECT_ID_GORDON
            elif 'blacklight' in resource:
                pdesc.project = XSEDE_PROJECT_ID_BLACKLIGHT
            else:
                print "ERROR: No XSEDE_PROJECT_ID given for resource %s." % \
                    resource
                sys.exit(1)

            pdesc.resource = resource  # label

            # We assume a uniform distribution of the total amount of cores
            # across all the available pilots. Future optimizations may take
            # into consideration properties of the resources that would justify
            # a bias distribution of the cores.
            pdesc.cores = math.ceil(float(cores/len(resources)))

            # We assume enough runtime for each pilot to run all the tasks of
            # the skeleton. This covers the case in which one pilot comes
            # online while all the others are still queued and the time delta
            # between the first and the second pilot coming online is greater
            # that the time that takes to run all the tasks on a single pilot
            # with 'number of tasks in the skeleton'/'number of pilots' cores.
            # NOTE: runtime expressed in minutes.
            pdesc.runtime = (math.ceil(
                             (math.ceil(
                              float(stages_time_limits['max'] /
                                    pdesc.cores))) /
                             60.0) +
                             rp_overhead)

            pdesc.cleanup = True
            pdescs.append(pdesc)
            print "Pilot description for %s created with %i cores." % \
                (resource, cores)

        for pdesc in pdescs:
            print pdesc

        sys.exit()

        # Submit the pilots just described.
        print "Submit pilots..."
        pilots = pmgr.submit_pilots(pdescs)

        # WORKLOAD

        # EXECUTION PATTERN: two stages, sequential:
        # - Describe CU for stage 1.
        # - Describe CU for stage 2.
        # - Run Stage 1.
        # - Retrieve all the output files from Stage 1.
        # - Stage all the output files of Stage 1 as input files for Stage 2 on
        #   all the remote resources on which a pilot is queued.
        # - Execute the task of Stage 2.
        # - Retrieve the output files.

        # CUs descriptions Stage 1
        print ("Creating Compute Units Stage 1: "),
        stage_1_cuds = []

        # for unit_count in range(0, BAG_SIZE):
        #     # OUPUT - Configure the staging action for output files.
        #     sd_outputs = {'source': ['state.cpt'   ,
        #                              'confout.gro' ,
        #                              'ener.edr'    ,
        #                              'traj.trr'    ,
        #                              'md.log'      ],
        #                   'target': ['outputs/state-%d.cpt'   % unit_count,
        #                              'outputs/confout-%d.gro' % unit_count,
        #                              'outputs/ener-%d.edr'    % unit_count,
        #                              'outputs/traj-%d.trr'    % unit_count,
        #                              'outputs/md-%d.log'      % unit_count],
        #                   'action':  radical.pilot.TRANSFER,
        #                   'flags' :  radical.pilot.CREATE_PARENTS}

        #     cud = radical.pilot.ComputeUnitDescription()
        #     cud.kernel         = 'GROMACS'
        #     cud.arguments      = ['-s', os.path.basename(SHARED_INPUT_FILE)]
        #     cud.cores          = 1
        #     cud.input_staging  = sd_shared
        #     cud.output_staging = sd_outputs

        # We assume a skeleton where staging files are created
        cud.input_staging = 'TODO'
        cud.output_staging = 'TODO'

        stage_1_cuds.append(cud)
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
            scheduler=radical.pilot.rp_scheduler)

        print "Unit Manager UIDs: {0}".format(session.list_unit_managers())

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
