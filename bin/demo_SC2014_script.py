#!/usr/bin/env python

# pylint: disable-msg=C0103

"""Implements an Execution Manager for the AIMES demo.

TODO:
- Separate output from code. Create a function for each output block.
- Decompose each block into a function and create a main that calls the
  sequence of blocks composing the demo.
- Use radical.utils configuration tools instead of environment variables.
- Centralized authentication/authorization tokens into the configuration file.
- Create a python module for radical.demos.
- Get single colors to format arbitrary strings in print from
  radical.utils.Reporter.
"""

__author__ = "Matteo Turilli, Andre Merzky"
__copyright__ = "Copyright 2014, RADICAL"
__license__ = "MIT"

import os
import sys
import math
import pandas as pd

import radical.utils as ru
import radical.pilot as rp

import aimes.bundle
import aimes.emanager
import aimes.skeleton

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
EMANAGER_DEBUG = os.getenv("EMANAGER_DEBUG", None)

DEMO_TITLE = os.getenv("RUN_TAG")

DEMO_FOLDER = os.getenv("DEMO_FOLDER", None)
if DEMO_FOLDER is None:
    print "ERROR: DEMO_FOLDER is not defined."
    sys.exit(1)
else:
    DEMO_FOLDER = DEMO_FOLDER+'/'
    if EMANAGER_DEBUG:
        print "DEBUG - Demo root directory: %s" % DEMO_FOLDER

DBURL = os.getenv("RADICAL_PILOT_DBURL", None)
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Session database: %s" % DBURL

# The Skeleton configuration file.
SKELETON_CONF = os.getenv("SKELETON_CONF", None)
if SKELETON_CONF is None:
    print "ERROR: SKELETON_CONF (i.e. skeleton description file) not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Skeleton description file: %s" % SKELETON_CONF

# The bundle configuration file.
BUNDLE_CONF = os.getenv("BUNDLE_CONF", None)
if BUNDLE_CONF is None:
    print "ERROR: BUNDLE_CONF (i.e. bundle configuration file) not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Bundle configuration file: %s" % BUNDLE_CONF

# The IP from which the submission originates. From example, the IP of the
# virtual machine from which an AIMES demo is executed.
ORIGIN = os.getenv("ORIGIN", None)
if ORIGIN is None:
    print "ERROR: ORIGIN (i.e. your current IP address) not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - IP address: %s" % ORIGIN

BUNDLE_DBURL = os.getenv("BUNDLE_DBURL", None)
if BUNDLE_DBURL is None:
    print "ERROR: BUNDLE_DBURL not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - IP address: %s" % BUNDLE_DBURL

USER_ID = os.environ.get('AIMES_USER_ID', None)
if EMANAGER_DEBUG:
    print "Target resources user name: %s" % USER_ID

USER_KEY = os.environ.get('AIMES_USER_KEY', None)
if EMANAGER_DEBUG:
    print "Target resources user key: %s" % USER_KEY


# -----------------------------------------------------------------------------
# Reporter
# -----------------------------------------------------------------------------
# Create a reporter for the demo. Takes care of colors and font attributes.
report = ru.Reporter(title=DEMO_TITLE)

pd.set_option('display.width', 1000)

# -----------------------------------------------------------------------------
# skeleton
# -----------------------------------------------------------------------------
skeleton = aimes.skeleton.Skeleton(SKELETON_CONF)
skeleton.generate(mode='shell')

report.header("Skeleton Workflow S01")

# Calculate total data size of the given workflow.
total_input_data = 0.0
max_input_data = 0.0
average_input_data = 0.0

for task in skeleton.tasks:
    for i in task.inputs:
        total_input_data += float(i['size'])

total_output_data = 0.0
max_output_data = 0.0
average_output_data = 0.0

for task in skeleton.tasks:
    for o in task.outputs:
        total_output_data += float(o['size'])

report.info("Stages")
print "Type of workflow       : pipeline"
print "Total number of stages : %d" % len(skeleton.stages)
print "Total number of tasks  : %d" % len(skeleton.tasks)
print "Total input data       : %.2f MB" % float((total_input_data/1024)/1024)
print "Total output data      : %.2f MB" % float((total_output_data/1024)/1024)

for stage in skeleton.stages:

    report.info("%s" % stage.name)

    print "Number of tasks : %d" % len(stage.tasks)

    # Define the space and type homogeneity or heterogeneity of the tasks of
    # this stage.
    task_space_type = 'homogeneous'
    task_time_type = 'homogeneous'

    t_cores = None
    t_length = None

    for task in stage.tasks:

        if not t_cores:
            t_cores = task.cores

        elif not t_length:
            t_length = task.length

        elif t_cores != task.cores:
            task_space_type = 'heterogeneous'

        elif t_length != task.length:
            task_time_type = 'heterogeneous'

    if task_space_type == 'homogeneous' and task_time_type == 'homogeneous':
        print "Type of tasks   : space and time homogeneous"

    elif (task_space_type == 'heterogeneous' and
          task_time_type == 'heterogeneous'):
        print "Type of tasks   : space and time heterogeneous"

    elif (task_space_type == 'heterogeneous' and
          task_time_type == 'homogeneous'):
        print "Type of tasks   : space heterogeneous; time homogeneous"

    elif (task_space_type == 'homogeneous' and
          task_time_type == 'heterogeneous'):
        print "Type of tasks   : space homogeneous; time heterogeneous"

    # Find out how many input/output files and how much space they require for
    # this stage.
    stage_input_data = 0
    stage_input_files = 0

    for task in skeleton.tasks:
        if task.stage().name == stage.name:
            for i in task.inputs:
                stage_input_data += float(i['size'])
                stage_input_files += 1

    stage_output_data = 0
    stage_output_files = 0

    for task in skeleton.tasks:
        if task.stage().name == stage.name:
            for o in task.outputs:
                stage_output_data += float(o['size'])
                stage_output_files += 1

    print "Input files     : %d for a total of %.2f MB" % \
        (stage_input_files, float((stage_input_data/1024.0)/1024.0))

    print "Output files    : %d for a total of %.2f MB" % \
        (stage_output_files, float((stage_output_data/1024.0)/1024.0))

# Skeleton API DEBUG
#------------------------------------------------------------------------------
if EMANAGER_DEBUG:
    report.info("Skeleton S01 setup")
    commands = skeleton.setup()

    for cmd in commands:
        print cmd

    for stage in skeleton.stages:
        report.info("stage.name       : %s" % stage.name)
        print "stage.tasks      : %s" % stage.tasks

        # Derive stage size
        print "len(stage.tasks) : %s" % len(stage.tasks)

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

else:
    skeleton.setup()


# -----------------------------------------------------------------------------
# bundle
# -----------------------------------------------------------------------------
report.header("Resource Bundle B01")

bundle = aimes.bundle.Bundle(query_mode=aimes.bundle.DB_QUERY,
                             mongodb_url=BUNDLE_DBURL,
                             origin=ORIGIN)

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

# Get the total core capacity offered by the default queues of the target
# resources.
total_core_capacity = 0

for r_name in bundle.resources:
        resource = bundle.resources[r_name]

        for q_name in resource.queues:
            queue = resource.queues[q_name]

            if (q_name == 'normal' or
                    q_name == 'batch' or
                    q_name == 'default' or
                    q_name == 'regular'):
                total_core_capacity += queue.num_procs_limit

# Report back to the demo about the available resource bundle.
report.info("Target Resources")
print "IDs: %s" % \
    [str(bundle.resources[resource].name) for resource in bundle.resources]
print "Total core capacity: %i" % total_core_capacity
print

# I could derive this but no point doing it for the demo.
print "Acquiring real time-resource information:"
print "  Number of nodes.............................. OK"
print "  Type of container............................ OK"
print "  Bandwidth from origin to resource............ OK"
print "  Bandwidth from resource to origin............ OK"
print "  Queue names.................................. OK"
print "  Queue max walltime........................... OK"
print "  Queue max number of jobs..................... OK"
print "  Queue core capacity.......................... OK"
print "  Queue available capacity..................... OK"
print "  Queue length................................. OK"

# Bundle API DEBUG
#------------------------------------------------------------------------------
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


#------------------------------------------------------------------------------
# Execution Boundaries
# -----------------------------------------------------------------------------
# Calculate the min/max time taken by each stage to execute and the
# mix/max amount of cores needed. Factor data transfer time into min/max
# time. Note: Max(compute) <=> Min(time) & Min(compute) <=> Max(time)

stages_compute_limits = dict()
stages_time_limits = dict()
task_compute_limits = dict()
task_time_limits = dict()

stages_compute_limits['max'] = 0
stages_time_limits['max'] = 0
task_compute_limits['min'] = 0
task_time_limits['min'] = 0

for task in skeleton.tasks:

    ceil = int(math.ceil(task.length))

    stages_compute_limits['max'] += task.cores
    stages_time_limits['max'] += ceil

    # We assume tasks with heterogeneous core requirements.
    if task_compute_limits['min'] < task.cores:
        task_compute_limits['min'] = task.cores

    # We assume tasks with heterogeneous runtime.
    if task_time_limits['min'] < ceil:
        task_time_limits['min'] = ceil

report.header("Execution Strategy")

report.info("Goal for the execution of S01")
print "G01 - Minimize time to completion (MinTTC)"

report.info("Derive execution boundaries for S01")
print "B01 - lowest number of cores  : %s" % task_compute_limits['min']
print "      longest execution time  : %s seconds" % stages_time_limits['max']
print "B02 - highest number of cores : %s" % stages_compute_limits['max']
print "      shortest execution time : %s seconds" % task_time_limits['min']


#------------------------------------------------------------------------------
# Execution Strategy
# -----------------------------------------------------------------------------

# DEFINE EURISTICS
report.info("Select heuristics to satisfy G01 for the workflow S01")

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
                if (queue == 'normal' or
                        queue == 'batch' or
                        queue == 'default' or
                        queue == 'regular'):
                    data[label].append(resource.queues[queue].num_procs_limit)
                    break

        elif label == 'Queue length':
            for queue in resource.queues:
                if (queue == 'normal' or
                        queue == 'batch' or
                        queue == 'default' or
                        queue == 'regular'):
                    data[label].append(
                        resource.queues[queue].num_queueing_jobs)
                    break

        elif label == 'Load':
            for queue in resource.queues:
                if (queue == 'normal' or
                        queue == 'batch' or
                        queue == 'default' or
                        queue == 'regular'):
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

    tag = {'blacklight_psc_xsede_org': 'xsede.blacklight',
           'gordon_sdsc_xsede_org'   : 'xsede.gordon',
           'stampede_tacc_utexas_edu': 'xsede.stampede',
           'stampede_tacc_xsede_org' : 'xsede.stampede',
           'stampede_xsede_org'      : 'xsede.stampede',
           'trestles_sdsc_xsede_org' : 'xsede.trestles',
           'hopper_nersc_gov'        : 'nersc.hopper'}.get(resource)

    if not tag :
        sys.exit("Unknown resource specified in bundle: %s" % resource)

    return tag

resource_avail = resource_priority['Name'].tolist()

resources = list()

while len(resources) < eur_resources_number and \
        len(resources) < len(resource_avail):
    resources.append(uri_to_tag(resource_avail[len(resources)]))

print "Decision D03 based on D02, E02 - How many resource should be used?"

for resource in resources:
    print "  %s Selected" % resource.ljust(24, '.')
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
else:
    rp_scheduler = 'SCHED_DIRECT_SUBMISSION'

print "Decision D05 based on D03, D04 - What pilot scheduler should be \
used? %s" % rp_scheduler

# WE CANNOT CHOOSE ABOUT:
#
# - Early/late binding. We use late by default.
# - Data staging strategies. We do not have this kind of capabilities in RP.


#------------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
def pilot_state_cb(pilot, state):
    """Called every time a ComputePilot changes its state.
    """

    # Mitigate the erroneous management of the pilot state from the RP
    # back-end. In some conditions, the callback is called when the state of
    # the pilot is not available even if it should be.
    if pilot:

        print "\033[34mPilot pilot-%-13s is %-13s on %s\033[0m" % \
            (pilot.uid, state, pilot.resource)


def unit_state_change_cb(cu, state, pilots):
    """Called every time a ComputeUnit changes its state.
    """

    # Mitigate the erroneous management of the CU state from the RP back-end.
    # In some conditions, the callback is called when the state of the CU is
    # not available even if it should be.
    if cu:

        resource = None

        for pilot in pilots:
            if pilot.uid == cu.pilot_id:
                resource = pilot.resource
                break

        if not resource:
            print "\033[1mCU %-20s\033[0m (unit-%s) is %s" % \
                (cu.name, cu.uid, state)

        elif not cu.pilot_id:
            print "\033[1mCU %-20s\033[0m (unit-%s) is %-20s on %s" % \
                (cu.name, cu.uid, state, resource)

        else:
            print "\033[1mCU %-20s\033[0m (unit-%s) is %-20s on %-15s (pilot-%s)" % \
                (cu.name, cu.uid, state, resource, cu.pilot_id)


def wait_queue_size_cb(umgr, wait_queue_size):
    """Called when the size of the unit managers wait_queue changes.
    """
    print "\033[31mUnitManager (unit-manager-%s) has queue size: %s\033[0m" % \
        (umgr.uid, wait_queue_size)


# -----------------------------------------------------------------------------
# RUN DEMO
# -----------------------------------------------------------------------------
if __name__ == "__main__":

    report.header("Enacting Execution Strategy")

    report.info("Resource overlay")

    # SESSION
    #--------------------------------------------------------------------------
    session = rp.Session(database_url=DBURL)

    print "Execution session created        : UID %s" % session.uid

    context = rp.Context('ssh')

    if USER_ID:
        context.user_id = USER_ID

    if USER_KEY:
        context.user_key = USER_KEY

    session.add_context(context)

    print "Credentials for target resources : ***"

    try:
        # PILOT MANAGER
        #----------------------------------------------------------------------
        # One pilot manager is used for all pilots.
        pmgr = rp.PilotManager(session=session)

        print "Pilot Manager Initialized        : UID %s" % pmgr.uid

        # Register the pilot callback with the pilot manager. Called
        # every time any of the pilots managed by the pilot manager
        # changes its state.
        pmgr.register_callback(pilot_state_cb)

        # PILOT DESCRIPTIONS
        #----------------------------------------------------------------------
        # Number of cored for each pilot.
        cores = math.ceil((stages_compute_limits['max']*eur_concurrency)/100.0)

        print "Total number of cores            : %d" % cores
        print "Total number of pilots           : %i" % len(resources)

        # TIME COMPONENTS OF EACH PILOT WALLTIME
        #----------------------------------------------------------------------
        # Compute time: the time that a group of tasks take to run on a pilot
        # of the resource overlay given the decided degree of concurrency.
        # Requirements: we need to be able to run all the given tasks on a
        # single pilot - i.e. the worse case scenario where of the multiple
        # pilots, a single one is available for enough time to run the whole
        # workload at 1/n of the optimal concurrency of having all the pilots
        # available. Hidden assumption: pilots are heterogeneous - all have the
        # same walltime and number of cores.
        compute_time = task_time_limits['min']*len(resources)

        # Staging time: the time needed to move the files needed by each task
        # of each pilot. We assume a conservative 5 seconds per MB. This figure
        # needs extreme refinement, based on collected networking/data
        # writing/reading performance within and among each resource and the
        # origin point of the data.
        staging_time = (((total_input_data+total_output_data)/1024)/1024)*5

        # RP overhead time: the time taken by RP to bootstrap and manage each
        # CU for each pilot. Also this value needs to be assessed by reiterated
        # measurement.
        rp_overhead_time = 600+len(skeleton.tasks)*4

        report.info("Pilot descriptions")

        # List of all the pilot descriptions
        pdescs = []

        # Create a pilot description for each resource
        for resource in resources:

            print "Resource          : %s" % resource

            pdesc = rp.ComputePilotDescription()

            if 'stampede' in resource:
                pdesc.project = XSEDE_PROJECT_ID_STAMPEDE
                print "Allocation        : %s" % pdesc.project

            elif 'trestles' in resource:
                pdesc.project = XSEDE_PROJECT_ID_TRESTLES
                print "Allocation        : %s" % pdesc.project

            elif 'gordon' in resource:
                pdesc.project = XSEDE_PROJECT_ID_GORDON
                print "Allocation        : %s" % pdesc.project

            elif 'blacklight' in resource:
                pdesc.project = XSEDE_PROJECT_ID_BLACKLIGHT
                print "Allocation        : %s" % pdesc.project

            elif 'hopper' in resource:
                print "Allocation        : Default"

            else:
                print "ERROR: No XSEDE_PROJECT_ID given for resource %s." % \
                    resource
                sys.exit(1)

            pdesc.resource = resource  # label

            # Select a specific queue for hopper. This will become another
            # decision point inferred from queue information and inferred
            # duration of the workflow.
            if 'hopper' in pdesc.resource:
                pdesc.queue = 'regular'

            # We assume a uniform distribution of the total amount of cores
            # across all the available pilots. Future optimizations may take
            # into consideration properties of the resources that would justify
            # a biased/proportional distribution of the cores.
            pdesc.cores = math.ceil(float(cores/len(resources)))

            print "Number of cores   : %s" % pdesc.cores

            # Aggregate time components for the pilot walltime.
            pdesc.runtime = math.ceil((compute_time +
                                       staging_time +
                                       rp_overhead_time))/60.0

            print "Walltime          : %s minutes" % pdesc.runtime

            # We clean the pilot files once execution is done.
            pdesc.cleanup = True

            print "Clean remote data : True\n"

            pdescs.append(pdesc)

        # DEBUG: print the full description.
        if EMANAGER_DEBUG:
            for pdesc in pdescs:
                print pdesc

        # WORKLOAD
        #----------------------------------------------------------------------
        # EXECUTION PATTERN: two stages, sequential:
        # - Describe CU for stage 1.
        # - Describe CU for stage 2.
        # - Run Stage 1: unit input stage in; run; unit output stage out.
        # - Run Stage 2: unit input stage in; run; unit output stage out.
        # - Shutdown.

        # TOD: We leverage the knowledge we have of the skeleton - we as in
        # coders. This is ad hoc and will have to be replaced by an automated
        # understanding of the constraints on the execution of a specific type
        # of workflow. For example, the emanager will have to learn that the
        # type of Skeleton (or application) is a pipeline and will have to
        # infer that a pipeline requires a sequential execution of all its
        # stages.
        cuds = dict()

        for stage in skeleton.stages:

            report.info("CUs descriptions for %s" % stage.name)

            cuds[stage.name] = list()

            print("Tasks translated into CUs"),

            for task in stage.tasks:
                cud = rp.ComputeUnitDescription()
                cud.name = stage.name+'_'+task.name
                #cud.executable = "./%s" % task.command.split()[0]
                cud.executable = task.command.split()[0]
                cud.arguments = task.command.split()[1:]
                cud.cores = task.cores
                cud.pre_exec = list()
                cud.input_staging = list()
                cud.output_staging = list()

                # make sure the task is compiled on the fly
                # FIXME: it does not work with trestles as it assumes only a
                # working cc compiler.
                #cud.input_staging.append (aimes.skeleton.TASK_LOCATION)
                #cud.pre_exec.append      (aimes.skeleton.TASK_COMPILE)

                iodirs = task.command.split()[9:-1]
                odir = iodirs[-1].split('/')[0]

                for i in range(0, len(iodirs)):

                    if iodirs[i].split('/')[0] != odir:
                        idir = iodirs[i].split('/')[0]
                        break

                for i in task.inputs:
                    cud.input_staging.append({
                        'source': DEMO_FOLDER + idir + '/' + i['name'],
                        'target': idir + '/' + i['name'],
                        'flags': rp.CREATE_PARENTS
                        })

                for o in task.outputs:
                    cud.output_staging.append({
                        'source': odir + '/' + o['name'],
                        'target': odir + '/' + o['name'],
                        'flags': rp.CREATE_PARENTS
                        })

                # FIXME: restartable CUs still do not work.
                #cud.restartable = True
                cud.cleanup = True

                cuds[stage.name].append(cud)
                print(": %s " % cud.name),

            # This is just aesthetics.
            print

        # PILOT SUBMISSIONS
        #----------------------------------------------------------------------
        # Submit the pilots just described.
        report.info("Pilot submissions")

        for pdesc in pdescs:
            print "Pilot on resource %s SUBMITTED to PM %s" % \
                (pdesc.resource.ljust(21, '.'), pmgr.uid)
        print

        pilots = pmgr.submit_pilots(pdescs)

        # UNIT MANAGERS
        #----------------------------------------------------------------------
        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object. One unit manager will be used for all
        # the pilots. 'Late binding' scheduler is used to backfill (a
        # type of load balance) compute units to pilots when they become
        # available.
        report.info("Compute Units")

        #TODO: Get the name from the variable rp_scheduler
        umgr = rp.UnitManager(session=session, scheduler=rp.SCHED_BACKFILLING)

        # Register the unit manager callback
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)

        print "Unit Manager initialized      : UID %s" % umgr.uid

        # Add pilots to the unit manager.
        print "Adding pilots to Unit Manager :"
        umgr.add_pilots(pilots)

        for pdesc in pdescs:
            print "  Pilot on resource %s ADDED to UM %s" % \
                (pdesc.resource.ljust(21, '.'), umgr.uid)

        # Register the compute unit callback with the UnitManager.
        # Called every time any of the unit managed by the
        # UnittManager changes its state.
        umgr.register_callback(unit_state_change_cb, callback_data=pilots)

        # EXECUTION
        #----------------------------------------------------------------------
        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to
        # start assigning ComputeUnits to the ComputePilots.

        for stage in skeleton.stages:

            report.info("Executing %s" % stage.name)

            print "CUs of %s submitted to the Unit Manager: UID %s\n" % \
                (stage.name, umgr.uid)

            umgr.submit_units(cuds[stage.name])

            # Wait for all compute units to finish.
            umgr.wait_units()
            print "%s execution is done." % stage.name

        print "\nWorkflow execution is done."

        # CLEAN UP AND SHUT DOWN
        #----------------------------------------------------------------------
        # Close the session so to shutdown all the pilots cleanly
        report.header("Shutting down resource overlay")

        #session.close(cleanup=False, terminate=True)
        #sys.exit(0)

    except Exception as e:
        # this catches all RP and system exceptions
        print "Caught exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), we catch the corresponding
        # KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit which gets raised if the main threads exits for
        # some other reason.
        print "Caught exception, exit now: %s" % e
        raise

    finally:
        # always clean up the session, no matter whether we caught an exception
        report.header("End of AIMES SC2014 demo.")
        session.close(cleanup=False, terminate=True)
