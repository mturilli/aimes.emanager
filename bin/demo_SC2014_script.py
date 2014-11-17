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

import radical.pilot as rp
import aimes.bundle
import aimes.emanager
import aimes.emanager.interface

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# TODO: use radical.utils configuration module instead of environmental
# variables.
EMANAGER_DEBUG = os.getenv("EMANAGER_DEBUG")

DEMO_FOLDER = os.getenv("DEMO_FOLDER")
if DEMO_FOLDER is None:
    print "ERROR: DEMO_FOLDER is not defined."
    sys.exit(1)
else:
    if EMANAGER_DEBUG:
        print "DEBUG - Demo root directory: %s" % DEMO_FOLDER

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

report.header("Skeleton Workflow S01")

# Calculate total data size of the given workflow.
total_input_data = 0

for task in skeleton.tasks:
    for i in task.inputs:
        total_input_data += int(i['size'])

total_output_data = 0

for task in skeleton.tasks:
    for o in task.outputs:
        total_output_data += int(o['size'])

report.info("Stages")
print "Type of workflow       : pipeline"
print "Total number of stages : %d" % len(skeleton.stages)
print "Total number of tasks  : %d" % len(skeleton.tasks)
print "Total input data       : %d MB" % ((total_input_data/1024)/1024)
print "Total output data      : %d MB" % ((total_output_data/104)/1024)

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
            t_cores == task.cores

        elif t_cores and t_cores != task.cores:
            task_space_type = 'heterogeneous'

        elif not t_length:
            t_length = task.length

        elif t_length and t_length != task.length:
            task_time_type = 'heterogeneous'


    if task_space_type == 'homogeneous' and task_time_type == 'homogeneous':
        print "Type of tasks   : space and time homogeneous"

    elif task_space_type == 'heterogeneous' and task_time_type == 'heterogeneous':
        print "Type of tasks   : space and time heterogeneous"

    elif task_space_type == 'heterogeneous' and task_time_type == 'homogeneous':
        print "Type of tasks   : space heterogeneous; time homogeneous"

    elif task_space_type == 'homogeneous' and task_time_type == 'heterogeneous':
        print "Type of tasks   : space homogeneous; time heterogeneous"

    # Find out how many input/output files and how much space they require for
    # this stage.
    stage_input_data = 0
    stage_input_files = 0

    for task in skeleton.tasks:
        if task.stage().name == stage.name:
            for i in task.inputs:
                stage_input_data += int(i['size'])
                stage_input_files += 1

    stage_output_data = 0
    stage_output_files = 0

    for task in skeleton.tasks:
        if task.stage().name == stage.name:
            for o in task.outputs:
                stage_output_data += int(o['size'])
                stage_output_files += 1

    print "Input files     : %d for a total of %d MB" % \
        (stage_input_files, (stage_input_data/1024)/1024)

    print "Output files    : %d for a total of %d MB" % \
        (stage_output_files, (stage_output_data/1024)/1024)

# eManager DEBUG
#------------------------------------------------------------------------------
if EMANAGER_DEBUG:
    report.info("Skeleton S01 setup")
    commands = skeleton.setup()

    for cmd in commands:
        print cmd

    for stage in skeleton.stages:
        report.info("stage.name       : %s" % stage.name)
        print "stage.tasks      : %s" % stage.tasks
        print "stage.inputdir[0]: %s" % stage.inputdir[0]

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
    [bundle.resources[resource].name for resource in bundle.resources]

# I could derive this but no point doing it for the demo.
print "Total core capacity: 7168"
print
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

stages_compute_limits['min'] = 1
stages_compute_limits['max'] = 0
stages_time_limits['min'] = 0
stages_time_limits['max'] = 0

for task in skeleton.tasks:
    stages_compute_limits['max'] += task.cores
    stages_time_limits['max'] += task.length

    # We assume tasks with heterogeneous runtime.
    if stages_time_limits['min'] < task.length:
        stages_time_limits['min'] = task.length

report.header("Execution Strategy")

report.info("Goal for the execution of S01")
print "G01 - Minimize time to completion (MinTTC)"

report.info("Derive execution boundaries for S01")
print "B01 - lowest number of cores  : %s" % stages_compute_limits['min']
print "      longest execution time  : %s seconds" % stages_time_limits['max']
print "B02 - highest number of cores : %s" % stages_compute_limits['max']
print "      shortest execution time : %s seconds" % stages_time_limits['min']


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

    print "\033[92mPilot %s is %s on %s\033[0m" % \
        (pilot.uid, state.ljust(13), pilot.resource.ljust(17))


def unit_state_change_cb(cu, state, pilots):
    """unit_state_change_cb() is a callback function. It gets called
    very time a ComputeUnit changes its state.
    """

    resource = None
    pilot_id = None
    for pilot in pilots:
        if pilot.uid == cu.pilot_id:
            resource = pilot.resource
            break

    if not resource:
        print "\033[1mCU %s\033[0m (unit-%s) is %s" % \
            (cu.name.ljust(12), cu.uid, state.ljust(20))

    elif not pilot_id:
        print "\033[1mCU %s\033[0m (unit-%s) is %s on %s" % \
            (cu.name.ljust(12),
             cu.uid, state.ljust(20),
             resource)

    else:
        print "\033[1mCU %s\033[0m (unit-%s) is %s on %s (pilot-%s)" % \
            (cu.name.ljust(12),
             cu.uid, state.ljust(20),
             resource,
             cu.pilot_id)

    if state == rp.FAILED:
        sys.exit(1)


def wait_queue_size_cb(umgr, wait_queue_size):
    """Called when the size of the unit managers wait_queue changes.
    """
    print "\033[1mUnitManager\033[0m '%s' is %s" % \
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
    context.user_id = 'mturilli'
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

        # TODO: derive overhead dynamically from stage_in time + agent
        # bootstrap + agent task queue management overheads. This would
        # required robust stats about the available bandwidth.
        if len(skeleton.tasks) <= 20:
            rp_overhead = 15

        elif len(skeleton.tasks) <= 2048:
            rp_overhead = 45

        elif len(skeleton.tasks) <= 3072:
            rp_overhead = 60

        elif len(skeleton.tasks) <= 4096:
            rp_overhead = 75

        print "Total number of pilots           : %i" % len(resources)

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

            print "Number of cores   : %s" % pdesc.cores

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

            print "Walltime          : %s minutes" % pdesc.runtime

            pdesc.cleanup = True

            print "Clean remote data : TRUE\n"

            pdescs.append(pdesc)

        if EMANAGER_DEBUG:
            for pdesc in pdescs:
                print pdesc

        # WORKLOAD
        #----------------------------------------------------------------------
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
        report.info("CUs descriptions for Stage 1")

        # TOD: We leverage the knowledge we have of the skeleton - we as in
        # coders. This is ad hoc and will have to be replaced by an automated
        # understanding of the constraints on the execution of a specific type
        # of workload. For example, the emanager will have to learn that the
        # type of Skeleton (or applicaiton) is a pipeline and will have to
        # infer by means of a knowledge base that a pipeline requires a
        # sequential execution of all its stages. The creation of the CUs in
        # terms of how and when will depend on that inference.
        stage_1_cuds = []

        print("Tasks translated into CUs"),

        for task in skeleton.tasks:
            if task.stage().name == "Stage_1":

                cud = rp.ComputeUnitDescription()
                cud.name = task.name
                cud.executable = "task"
                cud.arguments = task.command.split()[1:]
                cud.cores = 1
                cud.input_staging = list()
                cud.output_staging = list()

                for i in task.inputs:
                    cud.input_staging.append({
                        'source': DEMO_FOLDER + '/Stage_1_Input/' + i['name'],
                        'target': 'Stage_1_Input/' + i['name'],
                        'flags': rp.CREATE_PARENTS
                        })

                for o in task.outputs:
                    cud.output_staging.append({
                        'source': 'Stage_1_Output/' + o['name'],
                        'target': 'Stage_1_Output/' + o['name'],
                        'flags': rp.CREATE_PARENTS
                        })

                cud.cleanup = True

                stage_1_cuds.append(cud)
                print(": %s " % cud.name),

        # CUs descriptions Stage 2
        report.info("\nCUs descriptions for Stage 2")

        stage_2_cuds = []

        print("Tasks translated into CUs"),

        for task in skeleton.tasks:
            if task.stage().name == "Stage_2":

                cud = rp.ComputeUnitDescription()
                cud.name = task.name
                cud.executable = "task"
                cud.arguments = task.command.split()[1:]
                cud.cores = 1
                cud.input_staging = list()
                cud.output_staging = list()

                for i in task.inputs:
                    cud.input_staging.append({
                        'source': DEMO_FOLDER + '/Stage_1_Output/' + i['name'],
                        'target': 'Stage_1_Output/' + i['name'],
                        'flags': rp.CREATE_PARENTS
                        })

                for o in task.outputs:
                    cud.output_staging.append({
                        'source': 'Stage_2_Output/' + o['name'],
                        'target': 'Stage_2_Output/' + o['name'],
                        'flags': rp.CREATE_PARENTS
                        })

                cud.cleanup = True

                stage_2_cuds.append(cud)
                print(": %s" % cud.name),

        # PILOT SUBMISSIONS
        #----------------------------------------------------------------------
        # Submit the pilots just described.
        report.info("\nPilot submissions")

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
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_BACKFILLING)

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
        report.info("\nExecuting Stage 1")
        print "CUs of Stage 1 submitted to the Unit Manager: UID %s\n" % \
            umgr.uid

        umgr.submit_units(stage_1_cuds)

        # Wait for all compute units to finish.
        umgr.wait_units()
        print "Execution done."

        # Execute Stage 2
        report.info("Executing Stage 2")
        print "CUs of Stage 2 submitted to the Unit Manager: UID %s\n" % \
            umgr.uid

        umgr.submit_units(stage_2_cuds)

        # Wait for all compute units to finish.
        umgr.wait_units()
        report.ok("\nExecution done.")

        # CLEAN UP AND SHUT DOWN
        #----------------------------------------------------------------------
        # Close the session so to shutdown all the pilots cleanly
        report.header("Shutting down resource overlay")

        session.close(cleanup=False, terminate=True)
        #sys.exit(0)

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
        report.header("End of AIMES SC2014 demo.")
        session.close(cleanup=False, terminate=True)
