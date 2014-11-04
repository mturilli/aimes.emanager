# pylint: disable-msg=C0103

__author__ = "Matteo Turillii, Andre Merzky"
__copyright__ = "Copyright 2013, The AIMES Project"
__license__ = "MIT"


"""This API wraps the native bundle API and represents a instantiated
bundle as hierarchy of Resources and Queues.  The API cannot be used to
construct bundles, only to represent them (the constructors for Resource
and Queue are private).

*usage of the API*

import bundle

bundle = Bundle('etc/bundle.conf')

for resource in bundle.resources:
    print resource.ID             # 'stampede.tacc.utexas.edu'
    print resource.queues         # ['default', 'development', 'large', ...]
    print resource.queue_default  # TODO: queue ID
    print resource.num_nodes      # 1234
    print resource.container      # job, VM
    print resource.get_bandwidth(resource.ID, IP, 'in')
    print resource.get_bandwidth(resource.ID, IP, 'out')

for queue in resource.queues:
    print queue.ID            # 'large'
    print queue.resource      # 'stampede.tacc.utexas.edu'
    print queue.num_nodes     # 6061
    print queue.num_cores     # 96976
    print queue.num_jobs      # 5

"""


import aimes.bundle


# -----------------------------------------------------------------------------
#
class Bundle(object):
    """The main class, Bundle, accepts a list or resource names/IDs and
    a named/IDed origin. The class uses the bundle module to create a
    bundle for each given resource. The origin is used to pull the
    bandwith available between each given resource and that origin. For
    the mock_api implementation below, assume that the bundle module
    provides information about the bundle structure in the priv_<xyz>
    classes.

    """

    def __init__ (self, config_file, origin) :

        self.bm = aimes.bundle.impl.BundleManager()
        self.bm.load_cluster_credentials(config_file)

        self.resource = list()
        self._priv    = self.bm.get_data ()

      # for cluster in self._priv['cluster_list'] :
      #     print "===> %s" % cluster
      #     print
      #     print "--   config"
      #     pprint.pprint (self._priv['cluster_config'][cluster])
      #     print
      #     print "--   workload"
      #     pprint.pprint (self._priv['cluster_workload'][cluster])
      #     print
      #     print "--   bandwidth"
      #     pprint.pprint (self._priv['cluster_bandwidth'][cluster])
      #     print
      #     print

        # we have a dictionary of Resources instances, indexed by resource name
        self.resources = dict()
        for resource_name in self._priv['cluster_list']:
            config    = self._priv['cluster_config'][resource_name]
            workload  = self._priv['cluster_workload'][resource_name]
            bandwidth = self._priv['cluster_bandwidth'][resource_name]

            self.resources[resource_name] = Resource (resource_name, config, workload, bandwidth)

        # and a list of Queue instances, for all queues of all resources
        self.queues = list()
        for resource in self.resources:
            self.queues += self.resources[resource].queues.values ()


# -----------------------------------------------------------------------------
#
class Resource(object) :
    """This class represents a set of information on a resource.
    Specifically, the class also has a list of Queue instances, which
    represent information about the resource's batch queues.  It can
    only be created during the get_resources() call.

    """

    def __init__(self, name, config, workload, bandwidth):

        self.name = name
        self.num_nodes = config['num_nodes']

        # we have a list of Queue instances, to inspect queue information,
        # indexed by queue name
        self.queues = dict()
        for queue_name in config['queue_info'] :
            self.queues[queue_name] = Queue(self.name, queue_name,
                                            config['queue_info'][queue_name],
                                            workload[queue_name])


    def get_bandwidth(self, tgt, mode) :
        # tgt:  target IP number
        # mode: 'in' or 'out', relative to resource
        # returns float in mbyte/sec
        return 0.0


# -----------------------------------------------------------------------------
#
class Queue(object):
    """This class represents a set of information on a batch queue of a
    specific resource. It can only be created from the respective
    instance of a Resource class.

    """

    def __init__ (self, resource_name, name, config, workload):

        self.name                 = name
        self.resource_name        = resource_name
        self.max_walltime         = config['max_walltime']
        self.num_procs_limit      = config['num_procs_limit']
        self.alive_nodes          = workload['alive_nodes']
        self.alive_procs          = workload['alive_procs']
        self.busy_nodes           = workload['busy_nodes']
        self.busy_procs           = workload['busy_procs']
        self.free_nodes           = workload['free_nodes']
        self.free_procs           = workload['free_procs']
        self.num_queueing_jobs    = workload['num_queueing_jobs']
        self.num_running_jobs     = workload['num_running_jobs']


