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
    print resource.ID         # 'stampede.tacc.utexas.edu'
    print resource.queues     # ['default', 'development', 'large', ...]
    print resource.num_nodes  # 1234
    print resource.container  # job, VM
    print resource.get_bandwidth(resource.ID, IP, 'in')
    print resource.get_bandwidth(resource.ID, IP, 'out')

for queue in resource.queues:
    print queue.ID            # 'large'
    print queue.resource      # 'stampede.tacc.utexas.edu'
    print queue.num_nodes     # 6061
    print queue.num_cores     # 96976
    print queue.num_jobs      # 5

"""


import bundle


# -----------------------------------------------------------------------------
#
class Bundle(resources, origin):
    """The main class, Bundle, accepts a list or resource names/IDs and
    a named/IDed origin. The class uses the bundle module to create a
    bundle for each given resource. The origin is used to pull the
    bandwith available between each given resource and that origin. For
    the mock_api implementation below, assume that the bundle module
    provides information about the bundle structure in the priv_<xyz>
    classes.

    """

    self._priv = aimes.bundle.acquire_resources(resources)
                 # or something like this.

    # we have a dictionary of Resources instances, indexed by resource name
    self.resources = dict()
    for priv_resource in self._priv.resources:
        self.resources[priv_resource.ID] = Resource._create(priv_resource)

    # and a list of Queue instances, for all queues of all resources
    self.queues = list()
    for resource in self.resources:
        self.queues += resource.queues.values ()


# -----------------------------------------------------------------------------
#
class Resource(object) :
    """This class represents a set of information on a resource.
    Specifically, the class also has a list of Queue instances, which
    represent information about the resource's batch queues.  It can
    only be created during the get_resources() call.

    """

    # no public constructor

    @classmethod
    def _create(cls, priv_resource):

        r = Resource()
        r.ID = None
        r.num_nodes = None
        r.container = None

        # we have a list of Queue instances, to inspect queue information,
        # indexed by queue name
        r.queues = dict()
        for priv_queue in priv_resource.queues :
            r.queues[priv_queue.ID] = Queue._create(priv_queue)

        return r

    def get_bandwidth(self, tgt, mode) :
        # tgt:  target IP number
        # mode: 'in' or 'out', relative to resource
        # returns float in mbyte/sec
        pass


# -----------------------------------------------------------------------------
#
class Queue(object):
    """This class represents a set of information on a batch queue of a
    specific resource. It can only be created from the respective
    instance of a Resource class.

    """

    # no public constructor

    @classmethod
    def _create(cls, priv_queue):

        q = Queue()
        q.ID = None
        q.resource = None
        q.num_nodes = None
        q.num_cores = None
        q.num_jobs = None

        return q

