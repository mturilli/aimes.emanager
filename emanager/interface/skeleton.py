# pylint: disable-msg=C0103

__author__ = "Matteo Turillii, Andre Merzky"
__copyright__ = "Copyright 2013, The AIMES Project"
__license__ = "MIT"


"""This API wraps the native skeleton API and represents a instantiated
skeleton as hierarchy of Stages and Tasks.  The API cannot be used to
construct skeletons, only to represent skeletons (the constructors for
Stage and Task are private).

The main class, Skeleton, accepts a file name as construction parameter,
and will use the skeleton module to parse that respective file into a
skeleton represenation, i.e. Stages and Tasks are constructed according
to the skeleton description in the file.  For the mock_api
implementation below, assume that the skeleton module provides
information about the skeleton structure in the priv_<xyz> classes.

Note that Tasks contain lists of inputs and outputs (which are rendered
as dictionaries), and that those lists in turn contain lists of Tasks
(where the inputs originate; where the outputs will be consumed).  That
circularity is ignored in the mockup below (needs weakref's).

*usage of the API*

skeleton = Skeleton('etc/skeleton.conf')

for stage in skeleton.stages:
    print stage.ID
    print stage.tasks

    # Derive stage size
    print len(stage.tasks)

    # Derive stage duration
    print sum(task.runtime for task in stage.tasks)

    # Derive stage staged-in data
    print sum(task.i.size for task in stage.tasks)


for task in skeleton.tasks:
    print task.ID
    print task.description
    print task.stage
    print task.runtime
    print task.inputs
    print task.outputs
    print task.kernel
    print task.executable
    print task.arguments
    print task.cores

    for i in task.inputs:
        print i.ID
        print i.file_name
        print i.file_path
        print i.size
        print i.tasks

    for o in task.outputs:
        print o.ID
        print o.file_name
        print o.file_path
        print o.size
        print o.tasks

"""


import aimes.skeleton
import weakref
import json



# -----------------------------------------------------------------------------
#
class Skeleton(object) :
    """This is the base class.  A skeleton is created according to a
    skeleton description (in a file).  It consists of several stages,
    each stage consists of several tasks.  The Skeleton class also
    exposes the complete set of tasks for inspection.

    """

    def __init__(self, filename):

        self._app   = aimes.skeleton.Application ("skeleton", filename, 'shell')
        self._app.generate ()

        self.name   = filename
        self.shell  = self._app.as_shell ()
        self._json  = self._app.as_json  ()
        self._priv  = json.loads (self._json)

        if self.name.endswith (".input"):
            self.name = self.name[:-6]

        self.stages = list()
        for priv_stage in self._priv['stagelist']:
            self.stages.append(Stage(priv_stage, skeleton=self))

        self.tasks = list()
        for stage in self.stages:
            self.tasks += stage.tasks


    def __str__ (self) :

        out = "Skeleton: %s\n" % self.name
        for s in self.stages :
            out += str(s)
        out += "\n"
        out += "Shell   : \n%s\n" % self.shell
        return out



# -----------------------------------------------------------------------------
#
class Stage(object) :
    """A stage is a set of tasks which are independent from each other
    (vs. tasks from different stages, which can have dependencies).

    """

    def __init__ (self, priv, skeleton):

        self._priv    = priv
        self.name     = self._priv['name']
        self.skeleton = weakref.ref (skeleton)
        self.tasks    = list()

        for priv_task in self._priv['task_list']:
            self.tasks.append(Task(priv_task, stage=self))


    def __str__ (self) :

        out = "  Stage : %s\n" % self.name
        for t in self.tasks :
            out += str(t)
        return out

# -----------------------------------------------------------------------------
#
class Task(object) :
    """A task is an element of a stage, and represents a unit of work.
    A task can depend on multiple input files, and can produce multiple
    output files.

    """

    # no public constructor

    def __init__ (self, priv, stage):

        self._priv     = priv
        self.name      = self._priv['taskid']
        self.stage     = weakref.ref (stage)
        self.length    = int(self._priv['length'])
        self.cores     = int(self._priv['processes'])
        self.ttype     = self._priv['task_type']
        self.inputs    = list()
        self.outputs   = list()

        # inputs and outputs are represented as dictionaries. One could
        # render those information as classes as well.

        for priv_input in self._priv['inputlist']:
            input_dict = dict()
            input_dict['name'] = priv_input['name']
            input_dict['size'] = priv_input['size']

          # input_dict['tasks'] = list()
          # for priv_task in priv_input.tasks:
          #     input_dict['tasks'].append(Task._create (self._priv))

            self.inputs.append (input_dict)


        for priv_output in self._priv['outputlist']:
            output_dict = dict()
            output_dict['name'] = priv_output['name']
            output_dict['size'] = priv_output['size']

          # output_dict['tasks'] = list()
          # for priv_task in priv_output.tasks:
          #     output_dict['tasks'].append(Task._create (priv_task))


    def __str__ (self) :

        out  = "    Task: %s\n" % self.name
        out += "        : cores  : %s\n" % self.cores
        out += "        : length : %s\n" % self.length
        out += "        : ttype  : %s\n" % self.ttype
        out += "        : inputs : %s\n" % len (self.inputs)

        for i in self.inputs :
            out += "        :        : %10s %s\n" % (i['name'], i['size'])

        out += "        : outputs: %s\n" % len (self.outputs)
        for o in self.outputs :
            out += "        :        : %10s %s\n" % (i['name'], i['size'])

        return out


# -----------------------------------------------------------------------------
#
# The API also includes a free function which can convert a (set of)
# Skeleton Task(s) into a (set of) RADICAL-Pilot
# ComputeUnitDescription(s).
#
def task_to_cud (tasks) :

    import radical.pilot

    return_list = True
    if not isinstance (tasks, list):
        tasks = [tasks]
        return_list = False

    ret = list()
    for task in tasks:
        cud = radical.pilot.ComputeUnitDescription ()
        cud.executable = "/bin/sleep"
        cud.arguments  = [str(task.length)]

        ret.append (cud)

    return ret

