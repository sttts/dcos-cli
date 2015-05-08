import fnmatch
import itertools
import os

from dcos import http, util
from dcos.errors import DCOSException

from six.moves import urllib

logger = util.get_logger(__name__)


def get_master(config=None):
    """Create a MesosMaster object using the url stored in the
    'core.mesos_master_url' property if it exists.  Otherwise, we use
    the `core.dcos_url` property

    :param config: user config
    :type config: Toml
    :returns: MesosMaster
    :rtype: MesosMaster
    """

    if config is None:
        config = util.get_config()

    mesos_master_url = config.get('core.mesos_master_url')
    if mesos_master_url is not None:
        return MesosMaster(None, mesos_master_url)
    else:
        dcos_url = util.get_config_vals(config, ['core.dcos_url'])[0]
        return MesosMaster(dcos_url, None)


MESOS_TIMEOUT = 5


class MesosMaster(object):
    """Mesos Master Client.  One of `dcos_url` or `mesos_master_url` must
    be non-None.

    :param dcos_url: dcos url
    :rtype dcos_url: str
    :param mesos_master_url: mesos master url
    :type mesos_master_url: str
    """

    def __init__(self, dcos_url, mesos_master_url):
        assert dcos_url or mesos_master_url

        self._dcos_url = dcos_url
        self._mesos_master_url = mesos_master_url
        self._state = None
        self._slaves = {}  # id->Slave map
        self._frameworks = {}  # id->Framework map

    def _base_url(self):
        """The base url of the master node.  Uses either DCOS reverse proxy or
        the master directly.

        :returns: base url
        :rtype: str
        """

        if self._mesos_master_url is not None:
            return self._mesos_master_url
        else:
            return urllib.parse.urljoin(self._dcos_url, 'mesos/')

    def state(self):
        """Returns the master's state.json.  Fetches it if we haven't already.

        :returns: state.json
        :rtype: dict
        """

        if not self._state:
            self._state = self.fetch('master/state.json').json()
        return self._state

    def slave_base_url(self, slave):
        """Returns the base url of the provided slave object.

        :param slave: slave to create a url for
        :type slave: MesosSlave
        :returns: slave's base url
        :rtype: str
        """
        if self._mesos_master_url is not None:
            slave_ip = slave['pid'].split('@')[1]
            return 'http://{}'.format(slave_ip)
        else:
            return urllib.parse.urljoin(self._dcos_url,
                                        'slave/{}/'.format(slave['id']))

    def slave(self, fltr):
        """Returns the slave that has `fltr` in its id.  Raises a
        DCOSException if there is not exactly one such slave.

        :param fltr: filter string
        :type fltr: str
        :returns: the slave that has `fltr` in its id
        :rtype: MesosSlave
        """

        slaves = self.slaves(fltr)

        if len(slaves) == 0:
            raise DCOSException('Slave {} no longer exists'.format(fltr))

        elif len(slaves) > 1:
            matches = ['\t{0}'.format(slave.id) for slave in slaves]
            raise DCOSException(
                "There are multiple slaves with that id. " +
                "Please choose one: {}".format('\n'.join(matches)))

        else:
            return slaves[0]

    def task(self, fltr):
        """Returns the task with `fltr` in its id.  Raises a DCOSException if
        there is not exactly one such task.

        :param fltr: filter string
        :type fltr: str
        :returns: the task that has `fltr` in its id
        :rtype: Task
        """

        tasks = self.tasks(fltr)

        if len(tasks) == 0:
            raise DCOSException(
                'Cannot find a task containing "{}"'.format(fltr))

        elif len(tasks) > 1:
            msg = ["There are multiple tasks with that id. Please choose one:"]
            msg += ["\t{0}".format(t["id"]) for t in tasks]
            raise DCOSException('\n'.join(msg))

        else:
            return tasks[0]


    def framework(self, framework_id):
        """Returns a framework by id

        :param framework_id: the framework's id
        :type framework_id: str
        :returns: the framework
        :rtype: Framework
        """

        for f in self._framework_dicts(active_only=False):
            if f['id'] == framework_id:
                return self._framework_obj(f)
        return None

    def slaves(self, fltr=""):
        """Returns those slaves that have `fltr` in their 'id'

        :param fltr: filter string
        :type fltr: str
        :returns: Those slaves that have `fltr` in their 'id'
        :rtype: [MesosSlave]
        """

        return [self._slave_obj(slave)
                for slave in self.state()['slaves']
                if fltr in slave['id']]

    def tasks(self, fltr="", completed=False):
        """Returns tasks running under the master

        :param fltr: May be a substring or regex.  Only return tasks
                     whose 'id' matches `fltr`.
        :type fltr: str
        :param completed: also include completed tasks
        :type completed: bool
        :returns: a list of tasks
        :rtype: [Task]
        """

        keys = ['tasks']
        if completed:
            keys = ['completed_tasks']

        tasks = []
        for framework in self._framework_dicts(active_only):
            for task in _merge(framework, keys):
                if fltr in task['id'] or fnmatch.fnmatchcase(task['id'], fltr):
                    task = self._framework_obj(framework).task(task['id'])
                    tasks.append(task)

        return tasks


    def frameworks(self, inactive=False, completed=False):
        """Returns a list of all frameworks

        :param inactive: also include inactive frameworks
        :type inactive: bool
        :param completed: also include completed frameworks
        :type completed: bool
        :returns: a list of frameworks
        :rtype: [Framework]
        """

        return [self._framework_obj(framework)
                for framework in self._framework_dicts(inactive, completed)]

    @util.duration
    def fetch(self, path, **kwargs):
        """GET the resource located at `path`

        :param path: the URL path
        :type path: str
        :param **kwargs: http.get kwargs
        :type **kwargs: dict
        :returns: the response object
        :rtype: Response
        """

        url = urllib.parse.urljoin(self._base_url(), path)
        return http.get(url, timeout=MESOS_TIMEOUT, **kwargs)

    def _slave_obj(self, slave):
        """Returns the Slave object corresponding to the provided `slave`
        dict.  Creates it if it doesn't exist already.

        :param slave: slave
        :type slave: dict
        :returns: MesosSlave
        :rtype: MesosSlave
        """

        if slave['id'] not in self._slaves:
            self._slaves[slave['id']] = MesosSlave(slave, self)
        return self._slaves[slave['id']]

    def _framework_obj(self, framework):
        """Returns the Framework object corresponding to the provided `framework`
        dict.  Creates it if it doesn't exist already.

        :param framework: framework
        :type framework: dict
        :returns: Framework
        :rtype: Framework
        """

        if framework['id'] not in self._frameworks:
            self._frameworks[framework['id']] = Framework(framework, self)
        return self._frameworks[framework['id']]

    def _framework_dicts(self, inactive=False, completed=False):
        """Returns a list of all frameworks as their raw dictionaries

        :param inactive: also include inactive frameworks
        :type inactive: bool
        :param completed: also include completed frameworks
        :type completed: bool
        :returns: a list of frameworks
        """

        keys = ['frameworks']
        if completed:
            keys.append('completed_frameworks')
        for framework in _merge(self.state(), keys):
            if inactive or framework['active']:
                yield framework


class MesosSlave(object):
    """Mesos Slave Client

    :param slave: dictionary representing the slave.
                  retrieved from master/state.json
    :type slave: dict
    :param master: slave's master
    :type master: MesosMaster
    """

    def __init__(self, slave, master):
        self._slave = slave
        self._master = master
        self._state = None

    def state(self):
        """Returns the slave's state.json.  Fetches it if we haven't already.

        :returns: state.json
        :rtype: dict
        """

        if not self._state:
            self._state = self.fetch('state.json').json()
        return self._state

    def base_url(self):
        """The base url of the slave node.  Uses either DCOS reverse proxy or
        the slave directly.

        :returns: base url
        :rtype: str
        """

        return self._master.slave_base_url(self)

    @util.duration
    def fetch(self, path, **kwargs):
        """GET the resource located at `path`

        :param path: the URL path
        :type path: str
        :param **kwargs: http.get kwargs
        :type **kwargs: dict
        :returns: the response object
        :rtype: Response
        """

        url = urllib.parse.urljoin(self.base_url(), path)
        return http.get(url, timeout=MESOS_TIMEOUT, **kwargs)

    def _framework_dicts(self):
        """Returns the framework dictionaries from the state.json dict

        :returns: frameworks
        :rtype: [dict]
        """

        return _merge(self.state(), ['frameworks', 'completed_frameworks'])

    def executor_dicts(self):
        """Returns the executor dictionaries from the state.json dict

        :returns: executors
        :rtype: [dict]
        """

        iters = [_merge(framework, ['executors', 'completed_executors'])
                 for framework in self._framework_dicts()]
        return itertools.chain(*iters)

    def __getitem__(self, name):
        """Support the slave[attr] syntax

        :param name: attribute to get
        :type name: str
        :returns: the value for this attribute in the underlying
                  slave dictionary
        :rtype: object
        """

        return self._slave[name]


class Framework(object):
    """ Mesos Framework Model

    :param framework: framework properties
    :type framework: dict
    :param master: framework's master
    :type master: MesosMaster
    """

    def __init__(self, framework, master):
        self._framework = framework
        self._master = master
        self._tasks = {}  # id->Task map

    def task(self, task_id):
        """Returns a task by id

        :param task_id: the task's id
        :type task_id: str
        :returns: the task
        :rtype: Task
        """

        for task in _merge(self._framework, ['tasks', 'completed_tasks']):
            if task['id'] == task_id:
                return self._task_obj(task)
        return None

    def _task_obj(self, task):
        """Returns the Task object corresponding to the provided `task`
        dict.  Creates it if it doesn't exist already.

        :param task: task
        :type task: dict
        :returns: Task
        :rtype: Task
        """

        if task['id'] not in self._tasks:
            self._tasks[task['id']] = Task(task, self._master)
        return self._tasks[task['id']]

    def dict(self):
        return self._framework

    def __getitem__(self, name):
        """Support the framework[attr] syntax

        :param name: attribute to get
        :type name: str
        :returns: the value for this attribute in the underlying
                  framework dictionary
        :rtype: object
        """

        return self._framework[name]


class Task(object):
    """Mesos Task Model.

    :param master: mesos master
    :type master: MesosMaster
    :param task: task properties
    :type task: dict

    """

    def __init__(self, task, master):
        self._task = task
        self._master = master

    def dict(self):
        """
        :returns: dictionary representation of this Task
        :rtype: dict
        """

        return self._task

    def framework(self):
        """Returns this task's framework

        :returns: task's framework
        :rtype: Framework
        """

        return self._master.framework(self["framework_id"])

    def user(self):
        """Task owner

        :returns: task owner
        :rtype: str
        """

        return self.framework()['user']

    def slave(self):
        """ Returns this task's slave

        :returns: task's slave
        :rtype: MesosSlave
        """

        return self._master.slave(self["slave_id"])

    def executor(self):
        """ Returns this tasks' executor

        :returns: task's executor
        :rtype: dict
        """
        for executor in self.slave().executor_dicts():
            tasks = _merge(executor,
                           ['completed_tasks',
                            'tasks',
                            'queued_tasks'])
            if any(task['id'] == self['id'] for task in tasks):
                return executor
        raise DCOSException(
            'Could not find an executor for task [{0}]'.format(self['id']))

    def directory(self):
        """ Sandbox directory for this task

        :returns: path to task's sandbox
        :rtype: str
        """
        return self.executor()['directory']

    def __getitem__(self, name):
        """Support the task[attr] syntax

        :param name: attribute to get
        :type name: str
        :returns: the value for this attribute in the underlying
                  task dictionary
        :rtype: object
        """

        return self._task[name]


class MesosFile(object):
    """File-like object that is backed by a remote slave file.  Uses the
    files/read.json endpoint.  This endpoint isn't well documented
    anywhere, so here is the spec I've derived from the code:

    request format:
    {
       path: absolute path to read
       offset: start byte location, or -1.  -1 means read no data, and
               is used to fetch the size of the file in the response's
               'offset' parameter.
       length: number of bytes to read, or -1.  -1 means read the whole file.
    }

    response format:
    {
      data: file data.  Empty if a request.offset=-1.  Could be
            smaller than request.length if EOF was reached, or if (I
            believe) request.length is larger than the length
            supported by the server (16 pages I believe).

      offset: the offset value from the request, or the size of the
              file if the request offset was -1 or >= the file size.
    }

    :param task: file's task
    :type task: Task
    :param path: file's path, relative to the sandbox
    :type path: str
    """

    def __init__(self, task, path):
        self._task = task
        self._path = path
        self._cursor = 0

    def size(self):
        """Size of the file

        :returns: size of the file
        :rtype: int
        """

        params = self._params(0, offset=-1)
        return self._fetch(params)["offset"]

    def seek(self, offset, whence=os.SEEK_SET):
        """Seek to the provided location in the file.

        :param offset: location to seek to
        :type offset: int
        :param whence: determines whether `offset` represents a
                       location that is absolute, relative to the
                       beginning of the file, or relative to the end
                       of the file
        :type whence: os.SEEK_SET | os.SEEK_CUR | os.SEEK_END
        :returns: None
        :rtype: None
        """

        if whence == os.SEEK_SET:
            self._cursor = 0 + offset
        elif whence == os.SEEK_CUR:
            self._cursor += offset
        elif whence == os.SEEK_END:
            self._cursor = self.size() + offset

    def tell(self):
        """ The current cursor position.

        :returns: the current cursor position
        :rtype: int
        """

        return self._cursor

    def read(self, length=None):
        """Reads up to `length` bytes, or the entire file if `length` is None.

        :param length: number of bytes to read
        :type length: int | None
        :returns: data read
        :rtype: str
        """

        data = ''
        while length is None or length - len(data) > 0:
            chunk_length = -1 if length is None else length - len(data)
            chunk = self._fetch_chunk(chunk_length)
            if chunk == '':
                break
            data += chunk

        return data

    def _host_path(self):
        """ The absolute path to the file on slave.

        :returns: the absolute path to the file on slave
        :rtype: str
        """

        directory = self._task.directory()
        if directory[-1] == '/':
            return directory + self._path
        else:
            return directory + '/' + self._path

    def _params(self, length, offset=None):
        """GET parameters to send to files/read.json.  See the MesosFile
        docstring for full information.

        :param length: number of bytes to read
        :type length: int
        :param offset: start location.  if None, will use the location
                       of the current file cursor
        :type offset: int
        :returns: GET parameters
        :rtype: dict
        """

        if offset is None:
            offset = self._cursor

        return {
            'path': self._host_path(),
            'offset': offset,
            'length': length
        }

    def _fetch_chunk(self, length, offset=None):
        """Fetch data from files/read.json

        :param length: number of bytes to fetch
        :type length: int
        :param offset: start location.  If not None, this file's
                       cursor is set to `offset`
        :type offset: int
        :returns: data read
        :rtype: str
        """

        if offset is not None:
            self.seek(offset, os.SEEK_SET)

        params = self._params(length)
        data = self._fetch(params)["data"]
        self.seek(len(data), os.SEEK_CUR)
        return data

    def _fetch(self, params):
        """Fetch data from files/read.json

        :param params: GET parameters
        :type params: dict
        :returns: response dict
        :rtype: dict
        """

        resp = self._task.slave().fetch("files/read.json",
                                        params=params)
        return resp.json()

    def __str__(self):
        """String representation of the file: <task_id:file_path>

        :returns: string representation of the file
        :rtype: str
        """

        return "{0}:{1}".format(self._task['id'], self._path)


def _merge(d, keys):
    """ Merge multiple lists from a dictionary into one iterator.
        e.g. _merge({'a': [1, 2], 'b': [3]}, ['a', 'b']) ->
             iter(1, 2, 3)

    :param d: dictionary
    :type d: dict
    :param keys: keys to merge
    :type keys: [hashable]
    :returns: iterator
    :rtype: iter
    """

    return itertools.chain(*[d[k] for k in keys])
