"""Output the last part of files in a task's sandbox

Usage:
    dcos tail --info
    dcos tail [--follow --inactive --lines=N] <task> <file>

Options:
    -h, --help    Show this screen
    --info        Show a short description of this subcommand
    --follow      Output data as the file grows
    --inactive    Show inactive tasks as well
    --lines=N     Output the last N lines [default: 10]
    --version     Show version

Positional Arguments:

    <task>        Only match tasks whose ID matches <task>.  <task> may be
                  some substring of the ID, or a regular expression.
"""

import functools
import time

import concurrent.futures
import dcoscli
import docopt
import requests
from dcos import cmds, emitting, mesos, util
from dcos.errors import DCOSException, DefaultError

logger = util.get_logger(__name__)
emitter = emitting.FlatEmitter()


def main():
    try:
        return _main()
    except DCOSException as e:
        emitter.publish(e)
        return 1


def _main():
    util.configure_logger_from_environ()

    args = docopt.docopt(
        __doc__,
        version="dcos-tail version {}".format(dcoscli.version))

    return cmds.execute(_cmds(), args)


def _cmds():
    """
    :returns: All of the supported commands
    :rtype: [Command]
    """

    return [
        cmds.Command(
            hierarchy=['tail', '--info'],
            arg_keys=[],
            function=_info),

        cmds.Command(
            hierarchy=['tail'],
            arg_keys=['--follow', '--inactive', '--lines', '<task>', '<file>'],
            function=_tail),
    ]


def _info():
    """Print tail cli information.

    :returns: process return code
    :rtype: int
    """

    emitter.publish(__doc__.split('\n')[0])
    return 0


def _tail(follow, inactive, lines, task, path):
    """ Tail a file in the task's sandbox.

    :param follow: same as unix tail's -f
    :type follow: bool
    :param inactive: whether to include inactive tasks
    :type inactive: bool
    :param lines: number of lines to print
    :type lines: int
    :param task: task pattern to match
    :type task: str
    :param path: file path to read
    :type path: str
    :returns: process return code
    :rtype: int
    """

    if task is None:
        fltr = ""
    else:
        fltr = task

    lines = int(lines)

    mesos_files = _mesos_files(inactive, fltr, path)
    if not mesos_files:
        raise DCOSException('No files to read.  Exiting.')

    fn = functools.partial(_read_last_lines, lines)
    curr_header = None
    curr_header, mesos_files = _stream_files(curr_header, fn, mesos_files)

    while follow:
        curr_header, mesos_files = _stream_files(curr_header,
                                                 _read_rest,
                                                 mesos_files)
        if not mesos_files:
            raise DCOSException('No files to read.  Exiting')
        time.sleep(1)

    return 0


def _mesos_files(inactive, fltr, path):
    """Return MesosFile objects for the specified files.

    :param inactive: whether to include inactive tasks
    :type inactive: bool
    :param fltr: task pattern to match
    :type fltr: str
    :param path: file path to read
    :type path: str
    :returns: MesosFile objects
    :rtype: [MesosFile]
    """

    # get tasks
    master = mesos.get_master()
    tasks = master.tasks(active_only=(not inactive), fltr=fltr)

    # load slave state in parallel
    slaves = _load_slaves_state([task.slave() for task in tasks])

    # create files
    return [mesos.MesosFile(task, path)
            for task in tasks
            if task.slave() in slaves]


def _load_slaves_state(slaves):
    """Fetch each slave's state.json in parallel, and return the reachable
    slaves.

    :param slaves: slaves to fetch
    :type slaves: [MesosSlave]
    :returns: MesosSlave objects that were successfully reached
    :rtype: [MesosSlave]
    """

    reachable_slaves = []

    for job, slave in _stream(lambda slave: slave.state(), slaves):
        try:
            job.result()
            reachable_slaves.append(slave)
        except requests.exceptions.ConnectionError as e:
            emitter.publish(DefaultError(
                'Slave at URL {0} is unreachable: {1}'.
                format(slave.base_url(), e)))

    return reachable_slaves


def _stream_files(curr_header, fn, mesos_files):
    """Apply `fn` in parallel to each file in `mesos_files`.  `fn` must
    return a list of strings, and these strings are then printed
    serially as separate lines.

    `curr_header` is the most recently printed header.  It's used to
    group lines.  Each line has an associated header (e.g. a string
    representation of the MesosFile it was read from), and we only
    print the header before printing a line with a different header
    than the previous line.  This effectively groups lines together
    when the have the same header.

    :param curr_header: Most recently printed header
    :type curr_header: str
    :param fn: function that reads a sequence of lines from a MesosFile
    :type fn: MesosFile -> [str]
    :param mesos_files: files to read
    :type mesos_files: [MesosFile]
    :returns: Returns the most recently printed header, and a list of
        files that are still reachable.  Once we detect a file is
        unreachable, we stop trying to read from it.
    :rtype: (str, [MesosFile])
    """

    reachable_files = list(mesos_files)

    # TODO switch to map
    for job, mesos_file in _stream(fn, mesos_files):
        try:
            lines = job.result()
        except DCOSException as e:
            emitter.publish(DefaultError(
                "Error reading file: {}".format(str(e))))
            reachable_files.remove(mesos_file)
            continue

        curr_header = _output(curr_header, str(mesos_file), lines)

    return curr_header, reachable_files


def _output(curr_header, header, lines):
    """Prints a sequence of lines.  If `header` is different than
    `curr_header`, first print the header.

    :param curr_header: most recently printed header
    :type curr_header: str
    :param header: header for `lines`
    :type header: str
    :param lines: lines to print
    :type lines: [str]
    :returns: `header`
    :rtype: str
    """

    if lines:
        if header != curr_header:
            emitter.publish('===> {} <==='.format(header))
        for line in lines:
            emitter.publish(line)
    return header


def _stream(fn, objs):
    """Apply `fn` to `objs` in parallel, yielding the Future for each as
    it completes.

    :param fn: function
    :type fn: function
    :param objs: objs
    :type objs: objs
    :returns: iterator over (Future, typeof(obj))
    :rtype: iterator over (Future, typeof(obj))
    """

    with concurrent.futures.ThreadPoolExecutor(20) as pool:
        jobs = {pool.submit(fn, obj): obj for obj in objs}
        for job in concurrent.futures.as_completed(jobs):
            yield job, jobs[job]


# A liberal estimate of a line size.  Used to estimate how much data
# we need to fetch from a file when we want to read N lines.
LINE_SIZE = 200


def _read_last_lines(num_lines, mesos_file):
    """Returns the last `num_lines` of a file, or less if the file is
    smaller.  Seeks to EOF.

    :param num_lines: number of lines to read
    :type num_lines: int
    :param mesos_file: file to read
    :type mesos_file: MesosFile
    :returns: lines read
    :rtype: [str]
    """

    file_size = mesos_file.size()

    # estimate how much data we need to fetch to read `num_lines`.
    fetch_size = LINE_SIZE * num_lines

    end = file_size
    start = max(file_size - fetch_size, 0)
    data = ''
    while True:
        # fetch data
        mesos_file.seek(start)
        data = mesos_file.read(end - start) + data

        # break if we have enough lines
        data_tmp = _strip_trailing_newline(data)
        lines = data_tmp.split('\n')
        if len(lines) > num_lines:
            ret = lines[-num_lines:]
            break
        elif start == 0:
            ret = lines
            break

        # otherwise shift our read window and repeat
        end = start
        start = max(file_size - fetch_size, 0)

    mesos_file.seek(file_size)
    return ret


def _read_rest(mesos_file):
    """ Reads the rest of the file, and returns the lines.

    :param mesos_file: file to read
    :type mesos_file: MesosFile
    :returns: lines read
    :rtype: [str]
    """
    data = mesos_file.read()
    if data == '':
        return []
    else:
        data_tmp = _strip_trailing_newline(data)
        return data_tmp.split('\n')


def _strip_trailing_newline(s):
    """Returns a modified version of the string with the last character
    truncated if it's a newline.

    :param s: string to trim
    :type s: str
    :returns: modified string
    :rtype: str
    """

    if s == "":
        return s
    else:
        return s[:-1] if s[-1] == '\n' else s