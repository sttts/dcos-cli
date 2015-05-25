import collections
import contextlib
import json
import subprocess
import sys

import six
from dcos import util

import mock


def exec_command(cmd, env=None, stdin=None):
    """Execute CLI command

    :param cmd: Program and arguments
    :type cmd: list of str
    :param env: Environment variables
    :type env: dict of str to str
    :param stdin: File to use for stdin
    :type stdin: file
    :returns: A tuple with the returncode, stdout and stderr
    :rtype: (int, bytes, bytes)
    """

    print('CMD: {!r}'.format(cmd))

    process = subprocess.Popen(
        cmd,
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env)

    stdout, stderr = process.communicate()

    # We should always print the stdout and stderr
    print('STDOUT: {}'.format(stdout.decode('utf-8')))
    print('STDERR: {}'.format(stderr.decode('utf-8')))

    return (process.returncode, stdout, stderr)


def assert_command(
        cmd,
        returncode=0,
        stdout=b'',
        stderr=b'',
        env=None,
        stdin=None):
    """Execute CLI command and assert expected behavior.

    :param cmd: Program and arguments
    :type cmd: list of str
    :param returncode: Expected return code
    :type returncode: int
    :param stdout: Expected stdout
    :type stdout: str
    :param stderr: Expected stderr
    :type stderr: str
    :param env: Environment variables
    :type env: dict of str to str
    :param stdin: File to use for stdin
    :type stdin: file
    :rtype: None
    """

    returncode_, stdout_, stderr_ = exec_command(cmd, env, stdin)

    assert returncode_ == returncode
    assert stdout_ == stdout
    assert stderr_ == stderr


def exec_mock(main, args):
    """Call a main function with sys.args mocked, and capture
    stdout/stderr

    :param main: main function to call
    :type main: function
    :param args: sys.args to mock, excluding the initial 'dcos'
    :type args: [str]
    :returns: (returncode, stdout, stderr)
    :rtype: (int, bytes, bytes)
    """

    print('MOCK ARGS: {}'.format(' '.join(args)))

    with mock_args(args) as (stdout, stderr):
        returncode = main()

    stdout_val = six.b(stdout.getvalue())
    stderr_val = six.b(stderr.getvalue())

    print('STDOUT: {}'.format(stdout_val))
    print('STDERR: {}'.format(stderr_val))

    return (returncode, stdout_val, stderr_val)


def assert_mock(main, args,
                returncode=0,
                stdout=b'',
                stderr=b''):
    """Mock and call a main function, and assert expected behavior.

    :param main: main function to call
    :type main: function
    :param args: sys.args to mock, excluding the initial 'dcos'
    :type args: [str]
    :type returncode: int
    :param stdout: Expected stdout
    :type stdout: str
    :param stderr: Expected stderr
    :type stderr: str
    :rtype: None
    """

    returncode_, stdout_, stderr_ = exec_mock(main, args)

    assert returncode_ == returncode
    assert stdout_ == stdout
    assert stderr_ == stderr


def mock_called_some_args(mock, *args, **kwargs):
    """Convience method for some mock assertions.  Returns True if the
    arguments to one of the calls of `mock` contains `args` and
    `kwargs`.

    :param mock: the mock to check
    :type mock: mock.Mock
    :returns: True if the arguments to one of the calls for `mock`
    contains `args` and `kwargs`.
    :rtype: bool
    """

    for call in mock.call_args_list:
        call_args, call_kwargs = call

        if any(arg not in call_args for arg in args):
            continue

        if any(k not in call_kwargs or call_kwargs[k] != v
               for k, v in kwargs.items()):
            continue

        return True

    return False


def watch_deployment(deployment_id, count):
    """ Wait for a deployment to complete.

    :param deployment_id: deployment id
    :type deployment_id: str
    :param count: max number of seconds to wait
    :type count: int
    :rtype: None
    """

    returncode, stdout, stderr = exec_command(
        ['dcos', 'marathon', 'deployment', 'watch',
            '--max-count={}'.format(count), deployment_id])

    assert returncode == 0
    assert stderr == b''


def watch_all_deployments(count=60):
    """ Wait for all deployments to complete.

    :param count: max number of seconds to wait
    :type count: int
    :rtype: None
    """

    deps = list_deployments()
    for dep in deps:
        watch_deployment(dep['id'], count)


def deploy_app(app_path):
    """ Add an app, and wait for it to deploy

    :param app_path: path to app's json definition
    :type app_path: str
    :rtype: None
    """

    assert_command(['dcos', 'marathon', 'app', 'add', app_path])
    watch_all_deployments()


def remove_app(app_id):
    """ Remove an app

    :param app_id: id of app to remove
    :type app_id: str
    :rtype: None
    """

    assert_command(['dcos', 'marathon', 'app', 'remove', app_id])


def get_services(expected_count=None, args=[]):
    """Get services

    :param expected_count: assert exactly this number of services are
        running
    :type expected_count: int
    :param args: cli arguments
    :type args: [str]
    :returns: services
    :rtype: [dict]
    """

    returncode, stdout, stderr = exec_command(
        ['dcos', 'services', '--json'] + args)

    assert returncode == 0
    assert stderr == b''

    services = json.loads(stdout.decode('utf-8'))
    assert isinstance(services, collections.Sequence)
    if expected_count is not None:
        assert len(services) == expected_count

    return services


def list_deployments(expected_count=None, app_id=None):
    """Get all active deployments.

    :param expected_count: assert that number of active deployments
    equals `expected_count`
    :type expected_count: int
    :param app_id: only get deployments for this app
    :type app_id: str
    :returns: active deployments
    :rtype: [dict]
    """

    cmd = ['dcos', 'marathon', 'deployment', 'list']
    if app_id is not None:
        cmd.append(app_id)

    returncode, stdout, stderr = exec_command(cmd)

    result = json.loads(stdout.decode('utf-8'))

    assert returncode == 0
    if expected_count is not None:
        assert len(result) == expected_count
    assert stderr == b''

    return result


@contextlib.contextmanager
def app(path, app_id):
    """Context manager that deploys an app on entrance, and removes it on
    exit.

    :param path: path to app's json definition:
    :type path: str
    :param app_id: app id
    :type app_id: str
    :rtype: None
    """

    deploy_app(path)
    try:
        yield
    finally:
        remove_app(app_id)


@contextlib.contextmanager
def mock_args(args):
    """ Context manager that mocks sys.args and captures stdout/stderr

    :param args: sys.args values to mock
    :type args: [str]
    :rtype: None
    """
    with mock.patch('sys.argv', [util.which('dcos')] + args):
        stdout, stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = six.StringIO(), six.StringIO()
        try:
            yield sys.stdout, sys.stderr
        finally:
            sys.stdout, sys.stderr = stdout, stderr
