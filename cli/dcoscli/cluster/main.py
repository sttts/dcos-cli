"""Provides status about the DCOS cluster managed by this CLI

Usage:
   dcos cluster --help
   dcos cluster --info
   dcos cluster --version
   dcos cluster help
   dcos cluster status [--json]

Options:
    --help                  Show this screen
    --info                  Show info
    --json                  Print json-formatted cluster status
    --version               Show version
"""
import dcoscli
import docopt
from dcos import cmds, emitting, util
from dcos.errors import DCOSException
from dcos.http import request
from dcoscli import tables

logger = util.get_logger(__name__)

emitter = emitting.FlatEmitter()

MESOS_STATE_URL = '{}:5050/state.json'
OK_STATUS = 200


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
        version='dcos-status version {}'.format(dcoscli.version))

    return cmds.execute(_cmds(), args)


def _cmds():
    """
    :returns: All of the supported commands
    :rtype: [Command]
    """

    return [
        cmds.Command(
            hierarchy=['cluster', '--info'],
            arg_keys=[],
            function=_info),

        cmds.Command(
            hierarchy=['cluster', 'status'],
            arg_keys=['--json'],
            function=_status),
        ]


def _status(is_json):
    """ Print to cli cluster status

    :param: args: dcos cluster input arguments
    type: args: dict
    """
    cluster_data, exit_code = get_cluster_data()
    emitting.publish_table(emitter, cluster_data['Components'],
                           tables.cluster_table, is_json)
    if exit_code != 0:
        raise DCOSException()


def _info():
    """Print task cli information.

    :returns: process return code
    :rtype: int
    """

    emitter.publish(__doc__.split('\n')[0])
    return 0


def get_cluster_data():
    """ Get information about current cli cluster

    :returns: Cluster status data
    :rtype: dict
    """
    exit_code_list = list()

    def sum_codes(f):
        status, exit_code = f()
        exit_code_list.append(exit_code)
        return status

    return {'Components': [{'Name': 'Mesos Master',
                            'Status': sum_codes(check_master_status)},
                           {'Name': 'Mesos Marathon framework',
                            'Status': sum_codes(check_marathon_task_status)},
                           {'Name': 'Mesos active Slaves count',
                            'Status': sum_codes(get_active_slaves_number)},
                           {'Name': 'Marathon',
                            'Status': sum_codes(check_marathon_status)},
                           {'Name': 'DCOS UI',
                            'Status': sum_codes(check_ui_status)},
                           {'Name': 'Exhibitor',
                            'Status': sum_codes(check_exhibitor_status)
                            }]}, sum(exit_code_list)


def get_dcos_url():
    """Return a Mesos master client URL, using the URLs stored in the user's
    configuration.

    :returns: mesos master url
    :rtype: str
    """
    config = util.get_config()
    dcos_url = util.get_config_vals(config, ['core.dcos_url'])[0]
    return dcos_url


def check_master_status():
    """Check Mesos Master status on current cluster

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """

    return make_request(MESOS_STATE_URL.format(get_dcos_url()))


def check_marathon_task_status():
    """Check task with name "marathon" exist on current cluster

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """
    try:
        response = request("GET", MESOS_STATE_URL.format(get_dcos_url()))
        frameworks = response.json()['frameworks']
        if any(f['name'] == 'marathon' for f in frameworks):
            return 'OK', 0
        else:
            return 'Marathon framework is not registered.', 1
    except DCOSException:
        return 'Error. Unable to get Marathon framework status.', 1


def get_active_slaves_number():
    """Get all active Mesos slaves number on current cluster

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """

    try:
        response = request("GET", MESOS_STATE_URL.format(get_dcos_url()))
        return response.json()['activated_slaves'], 0
    except DCOSException:
        return 'Error. Unable to get Mesos slaves count.', 1


def check_marathon_status():
    """Check Marathon status on current cluster

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """

    return make_request('{}:8080'.format(get_dcos_url()))


def check_ui_status():
    """Check DCOS UI status on current cluster

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """

    return make_request('{}:80'.format(get_dcos_url()))


def check_exhibitor_status():
    """Check Exhibitor status on current cluster

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """

    return make_request('{}:8181/exhibitor/v1/cluster/'
                        'status'.format(get_dcos_url()))


def make_request(url):
    """Execute request to DCOS components

    :returns: tuple, which contain status and exit code
    :rtype (str, int)
    """
    try:
        request("GET", url)
        return "OK", 0
    except DCOSException:
        return "Error", 1
