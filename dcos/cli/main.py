"""
Usage:
    dcos [--log-level=<log-level>] <command> [<args>...]

Options:
    -h, --help                  Show this screen
    --version                   Show version
    --log-level=<log-level>     If set then print supplementary messages to
                                stderr at or above this level. The severity
                                levels in the order of severity are: debug,
                                info, warning, error, and critical. E.g.
                                Setting the option to warning will print
                                warning, error and critical messages to stderr.
                                Note: that this does not affect the output sent
                                to stdout by the command.

Environment Variables:
    DCOS_LOG_LEVEL              If set then it specifies that message should be
                                printed to stderr at or above this level. See
                                the --log-level option for details.

    DCOS_CONFIG                 This environment variable points to the
                                location of the DCOS configuration file.

'dcos help --all' lists available subcommands. See 'dcos <command> --help' to
read about a specific subcommand.
"""


import os
import subprocess

import docopt
from dcos.api import constants, emitting, util

emitter = emitting.FlatEmitter()


def main():
    if not _is_valid_configuration():
        return 1

    args = docopt.docopt(
        __doc__,
        version='dcos version {}'.format(constants.version),
        options_first=True)

    if not _config_log_level_environ(args['--log-level']):
        return 1

    err = util.configure_logger_from_environ()
    if err is not None:
        emitter.publish(err)
        return 1

    command = util.which(
        '{}{}'.format(constants.DCOS_COMMAND_PREFIX, args['<command>']))
    if command is not None:
        argv = [args['<command>']] + args['<args>']
        return subprocess.call([command] + argv)
    else:
        emitter.publish(
            "{!r} is not a dcos command. See 'dcos --help'.".format(
                args['<command>']))
        return 1


def _config_log_level_environ(log_level):
    """
    :param log_level: Log level to set
    :type log_level: str
    :returns: True if the log level was configured correctly; False otherwise.
    :rtype: bool
    """

    if log_level is None:
        os.environ.pop(constants.DCOS_LOG_LEVEL_ENV, None)
        return True

    log_level = log_level.lower()
    if log_level in constants.VALID_LOG_LEVEL_VALUES:
        os.environ[constants.DCOS_LOG_LEVEL_ENV] = log_level
        return True

    msg = 'Log level set to an unknown value {!r}. Valid values are {!r}'
    emitter.publish(msg.format(log_level, constants.VALID_LOG_LEVEL_VALUES))

    return False


def _is_valid_configuration():
    """Validates running environment

    :returns: True if the environment is configure correctly; False otherwise.
    :rtype: bool
    """

    dcos_path = os.environ.get(constants.DCOS_PATH_ENV)
    if dcos_path is None:
        msg = 'Environment variable {!r} not set to the DCOS CLI path.'
        emitter.publish(msg.format(constants.DCOS_PATH_ENV))
        return False

    if not os.path.isdir(dcos_path):
        msg = ('Environment variable {!r} maps to {!r} which is not a '
               'directory.')
        emitter.publish(msg.format(constants.DCOS_PATH_ENV, dcos_path))
        return False

    dcos_config = os.environ.get(constants.DCOS_CONFIG_ENV)
    if dcos_config is None:
        msg = 'Environment variable {!r} must be set to the DCOS config file.'
        emitter.publish(msg.format(constants.DCOS_CONFIG_ENV))
        return False

    if not os.path.isfile(dcos_config):
        msg = 'Environment variable {!r} maps to {!r} and it is not a file.'
        emitter.publish(msg.format(constants.DCOS_CONFIG_ENV, dcos_config))
        return False

    path = os.environ.get(constants.PATH_ENV)
    if path is None:
        msg = 'Environment variable {!r} not set.'
        emitter.publish(msg.format(constants.PATH_ENV))
        return False

    return True
