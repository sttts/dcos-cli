from common import assert_command

def test_help():
    stdout = b"""Output the last part of files in a task's sandbox

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
    assert_command(['dcos', 'tail', '--help'], stdout=stdout)


def test_info():
    stdout = b"Output the last part of files in a task's sandbox\n"
    assert_command(['dcos', 'tail', '--info'], stdout=stdout)

# tail a single file
# tail a single file with --lines
# tail a file on two slaves
