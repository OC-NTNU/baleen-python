"""
processing pipeline
"""

from logging import basicConfig

from argh import dispatch

from baleen.arghconfig import setup, add_commands, run_commands


def script(steps, optional,
           default_cfg_fnames=[], default_section='DEFAULT'):
    """
    run a processing pipeline as command line script
    """
    arg_parser, config, section, left_args = setup(default_cfg_fnames,
                                                   default_section)

    basicConfig(level=config.get(section, "LOG_LEVEL", fallback="WARNING"))

    def run_all():
        run_commands(steps)

    run_all.__doc__ = "Run complete  pipeline: {}".format(
        " --> ".join(step.__name__.replace('_', '-') for step in steps))

    functions = steps + optional + [run_all]

    add_commands(arg_parser, functions, config, section, prefix=True)

    dispatch(arg_parser, argv=left_args)
