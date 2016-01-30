"""
support for command line scripts with Argh and ConfigParser
"""

"""
# python usage sample

from argh import dispatch, arg
from nature.config import setup, add_commands


DEFAULT_CONFIG_FILENAME = 'config.ini'
DEFAULT_SECTION = 'DEFAULT'

arg_parser, config, section, left_args = setup(DEFAULT_CONFIG_FILENAME,
                                               DEFAULT_SECTION)


def command1(arg11, kw11=None, kw12=None):
    pass

# specify types
@arg("--kw21", type=int)
@arg("--kw22", action='store_true')
def command2(arg21, kw21=None, kw22=None, kw23='untouched'):
    pass


functions = [command1, command2]

add_commands(arg_parser, functions, config, section)

dispatch(arg_parser, argv=left_args)
"""

"""
# config.ini sample

[DEFAULT]
arg11 = 'a'
kw11 = 'x'
kw12 = 'y'

arg21 = 'b'
kw21 = 1
kw22 = true
"""

import argh
import configparser
import logging

log = logging.getLogger(__name__)


def setup(default_cfg_fname, default_section='DEFAULT'):
    """
    Setup Argh with defaults from config file

    Parameters
    ----------
    default_cfg_fname : str
        name of default configuration filename;
        overridden if -c/--config option is set
    default_section : str
        name of default section in configuration;
        overridden if -s/--section option is set

    Returns
    -------
    arg_parser : ArgParser
        argument parser
    config : ConfigParser
        configuration with default settings for parameters/options
        as read from config file
    section : str
        section in configuration
    left_args : list
        all remaining arguments (except -c and -s)
    """
    # Do not add help, because any "-h" or "--help" option must be handled
    # by argh instead of here.
    arg_parser = argh.ArghParser(add_help=False)

    # Add the option for one or more config files, including a default.
    # It is not possible to use nargs="+", because then argparse consumes
    # *all* arguments, leaving nothing to dispatch to argh.
    arg_parser.add_argument(
            '-c',
            '--config',
            action='append',
            metavar='CONFIG_FILE',
            default=[default_cfg_fname],
            help='configuration file; option can be repeated '
                 'where later configurations override earlier ones')

    arg_parser.add_argument(
            '-s',
            '--section',
            default=default_section,
            help='section in config file')

    # Parse the --config and --section options (if any),
    # leaving all others to for argh
    namespace, left_args = arg_parser.parse_known_args()

    # Get config filenames and section
    config_fnames = namespace.config
    # If -c option is used, then the default config file (first) is ignored!
    if len(config_fnames) > 1:
        config_fnames = config_fnames[1:]
    section = namespace.section

    # Now add the standard help option, to be handled/displayed by argh
    arg_parser.add_argument(
            '-h', '--help',
            action='help',
            help='show this help message and exit')

    # read config files
    config = configparser.ConfigParser()
    read_ok = config.read(config_fnames)

    # Non-existing config files are silently ignored by ConfigParser,
    # but we want an error message
    for fname in config_fnames:
        if fname not in read_ok:
            arg_parser.error(
                    "config file {!r} not found".format(fname))

    return arg_parser, config, section, left_args


def add_commands(arg_parser, functions, config, section='DEFAULT',
                 prefix=False, **kwargs):
    """
    Adds given functions as commands to given parser and also
    sets default values from given config.

    Parameters
    ----------
    arg_parser : ArghParser or ArgumentParser
        argument parser
    functions : list
        list of functions
    config : ConfigParser
        configuration, as read from config file(s)
    section : str
        section in configuration
    prefix: bool
        prefix argument with function name to create unique namespace

    Notes
    -----
    Current version fails with variable number of paramters (*args)
     or keyword arguments (**kwargs)
    """
    _inject_defaults(functions, config, section, prefix)
    argh.add_commands(arg_parser, functions, **kwargs)


def _inject_defaults(functions, config, section, use_namespace):
    """
    inject default values from config into functions,
    as if a @arg(default='value') decorator was applied to each function
    """
    # TODO: handle *args and **kwargs
    for func in functions:
        #log.info(func.__name__)
        # get the argh_args list for this function,
        # as used by @arg decorators to store their settings,
        # otherwise create a new one
        try:
            argh_args = func.argh_args
        except AttributeError:
            argh_args = []
            setattr(func, 'argh_args', argh_args)

        # map from option name to corresponding argument dict in argh_args list
        argh_map = {arg['option_strings'][-1]: arg
                    for arg in argh_args}
        #log.info(argh_args)
        #log.info(argh_map)

        # get function arguments through introspection
        for func_arg in argh.assembling._get_args_from_signature(func):
            #log.info(func_arg)
            opt_str = func_arg['option_strings']
            # option name is last elem in case of short options
            # e.g. {'option_strings': ('-m', '--max-n-records'), ...}
            opt_name = opt_str[-1]
            config_name = _get_config_name(opt_name, func, use_namespace)

            # if option's value is defined in the config,
            # then inject a default value in argh's argument dict
            if _option_has_value(config, section, config_name):
                # get argh's argument dict for this option,
                # otherwise create a new one
                try:
                    argh_arg = argh_map[opt_name]
                except KeyError:
                    argh_arg = {'option_strings': opt_str}
                    argh_args.append(argh_arg)

                argh_arg['default'] = _get_config_value(config, section,
                                                        config_name,
                                                        func_arg, argh_arg)
                #log.info('  {} = {!r}'.format(config_name, argh_arg['default']))

                # FIXME: optional argument hack
                # manditory arguments can not have a default,
                # so we pretend that each argument is an optional argument
                if not opt_name.startswith('-'):
                    argh_arg['nargs'] = '?'


def _get_config_name(opt_name, func, use_namespace):
    """
    translate function option name to config option name
    """
    prefix = func.__name__ + '.' if use_namespace else ''
    # e.g. --terms-n ==> TERMS_N
    opt_name = opt_name.lstrip('-').replace('-', '_')
    return prefix + opt_name


def _get_config_value(config, section, config_name, func_arg, argh_arg):
    """
    get config value of appropriate type
    """
    # type and action if defined through @arg decorator,
    # e.g. @arg("--number", type=int)
    given_type = argh_arg.get('type')
    action = argh_arg.get('action')
    # type from default value in function signature
    # e.g. func(number=10)
    default_type = type(func_arg.get('default'))

    if (given_type is int or default_type is int):
        # value must be an int
        config_value = config.getint(section, config_name)
    elif (given_type is bool  or
                  default_type is bool or
                  action in ('store_true', 'store_false')):
        # value must be True or False
        config_value = config.getboolean(section, config_name)
    else:
        # value is ordinary strong
        config_value = config.get(section, config_name)

    return config_value


def _option_has_value(config, section, name):
    """
    check if option exists and has a value
    """
    # does the name is exists?
    try:
        value = config.get(section, name)
    except configparser.NoOptionError:
        return False

    # does it have a value
    if value == '':
        return False

    return True


def docstring(from_func, first_line_only=True):
    """
    decorator to copy docstring from given function to decorated function
    """
    def wrapper(to_func):
        if first_line_only:
            to_func.__doc__ = from_func.__doc__.strip().split("\n")[0]
        else:
            to_func.__doc__ = from_func.__doc__
        return to_func
    return wrapper