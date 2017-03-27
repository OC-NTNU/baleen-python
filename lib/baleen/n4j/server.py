"""
Manage Neo4j server
"""

import logging
from pathlib import Path

import neokit
from neo4j.v1 import GraphDatabase, basic_auth

log = logging.getLogger(__name__)

DEFAULT_EDITION = 'community'
DEFAULT_VERSION = 'LATEST'
DEFAULT_USER = 'neo4j'
INITIAL_PASSWORD = 'neo4j'
DEFAULT_HTTP_ADDRESS = 'localhost:7474'
DEFAULT_BOLT_ADDRESS = 'localhost:7687'
DEFAULT_ENCRYPTED = False
# silence info logging from the py2neo and httpstream
DEFAULT_SILENCE_LOGGERS = True


# TODO 2: fix server certificate problem:
# neo4j.v1.exceptions.ProtocolError: Server certificate does not match known
# certificate for 'localhost'; check details in file '/Users/work/.neo4j/known_hosts'

def setup_server(warehouse_home, server_name,
                 edition=DEFAULT_EDITION,
                 version=DEFAULT_VERSION,
                 password=None,
                 http_address=DEFAULT_HTTP_ADDRESS,
                 bolt_address=DEFAULT_BOLT_ADDRESS):
    """
    Setup and run a new neo4j server

    Parameters
    ----------
    warehouse_home : str
        directory of neokit warehouse containing all neokit server instances
    server_name : str
        name of neokit server instance
    edition : str
         Neo4j edition ('community' or 'enterprise')
    version : str
        Neo4j version (e.g. '2.1.5' or 'LATEST')
    password: str or None
        If password is None, authorization will be disabled.
    http_address: str
    bolt_address: str
    """
    Path(warehouse_home).mkdir(parents=True, exist_ok=True)
    warehouse = neokit.Warehouse(warehouse_home)

    try:
        if warehouse.get(server_name):
            log.error(
                'server instance {!r} already exists!'.format(server_name))
            return
    except (FileNotFoundError, IOError):
        pass

    warehouse.install(server_name, edition, version)
    server = warehouse.get(server_name)

    if not password:
        server.auth_enabled = False

    if http_address:
        server.set_config('dbms.connector.http.address', http_address)

    if bolt_address:
        server.set_config('dbms.connector.bolt.address', bolt_address)

    log.info('Created server instance {!r} in {} configured on '
             'http://{} and bolt://{}'.format(server_name, server.home,
                                              http_address, bolt_address))
    # start server
    start_server(warehouse_home, server_name)

    if password:
        # TODO 3: support for changing user name
        # It seems that currently the neo4j server API does not support the
        # creation of a new user, so the only thing we can/must do is change
        # the password for the default user 'neo4j'.
        server.update_password(DEFAULT_USER, INITIAL_PASSWORD, password)
        log.info("Password change succeeded")

    warehouse_home = Path(warehouse_home).resolve()
    log.info('This server can be managed using the "neokit" command line '
             'script, e.g.\n$ NEOKIT_HOME={} neokit stop {}'.format(warehouse_home, server_name))


def remove_server(warehouse_home, server_name):
    """remove neo4j server"""
    warehouse = neokit.Warehouse(warehouse_home)
    server = warehouse.get(server_name)
    if server.running():
        stop_server(warehouse_home, server_name)
    warehouse.uninstall(server_name)
    log.info('removed server {!r}'.format(server_name))


def start_server(warehouse_home, server_name):
    """start neo4j server"""
    server = neokit.Warehouse(warehouse_home).get(server_name)
    pid = server.start()
    log.info('Started server {!r} at {} (pid={})'.format(server_name,
                                                         server.http_uri, pid))


def stop_server(warehouse_home, server_name):
    """stop neo4j server"""
    server = neokit.Warehouse(warehouse_home).get(server_name)
    server.stop()
    log.info('Stopped server {!r} at {}'.format(server_name,
                                                server.http_uri))


def get_session(warehouse_home, server_name, password=None,
                encrypted=DEFAULT_ENCRYPTED,
                silence_loggers=DEFAULT_SILENCE_LOGGERS):
    if silence_loggers:
        logging.getLogger('neo4j.bolt').setLevel(logging.WARNING)

    server = neokit.Warehouse(warehouse_home).get(server_name)
    address = server.config('dbms.connector.bolt.address', 'localhost:7687')
    server_url = 'bolt://' + address

    if password:
        driver = GraphDatabase.driver(server_url, encrypted=encrypted,
                                      auth=basic_auth(DEFAULT_USER, password))
    else:
        driver = GraphDatabase.driver(server_url, encrypted=encrypted)

    with driver.session() as session:
        return session
