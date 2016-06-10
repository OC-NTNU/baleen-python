"""
create neo4j graph from variables
"""

import logging
import subprocess
import csv
import json
from path import Path

from lxml import etree

from neo4j.v1 import GraphDatabase, basic_auth
import neokit

from baleen.utils import derive_path, get_doi

log = logging.getLogger(__name__)

DEFAULT_EDITION = 'community'
DEFAULT_VERSION = 'LATEST'
DEFAULT_USER = 'neo4j'
INITIAL_PASSWORD = 'neo4j'

DEFAULT_HTTP_ADDRESS = 'localhost:7474'
DEFAULT_BOLT_ADDRESS = 'localhost:7687'

# TODO: fix server certificate problem:
# neo4j.v1.exceptions.ProtocolError: Server certificate does not match known
# certificate for 'localhost'; check details in file
# '/Users/work/.neo4j/known_hosts'
DEFAULT_ENCRYPTED = False

# silence info logging from the py2neo and httpstream
DEFAULT_SILENCE_LOGGERS = True


def setup_server(warehouse_home, server_name, edition=DEFAULT_EDITION,
                 version=DEFAULT_VERSION, password=None,
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
    """
    Path(warehouse_home).makedirs_p()
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
        # TODO: support for changing user name
        # It seems that currently the neo4j server API does not support the
        # creation of a new user, so the only thing we can/must do is change
        # the password for the default user 'neo4j'.
        server.update_password(DEFAULT_USER, INITIAL_PASSWORD, password)
        log.info("Password change succeeded")

    warehouse_home = Path(warehouse_home).abspath()
    log.info('This server can be managed using the "neokit" command line '
             'script, e.g.\n$ NEOKIT_HOME={} neokit stop {}'.format(
        warehouse_home,
        server_name))


def remove_server(warehouse_home, server_name):
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


def neo4j_import(warehouse_home, server_name, nodes_dir, relations_dir,
                 options=None):
    """
    Create a new Neo4j database from data in CSV files

    Runs the neo4j-import command line tool and starts the server.

    Parameters
    ----------
    warehouse_home : str
        directory of neokit warehouse containing all neokit server instances
    server_name : str
        name of neokit server instance
    nodes_dir : str
        directory with .cvs filenames for nodes
    relations_dir : str
        directory with .csv filenames for relationships
    options : str
        additional options for neo4j-import

    Returns
    -------
    subprocess.CompletedProcess
        info on completed process

    Notes
    -------
    This will overwrite the existing database of the graph server!

    See http://neo4j.com/docs/stable/import-tool-usage.html
    """
    warehouse = neokit.Warehouse(warehouse_home)
    server = warehouse.get(server_name)
    server.stop()

    log.info('deleting database directory ' + server.store_path)
    server.delete_store()

    executable = Path(server.home) / 'bin' / 'neo4j-import'
    args = [executable, '--into', server.store_path]

    for fname in Path(nodes_dir).files('*.csv'):
        args.append('--nodes')
        args.append(fname.abspath())

    for fname in Path(relations_dir).files('*.csv'):
        args.append('--relationships')
        args.append(fname.abspath())

    if options:
        args += options.split()

    log.info('running subprocess: ' + ' '.join(args))

    completed_proc = subprocess.run(args)

    # restart server after import
    server.start()

    return completed_proc


def vars_to_csv(vars_dir, scnlp_dir, text_dir, nodes_csv_dir,
                relation_csv_dir, max_n=None):
    """
    Transform extracted variables to csv tables that can be imported
    by neo4j

    Parameters
    ----------
    vars_dir : str
        directory containing files with extracted variables in json format
    scnlp_dir : str
        directory containing files with scnlp output in xml format
    text_dir : str
        directory containing files with original text or sentences (one per line)
    bib_dir : str
        directory containing files with BibTex entries (one per file)
    nodes_csv_dir : str
        output directory for nodes csv files
    relation_csv_dir : str
        output directory for relationships csv files
    max_n: int or None
        process max_n variable files

    Notes
    -----
    See http://neo4j.com/docs/stable/import-tool-header-format.html
    """
    # TODO: change article nodes to document
    # TODO: extract the bibtex to separate step
    # hold on to open files
    open_files = []

    def create_csv_file(dir, csv_fname,
                        header=(':START_ID', ':END_ID', ':TYPE')):
        csv_fname = Path(dir) / csv_fname
        log.info('creating ' + csv_fname)
        f = open(csv_fname, 'w', newline='')
        open_files.append(f)
        csv_file = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        csv_file.writerow(header)
        return csv_file

    Path(nodes_csv_dir).makedirs_p()
    Path(relation_csv_dir).makedirs_p()

    # create csv files for nodes
    articles_csv = create_csv_file(nodes_csv_dir,
                                   'articles.csv',
                                   ('doi:ID',
                                    'filename',
                                    ':LABEL'))

    sentences_csv = create_csv_file(nodes_csv_dir,
                                    'sentences.csv',
                                    ('sentID:ID',
                                     'treeNumber:int',
                                     'charOffsetBegin:int',
                                     'charOffsetEnd:int',
                                     'sentChars',
                                     ':LABEL'))

    variables_csv = create_csv_file(nodes_csv_dir,
                                    'variables.csv',
                                    ('subStr:ID',
                                     ':LABEL'))

    events_csv = create_csv_file(nodes_csv_dir,
                                 'events.csv',
                                 ('eventID:ID',
                                  'filename',
                                  'nodeNumber:int',
                                  'extractName',
                                  'charOffsetBegin:int',
                                  'charOffsetEnd:int',
                                  ':LABEL'))

    # create csv files for relations
    has_sent_csv = create_csv_file(relation_csv_dir,
                                   'has_sent.csv')
    theme_csv = create_csv_file(relation_csv_dir,
                                'theme.csv')
    has_event_csv = create_csv_file(relation_csv_dir,
                                    'has_event.csv')

    # global set of all variable types (i.e. subStr) in collection
    variable_types = set()

    for json_fname in Path(vars_dir).files('*.json')[:max_n]:
        records = json.load(open(json_fname))

        if not records:
            log.warning('skipping empty variables file: ' + json_fname)
            continue

        log.info('processing variables from file: ' + json_fname)

        doi = get_doi(json_fname)
        hash_doi = doi.replace('/', '#')

        # read original text
        txt_fnames = (Path(text_dir).files(hash_doi + '.txt') or
                      Path(text_dir).files(hash_doi + '#*.txt'))
        if len(txt_fnames) == 0:
            log.error('no matching text file for ' + hash_doi)
            continue
        elif len(txt_fnames) > 1:
            log.warning('multiple matching text files for {}:\n{}'.format(
                hash_doi, '\n'.join(txt_fnames)))

        text_fname = txt_fnames[0]
        text = open(text_fname).read()

        # read corenlp analysis
        tree_fname = records[0]['filename']
        scnlp_fname = derive_path(tree_fname, new_dir=scnlp_dir, new_ext='xml')
        xml_tree = etree.parse(scnlp_fname)
        sentences_elem = xml_tree.find('.//sentences')

        # create article node
        articles_csv.writerow((doi, text_fname, 'Article'))

        tree_number = None

        for rec in records:
            if rec['treeNumber'] != tree_number:
                # moving to new tree
                tree_number = rec['treeNumber']
                sent_id = '{}/{}'.format(doi, tree_number)
                # get char offsets for sentence (tree numbers start at 1)
                sent_elem = sentences_elem[int(tree_number) - 1]
                begin = int(sent_elem[0][0][2].text)
                end = int(sent_elem[0][-1][3].text)
                sent_chars = text[begin:end]
                sentences_csv.writerow((sent_id,
                                        tree_number,
                                        begin,
                                        end,
                                        sent_chars,
                                        'Sentence'))
                has_sent_csv.writerow((doi,
                                       sent_id,
                                       'HAS_SENT'))

            event_id = rec['key']
            event_labels = 'Event;' + rec['label'].capitalize()
            events_csv.writerow((event_id,
                                 tree_fname,
                                 rec['nodeNumber'],
                                 rec['extractName'],
                                 rec['charOffsetBegin'],
                                 rec['charOffsetEnd'],
                                 event_labels))

            has_event_csv.writerow((sent_id,
                                    event_id,
                                    'HAS_EVENT'))

            var_type = rec['subStr']
            if var_type not in variable_types:
                variables_csv.writerow((var_type,
                                        'Variable'))
                variable_types.add(var_type)

            theme_csv.writerow((event_id,
                                var_type,
                                'THEME'))

    # release opened files
    for f in open_files:
        f.close()


def postproc_graph(warehouse_home, server_name, password=None):
    """
    Post-process graph after import.

    Creates VarChange/VarIncrease/VarDecrease aggregation nodes,
    creates co-occurrence relations and
    imposes constraints and indices.

    Parameters
    ----------
    warehouse_home : str
        directory of neokit warehouse containing all neokit server instances
    server_name : str
        name of neokit server instance
    password : str
    silence_loggers : bool
        silence info logging from the py2neo and httpstream
    """
    session = get_session(warehouse_home, server_name, password)

    # -----------------------------------------------------------------------------
    # Constraints
    # -----------------------------------------------------------------------------
    log.info('creating constraints')

    # Create a unique property constraint on the label and property combination.
    # If any other node with that label is updated or created with a property
    # that already exists, the write operation will fail.
    # This constraint will create an accompanying index.
    # See http://neo4j.com/docs/stable/query-constraints.html

    session.run("""
    CREATE CONSTRAINT ON (a:Article)
    ASSERT a.doi IS UNIQUE
    """)

    session.run("""
    CREATE CONSTRAINT ON (s:Sentence)
    ASSERT s.sentID IS UNIQUE
    """)

    session.run("""
    CREATE CONSTRAINT ON (v:Variable)
    ASSERT v.subStr IS UNIQUE
    """)

    # -----------------------------------------------------------------------------
    # Create event aggregation nodes
    # -----------------------------------------------------------------------------
    log.info('creating event aggregation nodes')

    events = ('Change', 'Increase', 'Decrease')

    # TODO: merge these loops?

    # For each changing/increasing/decreasing Variable,
    # create a VarChange/VarIncrease/VarDecrease node and
    # connect tthem with a VAR relation.
    for event in events:
        session.run("""
        MATCH (v:Variable) <-[:THEME]- (:{event})
        MERGE (v) <-[:VAR]- (ve:Var{event}:VarEvent {{subStr: v.subStr}})
        """.format(event=event))

    # Next connect each Change/Increase/Decrease event to the
    # VarChange/VarIncrease/VarDecrease node of the event's variable,
    # using an INST relation.
    for event in events:
        session.run("""
        MATCH (ve:Var{event}) -[:VAR]-> (:Variable) <-[:THEME]- (e:{event})
        MERGE (ve) -[:INST]-> (e)
        """.format(event=event))

    # Compute the out-degree of the VarChange/VarIncrease/VarDecrease nodes
    # on the INST relation, where n represents the number of
    # changing/increasing/decreasing events
    for event in events:
        session.run("""
        MATCH (ve:Var{event}) -[:INST]-> (:{event})
        WITH ve, count(*) AS n
        SET ve.n = n
        """.format(event=event))

    # impose more constraints
    session.run("""
    CREATE CONSTRAINT ON (ve:VarChange)
    ASSERT ve.subStr IS UNIQUE
    """)

    session.run("""
    CREATE CONSTRAINT ON (ve:VarIncrease)
    ASSERT ve.subStr IS UNIQUE
    """)

    session.run("""
    CREATE CONSTRAINT ON (ve:VarDecrease)
    ASSERT ve.subStr IS UNIQUE
    """)

    # create index on VarEvent (where subStr is not unique)
    session.run("CREATE INDEX ON :VarEvent(subStr)")

    # -----------------------------------------------------------------------------
    # Create co-occurrence relations
    # -----------------------------------------------------------------------------
    log.info('creating co-occurrence relations')

    # Compute how many times a pair of VarChange/VarIncrease/VarDecrease
    # co-occur in the same sentence.
    # The id(ve1) < id(ve2) statements prevents counting co-occurence twice
    # (because matching is symmetrical).
    # Store co-occurrence frequency on a new COOCCURS edge.
    for event1 in events:
        for event2 in events:
            session.run("""
                MATCH (ve1:Var{event1}) -[:INST]-> (:{event1}) <-[:HAS_EVENT]- (s:Sentence) -[:HAS_EVENT]-> (:{event2}) <-[:INST]- (ve2:Var{event2})
                WHERE id(ve1) < id(ve2)
                WITH ve1, ve2, count(*) AS n
                MERGE (ve1) -[:COOCCURS {{n: n}}]-> (ve2)
            """.format(event1=event1, event2=event2))

    session.close()


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

    session = driver.session()
    return session
