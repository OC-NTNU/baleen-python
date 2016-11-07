"""
create neo4j graph from variables
"""

import logging
import subprocess
import csv
import json
import time
from path import Path

from lxml import etree

from neo4j.v1 import GraphDatabase, basic_auth
import neokit

from tabulate import tabulate

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
    Transform extracted variables to csv tables that can be imported by neo4j

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
                                  'direction',
                                  ':LABEL'))

    # create csv files for relations
    has_sent_csv = create_csv_file(relation_csv_dir,
                                   'has_sent.csv')
    has_var_csv = create_csv_file(relation_csv_dir,
                                  'has_var.csv')
    has_event_csv = create_csv_file(relation_csv_dir,
                                    'has_event.csv')
    tentails_var_csv = create_csv_file(relation_csv_dir,
                                       'tentails_var.csv',
                                       (':START_ID',
                                        ':END_ID',
                                        'transformName',
                                        ':TYPE'))

    # set of all variable types in text collection
    variable_types = set()

    # mapping from DOI to text files
    doi2txt = _doi2txt_fname(text_dir)

    for json_fname in Path(vars_dir).files('*.json')[:max_n]:
        records = json.load(open(json_fname))

        if not records:
            log.warning('skipping empty variables file: ' + json_fname)
            continue

        log.info('processing variables from file: ' + json_fname)

        doi = get_doi(json_fname)

        try:
            text_fname = doi2txt[doi]
        except KeyError:
            log.error('no matching text file for DOI ' + doi)
            continue

        text = open(text_fname).read()

        # read corenlp analysis
        tree_fname = records[0]['filename']
        scnlp_fname = derive_path(tree_fname, new_dir=scnlp_dir, new_ext='xml')
        xml_tree = etree.parse(scnlp_fname)
        sentences_elem = xml_tree.getroot()[0][0]

        # create article node
        articles_csv.writerow((doi, text_fname, 'Article'))

        tree_number = None

        # mapping of record's "key" to "subStr" attribute,
        # needed for TENTAILS_VAR relation
        key2var = {}

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

            key2var[rec['key']] = var_type = rec['subStr']

            if var_type not in variable_types:
                variables_csv.writerow((var_type,
                                        'VariableType'))
                variable_types.add(var_type)

            # FIXME weak method of detecting preprocessing
            if ('transformName' in rec and
                    not rec['transformName'].startswith('PreProc')):
                # variable is transformed, but not by preprocessing,
                # so it is tentailed
                ancestor_var_type = key2var[rec['ancestor']]
                tentails_var_csv.writerow((ancestor_var_type,
                                           var_type,
                                           rec['transformName'],
                                           'TENTAILS_VAR'))
            else:
                # observed event
                event_id = rec['key']
                event_labels = 'EventInst;' + rec['label'].capitalize() + 'Inst'
                events_csv.writerow((event_id,
                                     tree_fname,
                                     rec['nodeNumber'],
                                     rec['extractName'],
                                     rec['charOffsetBegin'],
                                     rec['charOffsetEnd'],
                                     rec['label'],
                                     event_labels))

                has_event_csv.writerow((sent_id,
                                        event_id,
                                        'HAS_EVENT'))

                has_var_csv.writerow((event_id,
                                      var_type,
                                      'HAS_VAR'))

    # release opened files
    for f in open_files:
        f.close()


def postproc_graph(warehouse_home, server_name, password=None):
    """
    Post-process graph after import.

    Creates ChangeType/IncreaseType/DecreaseType aggregation nodes,
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

    # --------------------------------------------------------------------------
    # Constraints
    # --------------------------------------------------------------------------
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
    CREATE CONSTRAINT ON (v:VariableType)
    ASSERT v.subStr IS UNIQUE
    """)

    session.run("""
    CREATE CONSTRAINT ON (e:EventInst)
    ASSERT e.eventID IS UNIQUE
    """)

    # --------------------------------------------------------------------------
    # Remove TENTAILS_VAR edge duplicates
    # --------------------------------------------------------------------------
    log.info('removing TENTAILS_VAR edge duplicates (if any)')

    session.run("""
    MATCH
        (v1)-[r:TENTAILS_VAR]->(v2)
    WITH
        v1, v2, TAIL (COLLECT (r)) as duplicates
    FOREACH
        (r IN duplicates | DELETE r)
    """)

    # --------------------------------------------------------------------------
    # Removing non-branching tentailed variables nodes
    # --------------------------------------------------------------------------
    # For example, in
    #
    #     (global marine primary production) -[:TENTAILS_VAR]->
    #     (marine primary production) -[:TENTAILS_VAR]->
    #     (primary production) -[:TENTAILS_VAR]->
    #     (production)
    #
    # The two nodes in the middle are deleted, unless they have tentailed
    # relations to other nodes (i.e. more than one -[:TENTAILS_VAR]- relation),
    # or occur in observed events (i.e. have a -[:HAS_VAR]- relation).

    log.info('removing non-branching tentailed variables nodes (if any)')

    session.run("""
    MATCH
        (v:VariableType)
    WITH
         v,
         size(()-[:TENTAILS_VAR]->(v)) as inDegree,
         size((v)-[:TENTAILS_VAR]->()) as outDegree
    WHERE
         inDegree = 1 AND
         outDegree = 1 AND
         NOT (v)<-[:HAS_VAR]-()
    MATCH
         (vIn)-[:TENTAILS_VAR]->(v)-[:TENTAILS_VAR]->(vOut)
    DETACH DELETE
         v
    CREATE
         (vIn)-[:TENTAILS_VAR]->(vOut)
    """)

    # --------------------------------------------------------------------------
    # Create event aggregation nodes
    # --------------------------------------------------------------------------
    log.info('creating event aggregation nodes')

    events = ('Change', 'Increase', 'Decrease')

    # TODO: merge these loops?

    # For each changing/increasing/decreasing VariableType,
    # create a ChangeType/IncreaseType/DecreaseType node and
    # connect them with an HAS_VAR relation.
    # EventType nodes are therefore not unique.
    # The redundant "direction" property is to facilitate matching of
    # IncreaseInst to IncreaseType nodes, DecreaseInst to DecreaseType nodes,
    # etc.
    # Python string formatting is used because labels can not be parametrized
    # in Cypher:
    # http://stackoverflow.com/questions/24274364/in-neo4j-how-to-set-the-label-as-a-parameter-in-a-cypher-query-from-java
    for event in events:
        session.run("""
        MATCH
            (v:VariableType) <-[:HAS_VAR]- (ei:{event}Inst)
        MERGE
            (v) <-[:HAS_VAR]- (:EventType:{event}Type {{direction: ei.direction}})
        """.format(event=event))

    # For each combination of EventType & VariableType,
    # compute the corresponding EventInst count.
    session.run("""
    MATCH
        (et:EventType) -[:HAS_VAR]-> (v:VariableType) <-[:HAS_VAR]- (ei:EventInst)
    WHERE
        et.direction = ei.direction
    WITH
        et, count(*) AS n
    SET
        et.n = n
    """)

    # --------------------------------------------------------------------------
    # Create co-occurrence relations
    # --------------------------------------------------------------------------
    log.info('creating co-occurrence relations')

    # Compute how many times a combination of
    # ChangeType/IncreaseType/DecreaseType & VariableType
    # co-occur in the same sentence.
    # The id(et1) < id(et2) statement prevents counting co-occurence twice
    # (because matching is symmetrical).
    # Store co-occurrence frequency on a new COOCCURS edge.

    session.run("""
    MATCH
        (et1:EventType) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (ei1:EventInst)
        <-[:HAS_EVENT]- (s:Sentence) -[:HAS_EVENT]->
        (ei2:EventInst) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (et2:EventType)
    WHERE
        et1.direction = ei1.direction AND
        et2.direction = ei2.direction AND
        id(et1) < id(et2)
    WITH
        et1, et2, count(*) AS n
    MERGE
        (et1) -[:COOCCURS {n: n}]-> (et2)
        """)

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


def _doi2txt_fname(text_dir):
    """
    Create a dict mapping DOI to path of input text file
    """
    doi2txt = {}

    for p in Path(text_dir).files():
        doi = get_doi(p)
        if doi in doi2txt:
            log.error('DOI {} already mapped to text file {}; '
                      'ignoring text file {}'.format(doi, doi2txt[doi], p))
        else:
            doi2txt[doi] = p

    return doi2txt


def graph_report(warehouse_home, server_name, password, top_n=50):
    """
    Report on graph database
    """
    session = get_session(warehouse_home, server_name, password)

    def tq(query):
        print_table(session.run(query))

    print(time.asctime() + '\n')

    print_section('Database')

    version = (Path(warehouse_home).abspath() / 'run' /
               server_name).dirs('neo4j*')[0].basename()
    print('Neo4j version: ' + version)
    print('Warehouse home: ' + Path(warehouse_home).abspath())
    print('Server name: ' + server_name)
    print('Url: ' + session.driver.url)
    print('Password protected: {}'.format(True if password else False))
    print('Encrypted: {}'.format(session.driver.encrypted))
    db_path = (Path(warehouse_home).abspath() / 'run' /
               server_name / 'neo4j-community-*/data/databases')
    size = subprocess.check_output('du -hs ' + db_path,
                                   shell=True).decode('utf-8').split('\t')[0]
    print('Database size: ' + size)
    print()

    print_section('Nodes')

    tq("""
        MATCH (n)
        WITH labels(n) AS labels
        UNWIND labels AS NodeLabel
        RETURN DISTINCT NodeLabel, count(*) AS Count
        ORDER BY NodeLabel
        """)

    tq("""
        MATCH (n)
        WITH labels(n) AS NodeLabels
        RETURN DISTINCT NodeLabels, count(*) AS Count
        ORDER BY Count
        """)

    total = list(session.run('MATCH (n) RETURN COUNT(*) AS Total'))[0]['Total']
    print('Total number or nodes: {}\n'.format(total))

    print_section('Relations')

    tq("""
        MATCH () -[r]-> ()
        RETURN DISTINCT TYPE(r) AS Relation, COUNT(*) AS Count
        ORDER BY Relation
        """)

    total = list(session.run('MATCH () -[r]- () RETURN COUNT(*) AS Total'))[0][
        'Total']
    print('Total number or relations: {}\n'.format(total))

    print_section('Articles')

    tq("""
        MATCH (a:Article)
        RETURN DISTINCT a.journal as Journal, COUNT(*) as ArticleCount
        ORDER BY ArticleCount DESC
        """)

    tq("""
        MATCH (a:Article)
        RETURN DISTINCT a.year as Year, COUNT(*) as ArticleCount
        ORDER BY Year DESC
        """)

    tq("""
        MATCH (a:Article)
        RETURN DISTINCT a.publisher as Publisher, COUNT(*) as ArticleCount
        ORDER BY Publisher
        """)

    print_section('Events')

    print('Top {} event types:\n'.format(top_n))

    tq("""
        MATCH (et:EventType) -[:HAS_VAR]-> (v:VariableType)
        WITH
            CASE
                WHEN "IncreaseType" IN labels(et) THEN "Increase"
                WHEN "DecreaseType" IN labels(et) THEN "Decrease"
                ELSE "Change"
            END AS Event,
            v.subStr as Variable,
            et.n as Count
        RETURN Event, Variable, Count
        ORDER BY Count DESC
        LIMIT {top_n}
        """.format(top_n=top_n))

    for event in 'Change', 'Increase', 'Decrease':
        print('\nTop {} {} event type:\n'.format(top_n, event))
        tq("""
            MATCH (et:{event}Type) -[:HAS_VAR]-> (v:VariableType)
            WITH
                v.subStr as Variable,
                et.n as Count
            RETURN Variable, Count
            ORDER BY Count DESC
            LIMIT {top_n}
            """.format(event=event, top_n=top_n))

    print_section('Co-occurrence')

    print('Top {} co-occurring event types:\n'.format(top_n))

    tq("""
        MATCH
            (v1:VariableType) <-[:HAS_VAR]- (ve1:EventType)
            -[r:COOCCURS]->
            (ve2:EventType) -[:HAS_VAR]-> (v2:VariableType)
        WITH
            CASE
                WHEN "IncreaseType" IN labels(ve1) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve1) THEN "Decrease"
                ELSE "Change"
            END AS Event1,
            v1.subStr AS Variable1,

            CASE
                WHEN "IncreaseType" IN labels(ve2) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve2) THEN "Decrease"
                ELSE "Change"
            END AS Event2,
            v2.subStr AS Variable2,

            r.n as Count
        RETURN
            Count,
            Event1,
            Variable1,
            Event2,
            Variable2
            ORDER BY Count DESC
            LIMIT {top_n}
        """.format(top_n=top_n))

    print_section('Causal relations')

    print('Top {} causally related event types:\n'.format(top_n))

    tq("""
        MATCH
            (v1:VariableType) <-[:HAS_VAR]- (ve1:EventType)
            -[r:CAUSES]->
            (ve2:EventType) -[:HAS_VAR]-> (v2:VariableType)
        WITH
            CASE
                WHEN "IncreaseType" IN labels(ve1) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve1) THEN "Decrease"
                ELSE "Change"
            END AS Event1,
            v1.subStr AS Variable1,

            CASE
                WHEN "IncreaseType" IN labels(ve2) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve2) THEN "Decrease"
                ELSE "Change"
            END AS Event2,
            v2.subStr AS Variable2,

            r.n as Count
        RETURN
            Count,
            Event1,
            Variable1,
            Event2,
            Variable2
            ORDER BY Count DESC
            LIMIT {top_n}
        """.format(top_n=top_n))


def print_table(result, headers=None):
    if not headers:
        headers = result.keys()
    print(tabulate([r.values() for r in result], headers))
    print()


def print_section(title):
    print(80 * '=')
    print(title)
    print(80 * '=' + '\n')


def add_relations(rels_dir, warehouse_home, server_name, password=None):
    """
    Add extracted causal relations to graph
    """
    add_causation_instances(rels_dir, warehouse_home, server_name, password)
    add_causes_relations(warehouse_home, server_name, password)


def add_causation_instances(rels_dir, warehouse_home, server_name,
                            password=None):
    session = get_session(warehouse_home, server_name, password)

    for rel_fname in Path(rels_dir).files('*.json'):
        log.info('adding CausationInst from file ' + rel_fname)
        for rec in json.load(rel_fname.open()):
            r = session.run("""
            MATCH
                (e1:EventInst)
                <-[:HAS_EVENT]- (s:Sentence) -[:HAS_EVENT]->
                (e2:EventInst)
            WHERE
                e1.eventID = {fromNodeId} AND e2.eventID = {toNodeId}
            WITH e1, e2, s
            MERGE
                (e1) <-[:HAS_CAUSE]-
                (c:CausationInst)
                -[:HAS_EFFECT]-> (e2)
            ON CREATE SET
                c.patternName = [{patternName}]
            ON MATCH SET
                c.patternName = c.patternName + {patternName}
            MERGE
                (s) -[:HAS_EVENT]-> (c)
            """, rec)

    session.close()


def add_causes_relations(warehouse_home, server_name, password=None):
    log.info('adding CAUSES relations')

    session = get_session(warehouse_home, server_name, password)
    session.run("""
        MATCH
            (et1:EventType) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (ei1:EventInst)
            <-[:HAS_CAUSE]- (:CausationInst) -[:HAS_EFFECT]->
            (ei2:EventInst) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (et2:EventType)
        WHERE
            et1.direction = ei1.direction AND
            et2.direction = ei2.direction
        WITH
            et1, et2, count(*) AS n
        MERGE
            (et1) -[:CAUSES {n: n}]-> (et2)
    """)

    session.close()
