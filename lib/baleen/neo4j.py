"""
create neo4j graph from variables
"""

import logging
import glob
import subprocess
import csv
import json
import os.path as path

from lxml import etree

from py2neo.ext import neobox
from py2neo import password, authenticate

from baleen import bibtex
from baleen import utils


log = logging.getLogger(__name__)


def setup_neo4j_box(neobox_home, box_name, edition, version, user_password):
    """
    Setup and run a new neo4j server

    Parameters
    ----------
    neobox_home : str
        directory of neobox warehouse containing all neobox server instances
    box_name : str
        name of neobox server instance
    edition : str
         Neo4j edition ('community' or 'enterprise')
    version : str
        Neo4j version (e.g. '2.1.5')
    password : str
        new password for default user 'neo4'
    """
    # Create neo4j box
    # see py2neo/ext/neobox/__main__.py
    warehouse = neobox.Warehouse(neobox_home)
    box = warehouse.box(box_name)
    box.install(edition, version)
    port = warehouse._ports[box_name]
    https_port = port + 1
    log.info('Created server instance {} in warehouse {} configured on '
             'ports {} and {}'.format(box_name, box.home, port,
                                      https_port))
    # start server
    ps = box.server.start()
    box_uri = ps.service_root.uri
    log.info('Started server at {}'.format(box_uri))

    # set password
    # TODO: change user name
    # It seems that currently the neo4j server API does not support the
    # creation of a new user, so the only thing we can/must do is change
    # the default password for the user 'neo4j'.
    default_user_name = default_password = 'neo4j'
    service_root = password.ServiceRoot(box_uri)
    user_manager = password.UserManager.for_user(service_root,
                                                 default_user_name,
                                                 default_password)
    password_manager = user_manager.password_manager
    if password_manager.change(user_password):
        log.info("Password change succeeded")
    else:
        log.error("Password change failed")

    # stop server
    #box.server.stop()
    #log.info('Server stopped')
    log.info(
            'This server can be managed using the "neobox" command line script, '
            'e.g.\n$ NEOBOX_HOME={} neobox stop {}'.format(neobox_home,
                                                            box_name))


def neo4j_import(neobox_home, box_name, nodes_dir, relations_dir, options=None):
    """
    Create a new Neo4j database from data in CSV files

    Runs the neo4j-import command line tool and starts the server.

    Parameters
    ----------
    neobox_home : str
        directory of neobox warehouse containing all neobox server instances
    box_name : str
        name of neobox server instance
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
    warehouse = neobox.Warehouse(neobox_home)
    box = warehouse.box(box_name)
    server = box.server

    # store.drop will raise RuntimeError if server is running
    log.info('deleting database directory ' + server.store.path)
    server.store.drop()

    # server must be down for import
    server.stop()

    bin_dir = path.dirname(server.script)
    executable = path.join(bin_dir, 'neo4j-import')
    args = [executable, '--into', server.store.path]

    for fname in utils.file_list(nodes_dir, '*.csv'):
        args.append('--nodes')
        args.append(fname)

    for fname in utils.file_list(relations_dir, '*.csv'):
        args.append('--relationships')
        args.append(fname)

    if options:
        args += options.split()

    log.info('running subprocess: ' + ' '.join(args))

    completed_proc = subprocess.run(args)

    # restart server after import
    server.start()

    return completed_proc


def vars_to_csv(vars_fname, scnlp_dir, sent_dir, bib_dir, nodes_csv_dir,
                relation_csv_dir, max_n_vars=None):
    """
    Transform extracted variables to csv tables that can be imported
    by neo4j

    Parameters
    ----------
    vars_fname : str
        file with extracted variables in json format
    scnlp_dir : str
        directory containing scnlp output in xml format
    sent_dir : str
        directory containing sentences (one per line)
    bib_dir : str
        directory containing BibTex entries (one per file)
    nodes_csv_dir : str
        output directory for nodes csv files
    relation_csv_dir : str
        output directory for relationships csv files

    Notes
    -----
    See http://neo4j.com/docs/stable/import-tool-header-format.html
    """

    def create_csv_file(dir, fname, header=(':START_ID', ':END_ID', ':TYPE')):
        fname = path.join(dir, fname)
        log.info('creating ' + fname)
        file = open(fname, 'w', newline='')
        csv_file = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        csv_file.writerow(header)
        return csv_file

    utils.make_dir(nodes_csv_dir)
    utils.make_dir(relation_csv_dir)

    # create csv files for nodes
    articles_csv = create_csv_file(nodes_csv_dir,
                                   'articles.csv',
                                   ('doi:ID',
                                    'filename',
                                    'author',
                                    'year',
                                    'title',
                                    'journal',
                                    'volume',
                                    'number',
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

    filename = None
    tree_number = None
    variables = set()

    for rec in json.load(open(vars_fname))[:max_n_vars]:
        # TODO: this assumes vars are ordered per filename,
        # which may not be true for pruned variables
        if rec['filename'] != filename:
            # moving to new file
            filename = rec['filename']
            log.info('processing variables from file ' + filename)
            tree_number = None
            # get SCNLP analyses for sentences
            scnlp_fname = path.join(scnlp_dir,
                                    path.splitext(filename)[0] + '.xml')
            xml_tree = etree.parse(scnlp_fname)
            sentences_elem = xml_tree.find('.//sentences')
            # get sentences
            # Input to SCNLP may be tokenized with one sentence per line (abs),
            # e.g. 10.1038#ismej.2011.152#abs#sent#scnlp_v3.5.1.parse"
            # or raw text (full),
            # e.g. 10.1038#490352a#full#scnlp_v3.5.1.parse
            prefix = '#'.join(filename.split('#')[:3])
            # e.g. 10.1038#ismej.2011.152#abs*.txt
            glob_pat = path.join(sent_dir, prefix + '*.txt')
            matches = glob.glob(glob_pat)
            assert len(matches) == 1
            sent_fname = matches[0]
            ##sent_fname = ''.join(filename.partition('#sent')[:2]) + '.txt'
            ##sent_fname = path.join(sent_dir, sent_fname)
            sent_text = open(sent_fname).read()
            # get bibtex entry
            hash_doi = '#'.join(rec['filename'].split('#')[:2])
            bib_fname = path.join(bib_dir, hash_doi + '.bib')
            entry = bibtex.parse_bibtex_file(bib_fname)
            # use real doi with slash as article id
            doi = "/".join(rec['filename'].split('#')[:2])
            articles_csv.writerow((doi,
                                   filename,
                                   entry.get('author', '?'),
                                   entry.get('year', '?'),
                                   entry.get('title', '?'),
                                   entry.get('journal', '?'),
                                   entry.get('volume', '?'),
                                   entry.get('number', '?'),
                                   'Article'))

        if rec['treeNumber'] != tree_number:
            # moving to new tree
            tree_number = rec['treeNumber']
            sent_id = '{}/{}'.format(doi, tree_number)
            # get char offsets for sentence
            sent_elem = sentences_elem[int(tree_number) - 1]
            begin = int(sent_elem[0][0][2].text)
            end = int(sent_elem[0][-1][3].text)
            sent_chars = sent_text[begin:end]
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
                             filename,
                             rec['nodeNumber'],
                             rec['extractName'],
                             rec['charOffsetBegin'],
                             rec['charOffsetEnd'],
                             event_labels))

        has_event_csv.writerow((sent_id,
                                event_id,
                                'HAS_EVENT'))

        var_id = rec['subStr']
        if var_id not in variables:
            variables_csv.writerow((var_id,
                                    'Variable'))
            variables.add(var_id)

        theme_csv.writerow((event_id,
                            var_id,
                            'THEME'))




def postproc_graph(neobox_home, box_name, neobox_username, neobox_password,
                   silence_loggers=True):
    """
    Post-process graph after import.

    Creates VarChange/VarIncrease/VarDecrease aggregation nodes,
    creates co-occurrence relations and
    imposes constraints and indices.

    Parameters
    ----------
    neobox_home : str
        directory of neobox warehouse containing all neobox server instances
    box_name : str
        name of neobox server instance
    neobox_username : str
    neobox_password : str
    silence_loggers : bool
        silence info logging from the py2neo and httpstream
    """
    # TODO: configure timeout
    # a hack to increase timeout - see
    # http://stackoverflow.com/questions/27078352/py2neo-2-0-errorhttpstream-socketerror-timed-out
    from py2neo.packages.httpstream import http
    http.socket_timeout = 9999

    if silence_loggers:
        logging.getLogger('py2neo').setLevel(logging.WARNING)
        logging.getLogger('httpstream').setLevel(logging.WARNING)

    warehouse = neobox.Warehouse(neobox_home)
    box = warehouse.box(box_name)
    authenticate(box.server.service_root.uri.host_port,
                 neobox_username, neobox_password)
    graph = box.server.graph
    run = graph.cypher.execute

    # -----------------------------------------------------------------------------
    # Constraints
    # -----------------------------------------------------------------------------
    log.info('creating constraints')

    # Create a unique property constraint on the label and property combination.
    # If any other node with that label is updated or created with a property
    # that already exists, the write operation will fail.
    # This constraint will create an accompanying index.
    # See http://neo4j.com/docs/stable/query-constraints.html

    run("""
    CREATE CONSTRAINT ON (s:Article)
    ASSERT s.doi IS UNIQUE
    """)

    run("""
    CREATE CONSTRAINT ON (s:Sentence)
    ASSERT s.sentID IS UNIQUE
    """)

    run("""
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
        run("""
        MATCH (v:Variable) <-[:THEME]- (:{event})
        MERGE (v) <-[:VAR]- (ve:Var{event}:VarEvent {{subStr: v.subStr}})
        """.format(event=event))

    # Next connect each Change/Increase/Decrease event to the
    # VarChange/VarIncrease/VarDecrease node of the event's variable,
    # using an INST relation.
    for event in events:
        run("""
        MATCH (ve:Var{event}) -[:VAR]-> (:Variable) <-[:THEME]- (e:{event})
        MERGE (ve) -[:INST]-> (e)
        """.format(event=event))

    # Compute the out-degree of the VarChange/VarIncrease/VarDecrease nodes
    # on the INST relation, where n represents the number of
    # changing/increasing/decreasing events
    for event in events:
        run("""
        MATCH (ve:Var{event}) -[:INST]-> (:{event})
        WITH ve, count(*) AS n
        SET ve.n = n
        """.format(event=event))

    # impose more constraints
    run("""
    CREATE CONSTRAINT ON (ve:VarChange)
    ASSERT ve.subStr IS UNIQUE
    """)

    run("""
    CREATE CONSTRAINT ON (ve:VarIncrease)
    ASSERT ve.subStr IS UNIQUE
    """)

    run("""
    CREATE CONSTRAINT ON (ve:VarDecrease)
    ASSERT ve.subStr IS UNIQUE
    """)

    # create index on VarEvent (where subStr is not unique)
    run("CREATE INDEX ON :VarEvent(subStr)")

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
            run("""
                MATCH (ve1:Var{event1}) -[:INST]-> (:{event1}) <-[:HAS_EVENT]- (s:Sentence) -[:HAS_EVENT]-> (:{event2}) <-[:INST]- (ve2:Var{event2})
                WHERE id(ve1) < id(ve2)
                WITH ve1, ve2, count(*) AS n
                MERGE (ve1) -[:COOCCURS {{n: n}}]-> (ve2)
            """.format(event1=event1, event2=event2))
