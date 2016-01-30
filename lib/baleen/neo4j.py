"""
create neo4j graph from variables
"""

import logging

log = logging.getLogger(__name__)

import subprocess
import csv
import json
import os
import os.path as path

from lxml import etree

from baleen import bibtex


def neo4j_import(server, nodes=[], relationships=[], options=''):
    """
    Create a new Neo4j database from data in CSV files by
    running the neo4j-import command line tool

    Parameters
    ----------
    server : py2neo.server.GraphServer
        neo4j graph server (stopped)
    nodes : list
        .cvs filenames for nodes
    relationships :list
        .csv filenames for relationships
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
    # store.drop will raise RuntimeError if server is running
    log.info('deleting database directory ' + server.store.path)
    try:
        server.store.drop()
    except FileNotFoundError:
        log.warn('database directory ' + server.store.path + ' not found')

    bin_dir = path.dirname(server.script)
    executable = path.join(bin_dir, 'neo4j-import')
    args = [executable, '--into', server.store.path]

    for fname in nodes:
        args.append('--nodes')
        args.append(fname)

    for fname in relationships:
        args.append('--relationships')
        args.append(fname)

    args += options.split()

    log.info('running subprocess: ' + ' '.join(args))
    return subprocess.run(args)


def vars_to_csv(vars_fname, scnlp_dir, sent_dir, bib_dir, csv_dir):
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
        directory containig BibTex entries (one per file)
    csv_dir : str
        output directory for csv files

    Notes
    -----
    See http://neo4j.com/docs/stable/import-tool-header-format.html
    """

    def create_cvs_file(fname, header=(':START_ID', ':END_ID', ':TYPE')):
        fname = path.join(csv_dir, fname)
        log.info('creating ' + fname)
        file = open(fname, 'w', newline='')
        csv_file = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        csv_file.writerow(header)
        return csv_file

    if not path.exists(csv_dir):
        os.mkdir(csv_dir)

    # create csv files for nodes
    articles_csv = create_cvs_file('articles.csv',
                                   ('doi:ID',
                                    'filename',
                                    'author',
                                    'year',
                                    'title',
                                    'journal',
                                    'volume',
                                    'number',
                                    ':LABEL'))

    sentences_csv = create_cvs_file('sentences.csv',
                                    ('sentID:ID',
                                     'treeNumber:int',
                                     'charOffsetBegin:int',
                                     'charOffsetEnd:int',
                                     'sentChars',
                                     ':LABEL'))

    variables_csv = create_cvs_file('variables.csv',
                                    ('subStr:ID',
                                     ':LABEL'))

    events_csv = create_cvs_file('events.csv',
                                 ('eventID:ID',
                                  'filename',
                                  'nodeNumber:int',
                                  'extractName',
                                  'charOffsetBegin:int',
                                  'charOffsetEnd:int',
                                  ':LABEL'))

    # create csv files for relations
    has_sent_csv = create_cvs_file('has_sent.csv')
    theme_csv = create_cvs_file('theme.csv')
    has_event_csv = create_cvs_file('has_event.csv')

    filename = None
    tree_number = None
    variables = set()

    for rec in json.load(open(vars_fname))[:100]:
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
            sent_fname = ''.join(filename.partition('#sent')[:2]) + '.txt'
            sent_fname = path.join(sent_dir, sent_fname)
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


def postproc_graph(graph, silence_loggers=True):
    """
    Post-process graph after import,
    creating VarChange/VarIncrease/VarDecrease aggregation nodes,
    creating co-occurrence relations and
    imposing constraints and indices.

    Parameters
    ----------
    server : py2neo.server.GraphServer
        neo4j graph server (running)
    silence_loggers : bool
        silence info logging from the py2neo and httpstream
    """
    if silence_loggers:
        logging.getLogger('py2neo').setLevel(logging.WARNING)
        logging.getLogger('httpstream').setLevel(logging.WARNING)

    run = graph.cypher.execute

    # -----------------------------------------------------------------------------
    # Constraints
    # -----------------------------------------------------------------------------

    # Create a unique property constraint on the label and property combination.
    # If any other node with that label is updated or created with a property that
    # already exists, the write operation will fail.
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
