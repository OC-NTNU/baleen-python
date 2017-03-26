"""
Import CSV files into Neo4j
"""

import csv
import json
import re
import subprocess
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path

import neokit
from lxml import etree

from baleen.utils import get_doi, derive_path
from baleen.cite import get_cache, get_all_metadata, get_citation

log = logging.getLogger(__name__)


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

    log.info('deleting database directory {}'.format(server.store_path))
    server.delete_store()

    executable = Path(server.home) / 'bin' / 'neo4j-import'
    args = [executable, '--into', server.store_path]

    for fname in Path(nodes_dir).  glob('*.csv'):
        args.append('--nodes')
        args.append(fname.resolve())

    for fname in Path(relations_dir).glob('*.csv'):
        args.append('--relationships')
        args.append(fname.resolve())

    if options:
        args += options.split()

    args = [str(a) for a in args]

    log.info('running subprocess: ' + ' '.join(args))

    completed_proc = subprocess.run(args)

    # restart server after import
    server.start()

    return completed_proc


def create_unique_csv_nodes(file_pats, out_dir):
    """
    Create CSV files with unique nodes
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dd = defaultdict(list)

    # create mapping from file basenames to corresponding file paths
    for path in _expand_file_pats(file_pats):
        log.info('reading non-unique csv nodes from {}'.format(path))
        dd[path.name].append(path)

    for fname, paths in dd.items():
        uniq_lines = set()
        header = prev_header = None

        for path in paths:
            with path.open() as inf:
                header = inf.readline()
                if prev_header:
                    assert header == prev_header
                prev_header = header
                for line in inf:
                    uniq_lines.add(line)

        out_fname = out_dir / fname
        log.info('writing unique csv nodes to {}'.format(out_fname))

        with out_fname.open('w') as outf:
            outf.write(header)
            outf.writelines(uniq_lines)


def neo4j_import_multi(warehouse_home, server_name, node_file_pats, rel_file_pats, exclude_file_pats,
                       options=None):
    """
    Create a new Neo4j database from multiple data sources in CSV format

    Parameters
    ----------
    warehouse_home : str
        directory of neokit warehouse containing all neokit server instances
    server_name : str
        name of neokit server instance
    node_file_pats : list of str
        glob patterns for files with node in CSV format
    rel_file_pats : list of str
        glob patterns for files with relations in CSV format
    exclude_file_pats: list of str
        glob patterns for node/relation files to exclude
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

    excluded_files = set(_expand_file_pats(exclude_file_pats))

    for fname in _expand_file_pats(node_file_pats):
        if fname not in excluded_files:
            args.append('--nodes')
            args.append(fname.resolve())

    for fname in _expand_file_pats(rel_file_pats):
        if fname not in excluded_files:
            args.append('--relationships')
            args.append(fname.resolve())

    if options:
        args += options.split()

    args = [str(a) for a in args]
    log.info('running subprocess: ' + ' '.join(args))

    completed_proc = subprocess.run(args)

    # restart server after import
    server.start()

    return completed_proc


def articles_to_csv(vars_dir, text_dir, meta_cache_dir, cit_cache_dir, nodes_csv_dir,
                    max_n=None, online=True):
    """
    Transform articles to csv tables that can be imported by neo4j

    Parameters
    ----------
    vars_dir
    text_dir
    meta_cache_dir
    cit_cache_dir
    nodes_csv_dir
    max_n
    online

    Returns
    -------

    """
    Path(nodes_csv_dir).mkdir(parents=True, exist_ok=True)
    # hold on to open files
    open_files = []

    articles_csv = create_csv_file(nodes_csv_dir,
                                   'articles.csv',
                                   open_files,
                                   ('doi:ID',
                                    'filename',
                                    'title',
                                    'journal',
                                    'year',
                                    'month',
                                    'day',
                                    'ISSN',
                                    'publisher',
                                    'citation',
                                    ':LABEL'))

    # mapping from DOI to text files
    doi2txt = _doi2txt_fname(text_dir)

    meta_cache = get_cache(meta_cache_dir)
    cit_cache = get_cache(cit_cache_dir)
    pattern = re.compile(r"\s+")
    fnames = list(Path(vars_dir).glob('*.json'))[:max_n]

    for json_fname in fnames:
        doi = get_doi(json_fname)

        try:
            text_fname = doi2txt[doi]
        except KeyError:
            log.error('no matching text file for DOI ' + doi)
            continue

        metadata = get_all_metadata(doi, meta_cache, online=online)
        citation = get_citation(doi, cit_cache, online=online)

        # normalize by stripping whitespace and replacing any remaining whitespace substring
        # by a single space
        for k, v in metadata.items():
            if isinstance(v, str):
                metadata[k] = pattern.sub(' ', v.strip())

        citation = pattern.sub(' ', citation.strip())

        # create article node
        articles_csv.writerow((doi, text_fname, metadata['title'], metadata['journal'], metadata['year'],
                               metadata['month'], metadata['day'], metadata['ISSN'], metadata['publisher'],
                               citation, 'Article'))
        # TODO: post-process to remove Articles nodes without Sentence node

    # release opened files
    for f in open_files:
        f.close()


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
    # TODO 3: change article nodes to document
    # hold on to open files
    open_files = []

    Path(nodes_csv_dir).mkdir(parents=True, exist_ok=True)
    Path(relation_csv_dir).mkdir(parents=True, exist_ok=True)

    # create csv files for nodes
    sentences_csv = create_csv_file(nodes_csv_dir,
                                    'sentences.csv',
                                    open_files,
                                    ('sentID:ID',
                                     'treeNumber:int',
                                     'charOffsetBegin:int',
                                     'charOffsetEnd:int',
                                     'sentChars',
                                     ':LABEL'))

    variables_csv = create_csv_file(nodes_csv_dir,
                                    'variables.csv',
                                    open_files,
                                    ('subStr:ID',
                                     ':LABEL'))

    events_csv = create_csv_file(nodes_csv_dir,
                                 'events.csv',
                                 open_files,
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
                                   'has_sent.csv',
                                   open_files)
    has_var_csv = create_csv_file(relation_csv_dir,
                                  'has_var.csv',
                                  open_files)
    has_event_csv = create_csv_file(relation_csv_dir,
                                    'has_event.csv',
                                    open_files)
    tentails_var_csv = create_csv_file(relation_csv_dir,
                                       'tentails_var.csv',
                                       open_files,
                                       (':START_ID',
                                        ':END_ID',
                                        'transformName',
                                        ':TYPE'))

    # set of all variable types in text collection
    variable_types = set()

    # mapping from DOI to text files
    doi2txt = _doi2txt_fname(text_dir)

    pattern = re.compile('[\n\r]')
    filenames = list(Path(vars_dir).glob('*.json'))[:max_n]

    for json_fname in filenames:
        records = json.load(json_fname.open())

        if not records:
            log.warning('skipping empty variables file: {}'.format(json_fname))
            continue

        log.info('processing variables from file: {}'.format(json_fname))

        doi = get_doi(json_fname)

        try:
            text_fname = doi2txt[doi]
        except KeyError:
            log.error('no matching text file for DOI ' + doi)
            continue

        text = text_fname.open().read()

        # read corenlp analysis
        tree_fname = records[0]['filename']
        scnlp_fname = derive_path(tree_fname, new_dir=scnlp_dir, new_ext='xml')
        xml_tree = etree.parse(str(scnlp_fname))
        sentences_elem = xml_tree.getroot()[0][0]

        tree_number = None

        # mapping of record's "key" to "subStr" attribute,
        # needed for TENTAILS_VAR relation
        key2var = {}
        sent_id = None

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
                # neo4j-import fails on newlines, so replace all \n and \r with a space
                sent_chars = pattern.sub(' ', sent_chars)
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

            # TODO 2: weak method of detecting preprocessing
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


def rels_to_csv(rels_dir, nodes_csv_dir, relation_csv_dir, max_n=None):
    """
    Transform extracted relations to csv tables that can be imported by neo4j

    Parameters
    ----------
    rels_dir
    nodes_csv_dir
    relation_csv_dir
    max_n

    Returns
    -------

    """
    # hold on to open files
    open_files = []

    # create csv files for nodes
    causation_csv = create_csv_file(nodes_csv_dir,
                                    'causations.csv',
                                    open_files,
                                    (':ID',
                                     'patternName',
                                     ':LABEL'))

    # create csv files for relations
    has_cause_csv = create_csv_file(relation_csv_dir,
                                    'has_cause.csv',
                                    open_files, )
    has_effect_csv = create_csv_file(relation_csv_dir,
                                     'has_effect.csv',
                                     open_files)
    has_event_csv = create_csv_file(relation_csv_dir,
                                    'has_event2.csv',
                                    open_files)

    causation_n = 0
    filenames = list(Path(rels_dir).glob('*.json'))[:max_n]

    for rel_fname in filenames:
        log.info('adding CausationInst from file {}'.format(rel_fname))
        doi = get_doi(rel_fname)

        for rec in json.load(rel_fname.open()):
            causation_id = '{}/CausationInst/{}'.format(doi, causation_n)
            causation_csv.writerow((causation_id, rec['patternName'], 'CausationInst'))
            has_cause_csv.writerow((causation_id, rec['fromNodeId'], 'HAS_CAUSE'))
            has_effect_csv.writerow((causation_id, rec['toNodeId'], 'HAS_EFFECT'))
            has_event_csv.writerow((rec['sentenceId'], causation_id, 'HAS_EVENT'))
            causation_n += 1

    # release opened files
    for f in open_files:
        f.close()


def create_csv_file(csv_dir, csv_fname, open_files,
                    header=(':START_ID', ':END_ID', ':TYPE')):
    csv_fname = Path(csv_dir) / csv_fname
    log.info('creating {}'.format(csv_fname))
    outf = csv_fname.open('w', newline='')
    csv_file = csv.writer(outf, quoting=csv.QUOTE_MINIMAL)
    csv_file.writerow(header)
    open_files.append(outf)
    return csv_file


def _doi2txt_fname(text_dir):
    """
    Create a dict mapping DOI to path of input text file
    """
    doi2txt = {}

    for p in Path(text_dir).glob('*'):
        doi = get_doi(p)
        if doi in doi2txt:
            log.error('DOI {} already mapped to text file {}; '
                      'ignoring text file {}'.format(doi, doi2txt[doi], p))
        else:
            doi2txt[doi] = p

    return doi2txt


def _expand_file_pats(patterns):
    return (Path(path) for pat in patterns for path in glob(pat))
