"""
extract, prune and process variables
"""

import json
import logging
import re
from os.path import join, dirname
from subprocess import check_output, STDOUT
from tempfile import NamedTemporaryFile

from lxml import etree

from baleen.utils import make_dir


log = logging.getLogger(__name__)

PRUNE_OPTIONS = '--unique'


def extract_vars(extract_vars_exec, trees_dir, vars_file):
    """
    Extract variables in change/increase/decrease events
    """
    make_dir(dirname(vars_file))
    cmd = "{} {} {}".format(extract_vars_exec, trees_dir, vars_file)
    log.info("\n" + cmd)
    # universal_newlines=True is passed so the return value will be a string
    # rather than bytes
    ret = check_output(cmd, shell=True, stderr=STDOUT, universal_newlines=True)
    log.info("\n" + ret)


def preproc_vars(trans_exec, in_vars_file, out_vars_file, trans_file,
                 tmp_file=None):
    """
    Preprocess variables

    Deletes determiners (DT), personal/possessive pronouns (PRP or PRP$) and
    list item markers (LS or LST).
    """
    make_dir(dirname(out_vars_file))
    if not tmp_file:
        tmp_file = NamedTemporaryFile()
        tmp_file = tmp_file.name
    cmd = ' '.join([trans_exec, in_vars_file, tmp_file, trans_file])
    log.info("\n" + cmd)
    # universal_newlines=True is passed so the return value will be a string
    # rather than bytes
    ret = check_output(cmd, shell=True, universal_newlines=True)
    log.info("\n" + ret)
    records = json.load(open(tmp_file))
    # Remove any var that has descendents (i.e. from which a node was deleted)
    # Also remove empty vars
    out_vars_records = [rec for rec in records
                       if rec['subStr'] and not 'descendants' in rec]
    json.dump(out_vars_records, open(out_vars_file, 'w'), indent=0)


def prune_vars(prune_vars_exec, vars_file, pruned_file, options=PRUNE_OPTIONS):
    """
    Prune variables in change/increase/decrease events
    """
    make_dir(dirname(pruned_file))
    cmd = "{} {} {} {}".format(prune_vars_exec, options, vars_file,
                               pruned_file)
    log.info("\n" + cmd)
    # universal_newlines=True is passed so the return value will be a string
    # rather than bytes
    ret = check_output(cmd, shell=True, stderr=STDOUT, universal_newlines=True)
    log.info("\n" + ret)


def add_offsets(in_vars_fname, scnlp_dir, out_vars_fname=None):
    """
    Add character offsets to extracted variables

    Adds charOffsetBegin and charOffesetEnd fields to JSON records for
    variables. Offsets are absolute w.r.t. to the input text (abstract) to
    SCNLP.

    Parameters
    ----------
    in_vars_fname : str
        name of file with extracted variables in json format
    scnlp_dir : str
        directory containing scnlp output in xml format
    out_vars_fname : str
        name of file for writing updated vars

    Returns
    -------
    json records
    """
    records = json.load(open(in_vars_fname))
    filename = None
    tree_number = None

    for rec in records:
        #log.debug('adding offset to:\n' + json.dumps(rec))

        if rec['filename'] != filename:
            filename = rec['filename']
            scnlp_fname = join(scnlp_dir, filename[:-6] + '.xml')
            xml_tree = etree.parse(scnlp_fname)
            sentences_elem = xml_tree.find('.//sentences')
            tree_number = None

        if rec['treeNumber'] != tree_number:
            tree_number = rec['treeNumber']
            sent_elem = sentences_elem[int(tree_number) - 1]
            tokens_elem = sent_elem.find('tokens')
            parse = sent_elem.find('parse').text
            node2indices = parse_pstree(parse, tokens_elem)

        indices = node2indices[rec['nodeNumber']]
        rec['charOffsetBegin'], rec['charOffsetEnd'] = indices

    if out_vars_fname:
        with open(out_vars_fname, 'w') as outf:
            log.info('writing vars with offsets to ' + out_vars_fname)
            json.dump(records, outf, indent=0)

    return records


def parse_pstree(parse, tokens_elem):
    """
    Traverse phrase structure tree in labeled bracket format, storing
    character offsets for each node in the node2offsets dict. The parse is a
    labeled brackets strings as under <parse>...</parse> in the XML output by
    Stanford CoreNLP. The corresponding tokens are the elements contained in
    <tokens>...</tokens>.
    """

    # Construct a regexp that will tokenize the string.
    open_b, close_b = '()'
    open_pattern, close_pattern = (re.escape(open_b), re.escape(close_b))
    node_pattern = '[^\s%s%s]+' % (open_pattern, close_pattern)
    leaf_pattern = '[^\s%s%s]+' % (open_pattern, close_pattern)
    symbol_re = re.compile('%s\s*(%s)?|%s|(%s)' % (
        open_pattern, node_pattern, close_pattern, leaf_pattern), )

    # Build an element tree to conveniently propagate offsets from child to
    # parent nodes
    parent = etree.Element('__ROOT__')
    token_count = 0

    for match in symbol_re.finditer(parse):
        symbol = match.group()
        # Beginning of a tree/subtree
        if symbol[0] == open_b:
            # create non-terminal node
            child = etree.Element('node')
            parent.append(child)
            parent = child
        # End of a tree/subtree
        elif symbol == close_b:
            parent = parent.getparent()
            # Propagate character offsets upwards
            parent.set('start', parent[0].get('start'))
            parent.set('end', parent[-1].get('end'))
        # Leaf node
        else:
            # Copy offsets from corresponding token
            token = tokens_elem[token_count]
            token_count += 1
            start, end = token[2].text, token[3].text
            # Propagate character offsets upwards
            parent.set('start', start)
            parent.set('end', end)
            # create terminal node
            child = etree.Element('node', start=start, end=end)
            parent.append(child)

    # Finally map node numbers to character offsets
    return dict((n, (int(elem.get('start')), int(elem.get('end'))))
                for n, elem in enumerate(parent.iter()))

