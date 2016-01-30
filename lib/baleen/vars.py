"""
processing variables
"""

import json
import re
from os.path import join
from lxml import etree


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
        open_pattern, node_pattern, close_pattern, leaf_pattern))

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
