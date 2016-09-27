"""
extract, prune and process variables
"""

import json
import logging
import re
from os.path import join, dirname, splitext
from subprocess import check_output, STDOUT
from tempfile import TemporaryDirectory
from path import Path

from lxml import etree

from baleen.utils import make_dir, file_list, derive_path

log = logging.getLogger(__name__)

PRUNE_OPTIONS = '--unique'
RESUME_EXTRACT = False
RESUME_OFFSET = False
RESUME_PREP = False
RESUME_PRUNE = False


def extract_vars(extract_vars_exec, trees_dir, vars_dir, resume=RESUME_EXTRACT):
    """
    Extract variables in change/increase/decrease events
    """
    resume_option = '--resume' if resume else ''
    cmd = "{} {} {} {}".format(extract_vars_exec, resume_option, trees_dir,
                               vars_dir)
    log.info("\n" + cmd)
    # universal_newlines=True is passed so the return value will be a string
    # rather than bytes
    ret = check_output(cmd, shell=True, stderr=STDOUT, universal_newlines=True)
    log.info("\n" + ret)


def preproc_vars(trans_exec, trans_fname, in_vars_dir, out_vars_dir,
                 tmp_dir=None, resume=RESUME_PREP):
    """
    Preprocess variables

    Deletes determiners (DT), personal/possessive pronouns (PRP or PRP$) and
    list item markers (LS or LST).
    """
    # TODO: resume only works if tmp_dir is given
    if not tmp_dir:
        tmp = TemporaryDirectory()
        tmp_dir = tmp.name

    parts = [trans_exec, '--tag "#prep"']
    if resume:
        parts.append('--resume')
    parts += [in_vars_dir, tmp_dir, trans_fname]
    cmd = ' '.join(parts)
    log.info('\n' + cmd)
    # universal_newlines=True is passed so the return value will be a string
    # rather than bytes
    ret = check_output(cmd, shell=True, universal_newlines=True)
    log.info('\n' + ret)

    Path(out_vars_dir).makedirs_p()

    for in_vars_fname in Path(tmp_dir).files():
        out_vars_fname = derive_path(in_vars_fname, new_dir=out_vars_dir)

        if resume and out_vars_fname.exists():
            log.info('skipping existing preprocessed file ' + out_vars_fname)
            continue

        records = json.load(open(in_vars_fname))
        # Remove any var that has descendents
        # (i.e. from which a node was deleted)
        # Also remove empty vars or "NP" vars
        out_vars_records = [rec for rec in records
                            if rec['subStr'] not in ['','NP'] and
                            not 'descendants' in rec]
        if out_vars_fname:
            log.info('writing to preprocessed variable file ' + out_vars_fname)
            json.dump(out_vars_records, out_vars_fname.open('w'), indent=0)
        else:
            log.info('skipping empty preprocessed variable file ' +
                     out_vars_fname)


def prune_vars(prune_vars_exec, in_vars_dir, out_vars_dir, resume=False,
               options=PRUNE_OPTIONS):
    """
    Prune variables in change/increase/decrease events
    """
    parts = [prune_vars_exec]

    if resume:
        parts.append('--resume')

    if options:
        parts.append(options)

    parts += [in_vars_dir, out_vars_dir]

    cmd = ' '.join(parts)
    # universal_newlines=True is passed so the return value will be a string
    # rather than bytes
    ret = check_output(cmd, shell=True, stderr=STDOUT, universal_newlines=True)
    log.info("\n" + ret)


def add_offsets(vars_dir, scnlp_dir, resume=RESUME_OFFSET):
    """
    Add character offsets to extracted variables

    Adds charOffsetBegin and charOffesetEnd fields to JSON records for
    variables. Offsets are absolute w.r.t. to the input text (abstract) to
    SCNLP.

    Parameters
    ----------
    vars_dir : str
        directory of files with extracted variables in json format
    scnlp_dir : str
        directory containing scnlp output in xml format
    resume: bool
       resume process, skipping files that already have offsets
    """
    for var_fname in Path(vars_dir).files():
        records = json.load(open(var_fname))

        try:
            rec = records[0]
        except IndexError:
            log.info('skipping file without extracted variables: ' + var_fname)
            continue

        if (resume and records and
                rec.get('charOffsetBegin') and
                rec.get('charOffsetEnd')):
            log.info('skipping file with existing offsets: ' + var_fname)
            continue

        scnlp_fname = join(scnlp_dir, splitext(rec['filename'])[0] + '.xml')
        xml_tree = etree.parse(scnlp_fname)
        sentences_elem = xml_tree.find('.//sentences')
        tree_number = None

        for rec in records:
            if rec['treeNumber'] != tree_number:
                tree_number = rec['treeNumber']
                sent_elem = sentences_elem[int(tree_number) - 1]
                tokens_elem = sent_elem.find('tokens')
                parse = sent_elem.find('parse').text
                node2indices = parse_pstree(parse, tokens_elem)

            indices = node2indices[rec['nodeNumber']]
            rec['charOffsetBegin'], rec['charOffsetEnd'] = indices

        with open(var_fname, 'w') as outf:
            log.info('adding offsets to file: ' + var_fname)
            json.dump(records, outf, indent=0)


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
    #leaf_pattern = '[^\s%s%s]+' % (open_pattern, close_pattern)
    # Modified original leaf pattern from NLTK to accommodate case like
    # (NP (NP (DT the) (CD 8 1/2) (NN day) (NN period))
    # where the leaf "8 1/2" contains whitespace
    leaf_pattern = '[^\s%s%s]+[^%s%s]*' % (open_pattern, close_pattern,
                                           open_pattern, close_pattern)
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
