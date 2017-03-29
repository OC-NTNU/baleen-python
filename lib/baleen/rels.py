"""
relation extraction through tree pattern matching
"""

import logging
import json
import subprocess
from collections import defaultdict
from configparser import ConfigParser

from pathlib import Path
from nltk.tree import Tree

from baleen.utils import derive_path, get_doi

log = logging.getLogger(__name__)
log.setLevel('INFO')


# TODO 3: doc strings


def tag_var_nodes(vars_dir, trees_dir, tagged_dir):
    """
    Tag variable nodes in tree

    Tag variables nodes in trees with "_VAR:f:n:m:e+" suffix where
    f is the name of the parse file,
    n is the tree number,
    m is the variable's node number and
    e is name of the pattern(s) used for extracting this variable.
    Will only output those trees containing at least two variables.
    """
    # At first I used the tregex's '-f' option to print the filename,
    # but when traversing the files in a directory,
    # it prints the wrong filenames (after the first one?),
    # so now the filename is encoded in the node label too.
    trees_dir = Path(trees_dir)
    tagged_dir = Path(tagged_dir)
    tagged_dir.mkdir(parents=True, exist_ok=True)

    for vars_fname in Path(vars_dir).glob('*.json'):
        records = json.load(vars_fname.open())

        if not len(records) > 1:
            # must contain at least two variables
            continue

        # create a dict mapping each tree number to a list of
        # (nodeNumber, extractName) tuples for its variables
        d = defaultdict(list)
        record = {}

        for record in records:
            pair = record['nodeNumber'], record['key']
            d[record['treeNumber']].append(pair)

        lemtree_fname = record['filename']
        parses_path = (trees_dir / lemtree_fname)
        log.info('reading parses from {}'.format(parses_path))
        parses = parses_path.open().readlines()
        tagged_parses = []

        for tree_number, pairs in d.items():
            if len(pairs) > 1:
                # tree numbers in records count from one
                lbs = parses[tree_number - 1]
                log.debug(lbs)
                tree = Tree.fromstring(lbs)
                # get NLTK-style indices for all nodes in a preorder
                # traversal of the tree
                positions = tree.treepositions()
                vars_count = 0

                for node_number, key in pairs:
                    # node numbers in records count from one
                    position = positions[node_number - 1]

                    try:
                        subtree = tree[position]
                    except RecursionError:
                        # TODO This is a quick fix for some problem with extremely long trees
                        log.error('skipping node_number {}, key {} because of RecursionError in tree\n{}'.format(
                            node_number, key, tree))
                        continue

                    try:
                        subtree.set_label(
                            '{}_VAR_{}'.format(subtree.label(), key))
                    except AttributeError:
                        log.error('skipping variable "{}" because it is a leaf '
                                  'node ({})'.format(subtree, key))
                    else:
                        vars_count += 1

                if vars_count > 1:
                    tagged_parses.append(tree.pformat(margin=99999))

        if tagged_parses:
            tagged_fname = derive_path(lemtree_fname, new_dir=tagged_dir)
            log.info('writing tagged trees to {}'.format(tagged_fname))
            with tagged_fname.open('w') as outf:
                outf.writelines(tagged_parses)


def extract_relations(class_path,
                      tagged_dir,
                      pattern_path,
                      rels_dir):
    """
    Extract relations between events
    """
    pat_defs = read_patterns(pattern_path)
    rel_records = defaultdict(list)

    for pat_name, items in pat_defs.items():
        if pat_name != 'DEFAULT':
            relation = items['relation']
            pattern = items['pattern'].strip()

            matches = tregex(class_path, tagged_dir, pattern)
            parse_matches(matches, pat_name, relation, rel_records)

    write_relations(rel_records, rels_dir)


def read_patterns(pattern_path):
    # abusing config parser to read patterns, e.g.
    #
    #     [CAUSE_1]
    #     pattern = S <<# cause << /^NP_VAR/=from << (/^NP_VAR/=to ,, =from)
    #     relation = cause
    #
    # where the section defines the name of the pattern
    pat_defs = ConfigParser()
    pattern_path = Path(pattern_path)

    # pattern_path can be single filename or directory
    if pattern_path.is_dir():
        pattern_fnames = pattern_path.glob('*')
    else:
        pattern_fnames = [pattern_path]

    for fname in pattern_fnames:
        log.info('reading relation extraction patterns from {}'.format(fname))
        pat_defs.read_file(fname.open())

    return pat_defs


def tregex(class_path,
           trees_dir,
           pattern,
           memory='3g'):
    """
    Run Stanford Tregex
    """
    cmd = ('java '
           '-Xmx{memory} '
           '-cp "{class_path}/*" '
           'edu.stanford.nlp.trees.tregex.TregexPattern '
           # '-f '  # print filename
           '-u '  # print only node label, not complete subtrees
           '-h from '  # print node assigned to handle 'from'
           '-h to '  # print node assigned to handle 'to'
           '"{pattern}" '
           '{trees_dir}'
           ).format(memory=memory, class_path=class_path, pattern=pattern,
                    trees_dir=trees_dir)

    log.info('\n' + cmd)
    matches = subprocess.check_output(cmd, shell=True, universal_newlines=True)
    log.debug('\n{}'.format(matches))
    return matches


def parse_matches(matches, pat_name, relation, rel_records):
    if matches:
        lines = matches.strip().split('\n')
        for i in range(0, len(lines), 2):
            from_node, to_node = lines[i:i + 2]
            filename, tree_number, node_number, *_ = from_node.split('_VAR_')[-1].split(':')
            sent_id = get_doi(filename) + '/' + tree_number
            record = dict(
                filename=filename,
                sentenceId=sent_id,
                fromNodeId=from_node.split('_VAR_')[-1],
                toNodeId=to_node.split('_VAR_')[-1],
                patternName=pat_name,
                relation=relation)
            rel_records[filename].append(record)


# Old version for use with '-f' option
#
# def parse_matches(matches, pat_name, relation, rel_records):
#     for triple in matches.rstrip().split('# ')[1:]:
#         filename, from_node, to_node = triple.strip().split('\n')
#         filename = str(Path(filename).basename())
#         record = dict(
#             filename=filename,
#             fromNodeId=filename + ':' + from_node.split('_VAR_')[-1],
#             toNodeId=filename + ':' + to_node.split('_VAR_')[-1],
#             patternName=pat_name,
#             relation=relation)
#         rel_records[filename].append(record)


def write_relations(rel_records, rels_dir):
    """
    write extracted relations per file as json records
    """
    rels_dir = Path(rels_dir)
    rels_dir.mkdir(parents=True, exist_ok=True)

    for fname, rec_list in rel_records.items():
        rels_fname = derive_path(fname, new_dir=rels_dir, append_tags=['rels'],
                                 new_ext='json')
        log.info('writing extracted relations to {}'.format(rels_fname))
        json.dump(rec_list, rels_fname.open('w'), indent=0)
