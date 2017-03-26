"""
Stanford CoreNLP wrapper and CoreNLP related post-processing
"""

import logging

from os.path import join
from tempfile import NamedTemporaryFile
from subprocess import check_output

from lxml.etree import ElementTree

from baleen.utils import make_dir, file_list, derive_path

log = logging.getLogger(__name__)

ANNOTATORS = 'tokenize,ssplit,pos,lemma,parse'
OUT_DIR = ''
CLASS_PATH = ''
VERSION = ''
MEMORY = '3g'
THREADS = 1
REPLACE_EXT = True
OUTPUT_EXT = '.xml'
OPTIONS = ''
STAMP = True
RESUME = False
USE_SR_PARSER = False


def core_nlp(input,
             out_dir=OUT_DIR,
             annotators=ANNOTATORS,
             class_path=CLASS_PATH,
             version=VERSION,
             memory=MEMORY,
             threads=THREADS,
             replace_ext=REPLACE_EXT,
             output_ext=OUTPUT_EXT,
             options=OPTIONS,
             stamp=STAMP,
             resume=RESUME,
             use_sr_parser=USE_SR_PARSER):
    """
    Run Stanford CoreNLP

    Parameters
    ----------
    input
    out_dir
    annotators
    class_path
    version
    memory
    threads
    replace_ext
    output_ext
    options
    stamp
    resume
    use_sr_parser

    Returns
    -------

    """
    in_files = file_list(input)
    make_dir(out_dir)

    cmd = ['java']

    if memory:
        cmd.append('-Xmx' + memory)

    if class_path:
        class_path = '"{}"'.format(join(class_path or '.', "*"))
        cmd.append('-cp ' + class_path)

    cmd.append('edu.stanford.nlp.pipeline.StanfordCoreNLP')

    if annotators:
        cmd.append('-annotators ' + annotators)

    if stamp:
        replace_ext = True
        output_ext = '#scnlp_v{}{}'.format(version or '', output_ext)

    if replace_ext:
        cmd.append('-replaceExtension')

    if output_ext:
        cmd.append('-outputExtension "{}"'.format(output_ext))

    if out_dir:
        cmd.append('-outputDirectory ' + out_dir)

    if threads:
        cmd.append('-threads {}'.format(threads))

    if resume:
        in_files = [fname for fname in in_files
                    if not derive_path(fname,
                                       new_dir=out_dir,
                                       new_ext=output_ext).exists()]

    if options:
        cmd.append(options)

    if 'parse' in annotators and use_sr_parser:
        cmd.append(
            '-parse.model edu/stanford/nlp/models/srparser/englishSR.ser.gz')

    # create a temporary file with input filenames
    tmp_file = NamedTemporaryFile("wt", buffering=1)
    tmp_file.write('\n'.join(in_files) + "\n")

    cmd.append('-filelist ' + tmp_file.name)

    cmd = ' '.join(cmd)
    log.info('\n' + cmd)
    ret = check_output(cmd, shell=True, universal_newlines=True)
    log.info('\n {}'.format(ret))

    return ret


def extract_parse_trees(scnlp_files, parse_dir):
    """
    extract parse trees (PTB labeled bracket structures) from Stanford
    CoreNLP XML ouput
    """
    make_dir(parse_dir)

    for scnlp_fname in file_list(scnlp_files, "*.xml"):
        nlp_doc = ElementTree(file=scnlp_fname)

        parse_fname = derive_path(scnlp_fname,
                                  new_dir=parse_dir,
                                  new_ext='.parse')
        log.info('writing {}'.format(parse_fname))

        with open(parse_fname, "wt", encoding="utf-8") as parse_file:
            for parse_elem in nlp_doc.findall(".//parse"):
                parse_file.write(parse_elem.text + "\n")


def extract_lemmatized_parse_trees(scnlp_files, parse_dir):
    """
    extract lemmatzied parse trees (PTB labeled bracket structures) from
    Stanford CoreNLP XML ouput
    """
    make_dir(parse_dir)

    for scnlp_fname in file_list(scnlp_files, "*.xml"):
        nlp_doc = ElementTree(file=scnlp_fname)

        parse_fname = derive_path(scnlp_fname,
                                  new_dir=parse_dir,
                                  new_ext='.parse')
        log.info('writing {}'.format(parse_fname))

        with parse_fname.open("wt", encoding="utf-8") as parse_file:
            for sentence_elem in nlp_doc.iterfind(".//sentence"):
                lemmas = sentence_elem.iterfind("tokens/token/lemma")
                word_parse = sentence_elem.find("parse").text.strip()
                lemma_parse = " ".join(_lemmatized_node(node, lemmas)
                                       for node in word_parse.split(" "))
                parse_file.write(lemma_parse + "\n")


def _lemmatized_node(node, lemmas):
    if node.startswith("("):
        # non-terminal node
        return node
    else:
        # terminal
        brackets = "".join(node.partition(")")[1:])
        return next(lemmas).text + brackets
