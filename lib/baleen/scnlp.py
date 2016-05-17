"""
Stanford CoreNLP wrapper
"""

# TODO doc string

import logging

from os.path import join, exists
from tempfile import NamedTemporaryFile
from os import getenv
from subprocess import check_output

from lxml.etree import ElementTree

from baleen.utils import make_dir, file_list, new_name


log = logging.getLogger(__name__)


class CoreNLP(object):
    def __init__(self,
                 lib_dir=getenv("CORENLP_HOME",
                                "/Users/erwin/local/src/corenlp"),
                 lib_ver=getenv("CORENLP_VER", "3.5.1")):
        self.lib_dir = lib_dir
        self.lib_ver = lib_ver

    def run(self,
            txt_files,
            out_dir,
            annotators="tokenize,ssplit,pos,lemma,ner,parse,dcoref",
            memory="3g",
            threads=1,
            replace_extension=True,
            output_extension=".xml",
            options="",
            stamp=True,
            resume=False):
        """
    
        txt_files:
            a directory, a glob pattern, a single filename or a list of filenames
        """
        make_dir(out_dir)
        in_files = file_list(txt_files)
        class_path = '"{}"'.format(join(self.lib_dir, "*"))

        if stamp:
            replace_extension = True
            output_extension = "#scnlp_v{}{}".format(self.lib_ver,
                                                     output_extension)
        if replace_extension:
            options += " -replaceExtension"
        if output_extension:
            options += ' -outputExtension "{}"'.format(output_extension)

        if resume:
            in_files = [fname for fname in in_files
                        if
                        not exists(new_name(fname, out_dir, output_extension))]

        tmp_file = NamedTemporaryFile("wt", buffering=1)
        tmp_file.write("\n".join(in_files) + "\n")

        cmd = ("java -Xmx{} -cp {} "
               "edu.stanford.nlp.pipeline.StanfordCoreNLP "
               "-annotators {} -filelist {} "
               "-outputDirectory {} -threads {} {}").format(
            memory, class_path, annotators,
            tmp_file.name, out_dir, threads, options)

        log.info("\n" + cmd)
        ret = check_output(cmd, shell=True, universal_newlines=True)
        log.info("\n" + ret)

    def ssplit(self,
               txt_files,
               out_dir=None,
               annotators="tokenize,ssplit",
               memory="3g",
               threads=1,
               options=" -ssplit.newlineIsSentenceBreak always",
               stamp=False,
               resume=False):
        log.info("start splitting sentences")
        self.run(txt_files,
                 out_dir,
                 annotators,
                 memory=memory,
                 threads=threads,
                 options=options,
                 stamp=stamp,
                 resume=resume)

    def parse(self,
              txt_files,
              out_dir=None,
              annotators="tokenize,ssplit,pos,lemma,parse",
              memory="3g",
              threads=1,
              options=" -ssplit.eolonly",
              resume=False):
        log.info("start parsing sentences")
        self.run(txt_files,
                 out_dir,
                 annotators,
                 memory=memory,
                 threads=threads,
                 options=options,
                 resume=resume)


def extract_parse_trees(scnlp_files, parse_dir):
    """
    extract parse trees (PTB labeled bracket structures) from Stanford
    CoreNLP XML ouput
    """
    make_dir(parse_dir)

    for scnlp_fname in file_list(scnlp_files, "*.xml"):
        nlp_doc = ElementTree(file=scnlp_fname)

        parse_fname = new_name(scnlp_fname, parse_dir, ".parse",
                               strip_ext=["xml"])
        log.info("writing " + parse_fname)

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

        parse_fname = new_name(scnlp_fname, parse_dir, ".parse",
                               strip_ext=["xml"])
        log.info("writing " + parse_fname)

        with open(parse_fname, "wt", encoding="utf-8") as parse_file:
            for sentence_elem in nlp_doc.iterfind(".//sentence"):
                lemmas = sentence_elem.iterfind("tokens/token/lemma")
                word_parse = sentence_elem.find("parse").text.strip()
                lemma_parse = " ".join( _lemmatized_node(node, lemmas)
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