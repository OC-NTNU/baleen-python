from argh import arg

from baleen.arghconfig import docstring

from baleen import scnlp, vars
from baleen import n4j
from baleen import cite
from baleen.utils import remove_any


# TODO: consider defining __all__


# not using *args and **kwargs below,
# because function needs to wrapped by argh


@docstring(scnlp.core_nlp)
@arg('--resume', help='toggle default for resuming process')
def core_nlp(input,
             out_dir=scnlp.OUT_DIR,
             annotators=scnlp.ANNOTATORS,
             class_path=scnlp.CLASS_PATH,
             version=scnlp.VERSION,
             memory=scnlp.MEMORY,
             threads=scnlp.THREADS,
             replace_ext=scnlp.REPLACE_EXT,
             output_ext=scnlp.OUTPUT_EXT,
             options=scnlp.OPTIONS,
             stamp=scnlp.STAMP,
             resume=scnlp.RESUME,
             use_sr_parser=scnlp.USE_SR_PARSER):
    scnlp.core_nlp(input=input,
                   out_dir=out_dir,
                   annotators=annotators,
                   class_path=class_path,
                   version=version,
                   memory=memory,
                   threads=threads,
                   replace_ext=replace_ext,
                   output_ext=output_ext,
                   options=options,
                   stamp=stamp,
                   resume=resume,
                   use_sr_parser=use_sr_parser)


def split_sent(input,
               out_dir=scnlp.OUT_DIR,
               class_path=scnlp.CLASS_PATH,
               version=scnlp.VERSION,
               memory=scnlp.MEMORY,
               threads=scnlp.THREADS,
               replace_ext=scnlp.REPLACE_EXT,
               output_ext=scnlp.OUTPUT_EXT,
               options='-ssplit.newlineIsSentenceBreak always',
               stamp=scnlp.STAMP,
               resume=scnlp.RESUME):
    """
    Split text into sentences
    """
    core_nlp(input=input,
             out_dir=out_dir,
             annotators='tokenize,ssplit',
             class_path=class_path,
             version=version,
             memory=memory,
             threads=threads,
             replace_ext=replace_ext,
             output_ext=output_ext,
             options=options,
             stamp=stamp,
             resume=resume)


def parse_sent(input,
               out_dir=scnlp.OUT_DIR,
               annotators='tokenize,ssplit,pos,lemma,parse',
               class_path=scnlp.CLASS_PATH,
               version=scnlp.VERSION,
               memory=scnlp.MEMORY,
               threads=scnlp.THREADS,
               replace_ext=scnlp.REPLACE_EXT,
               output_ext=scnlp.OUTPUT_EXT,
               options='-ssplit.eolonly',
               stamp=scnlp.STAMP,
               resume=scnlp.RESUME,
               use_sr_parser=scnlp.USE_SR_PARSER):
    """
    Parse sentences (one sentence per line)
    """
    core_nlp(input=input,
             out_dir=out_dir,
             annotators=annotators,
             class_path=class_path,
             version=version,
             memory=memory,
             threads=threads,
             replace_ext=replace_ext,
             output_ext=output_ext,
             options=options,
             stamp=stamp,
             resume=resume,
             use_sr_parser=use_sr_parser)


def lemma_trees(scnlp_dir, out_dir):
    """
    Extract lemmatized parse trees
    """
    scnlp.extract_lemmatized_parse_trees(scnlp_dir, out_dir)


@docstring(vars.extract_vars)
@arg('-r', '--resume', help='toggle default for resuming process')
def ext_vars(extract_vars_exec, trees_dir, vars_dir,
             resume=vars.RESUME_EXTRACT):
    vars.extract_vars(extract_vars_exec, trees_dir, vars_dir, resume)


@docstring(vars.add_offsets)
@arg('-r', '--resume', help='toggle default for resuming process')
def offsets(vars_dir, scnlp_dir, resume=vars.RESUME_OFFSET):
    vars.add_offsets(vars_dir, scnlp_dir, resume)


@docstring(vars.preproc_vars)
def prep_vars(trans_exec, trans_file, in_vars_dir, out_vars_dir, tmp_dir=None,
              resume=vars.RESUME_PREP):
    vars.preproc_vars(trans_exec, trans_file, in_vars_dir, out_vars_dir,
                      tmp_dir, resume)


@docstring(vars.prune_vars)
def prune_vars(prune_vars_exec, in_vars_dir, out_vars_dir,
               resume=vars.RESUME_PRUNE, options=vars.PRUNE_OPTIONS):
    vars.prune_vars(prune_vars_exec, in_vars_dir, out_vars_dir, resume,
                    options=options)


@arg('--max-n-vars', type=int)
@docstring(n4j.vars_to_csv)
def tocsv(vars_dir, scnlp_dir, text_dir, nodes_dir, relations_dir,
          max_n_vars=None):
    n4j.vars_to_csv(vars_dir, scnlp_dir, text_dir, nodes_dir,
                    relations_dir, max_n_vars)


@docstring(n4j.neo4j_import)
def toneo(warehouse_home, server_name, nodes_dir, relations_dir, options=None):
    n4j.neo4j_import(warehouse_home, server_name, nodes_dir, relations_dir,
                     options=options)


@docstring(n4j.postproc_graph)
def ppgraph(warehouse_home, server_name, password=None):
    n4j.postproc_graph(warehouse_home, server_name, password)


def clean(dir):
    """
    Clean output
    """
    remove_any(dir)


setup_server = n4j.setup_server
remove_server = n4j.remove_server
start_server = n4j.start_server
stop_server = n4j.stop_server


@arg('-r', '--resume', help='toggle default for resuming process')
@docstring(cite.add_citations)
def add_cit(warehouse_home, server_name, cache_file, resume=False,
            password=None):
    cite.add_citations(warehouse_home, server_name, cache_file, resume=resume,
                       password=password)


@arg('-r', '--resume', help='toggle default for resuming process')
@docstring(cite.add_metadata)
def add_meta(warehouse_home, server_name, cache_file, resume=False,
             password=None):
    cite.add_metadata(warehouse_home, server_name, cache_file, resume=resume,
                      password=password)
