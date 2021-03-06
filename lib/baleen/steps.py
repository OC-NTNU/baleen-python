from argh import arg

from baleen.arghconfig import docstring
from baleen import scnlp, vars, cite, rels
from baleen.utils import remove_any
from baleen.n4j.csvimport import articles_to_csv, vars_to_csv, rels_to_csv, neo4j_import, neo4j_import_multi, \
    create_unique_csv_nodes
from baleen.n4j.postproc import postproc_graph, add_citations, add_metadata
from baleen.n4j.report import graph_report
from baleen.n4j.server import setup_server, start_server, stop_server, remove_server


# TODO 3: consider defining __all__


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
@docstring(articles_to_csv)
def arts2csv(vars_dir, text_dir, meta_cache_dir, cit_cache_dir, nodes_dir, max_n_vars=None, online=True):
    articles_to_csv(vars_dir, text_dir, meta_cache_dir, cit_cache_dir, nodes_dir, max_n_vars, online)


@arg('--max-n-vars', type=int)
@docstring(vars_to_csv)
def vars2csv(vars_dir, scnlp_dir, text_dir, nodes_dir, relations_dir,
             max_n_vars=None):
    vars_to_csv(vars_dir, scnlp_dir, text_dir, nodes_dir,
                relations_dir, max_n_vars)


@arg('--max-n-vars', type=int)
@docstring(rels_to_csv)
def rels2csv(rels_dir, nodes_dir, relations_dir, max_n_vars=None):
    rels_to_csv(rels_dir, nodes_dir, relations_dir, max_n_vars)


@docstring(neo4j_import)
def toneo(warehouse_home, server_name, nodes_dir, relations_dir, options=None):
    neo4j_import(warehouse_home, server_name, nodes_dir, relations_dir, options=options)


@docstring(neo4j_import_multi)
def multi_toneo(warehouse_home, server_name, node_file_pats, rel_file_pats, exclude_file_pats,
                options=None):
    neo4j_import_multi(warehouse_home, server_name,
                       node_file_pats.split(':'),
                       rel_file_pats.split(':'),
                       exclude_file_pats.split(':'),
                       options=options)


@docstring(create_unique_csv_nodes)
def uniq(file_pats, out_dir):
    create_unique_csv_nodes(file_pats=file_pats.split(':'), out_dir=out_dir)


@docstring(postproc_graph)
def ppgraph(warehouse_home, server_name, password=None):
    postproc_graph(warehouse_home, server_name, password)


@docstring(graph_report)
def report(warehouse_home, server_name, password=None):
    graph_report(warehouse_home, server_name, password)


def clean(dir):
    """
    Clean output
    """
    remove_any(dir)


@docstring(cite.clean_metadata_cache)
def clean_cache(cache_dir):
    cite.clean_metadata_cache(cache_dir)


@docstring(rels.tag_var_nodes)
def tag_trees(vars_dir, trees_dir, tagged_dir):
    rels.tag_var_nodes(vars_dir, trees_dir, tagged_dir)


@docstring(rels.extract_relations)
def ext_rels(class_path, tagged_dir, pattern_path, rels_dir):
    rels.extract_relations(class_path, tagged_dir, pattern_path, rels_dir)


# Old commands superseded by CSV import of citations and metadata

@arg('-r', '--resume', help='toggle default for resuming process')
@arg('-o', '--online', help='toggle default for using online lookup')
@docstring(add_citations)
def add_cit(warehouse_home, server_name, cache_dir, resume=False,
            password=None, online=True):
    add_citations(warehouse_home, server_name, cache_dir, resume=resume,
                  password=password, online=online)


@arg('-r', '--resume', help='toggle default for resuming process')
@arg('-o', '--online', help='toggle default for using online lookup')
@docstring(add_metadata)
def add_meta(warehouse_home, server_name, cache_dir, resume=False,
             password=None, online=True):
    add_metadata(warehouse_home, server_name, cache_dir, resume=resume,
                 password=password, online=online)
