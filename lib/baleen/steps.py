from argh import arg

from baleen.arghconfig import docstring

from baleen import scnlp, vars
from baleen.utils import remove_any


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
               annotators='tokenize,ssplit',
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
             annotators=annotators,
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
               annotators=scnlp.ANNOTATORS,
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
def ext_vars(extract_vars_exec, trees_dir, vars_file):
    vars.extract_vars(extract_vars_exec, trees_dir, vars_file)


@docstring(vars.add_offsets)
def offsets(in_vars_file, scnlp_dir, out_vars_file):
    vars.add_offsets(in_vars_file, scnlp_dir, out_vars_file)


@docstring(vars.preproc_vars)
def prep_vars(trans_exec, in_vars_file, out_vars_file, trans_file, prep_file):
    vars.preproc_vars(trans_exec, in_vars_file, out_vars_file, trans_file, prep_file)


@docstring(vars.prune_vars)
def prune_vars(prune_vars_exec, vars_file, pruned_file,
              options=vars.PRUNE_OPTIONS):
    vars.prune_vars(prune_vars_exec, vars_file, pruned_file, options=options)


def clean(dir):
    """
    Clean output
    """
    remove_any(dir)







