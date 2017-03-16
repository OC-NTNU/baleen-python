#!/usr/bin/env python

"""
pipeline for extracting variables from text
"""

from baleen.pipeline import script
from baleen.steps import *

script(
    steps=[core_nlp,
           lemma_trees,
           ext_vars,
           offsets,
           prep_vars,
           prune_vars,
           vars2csv,
           rels2csv,
           setup_server,
           toneo,
           tag_trees,
           ext_rels,
           add_cit,
           add_meta,
           ppgraph],
    optional=[remove_server,
              start_server,
              stop_server,
              clean,
              clean_cache,
              report])
