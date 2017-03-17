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
           tag_trees,
           ext_rels,
           arts2csv,
           vars2csv,
           rels2csv,
           setup_server,
           toneo,
           ppgraph],
    optional=[remove_server,
              start_server,
              stop_server,
              add_cit,
              add_meta,
              clean,
              clean_cache,
              report])
