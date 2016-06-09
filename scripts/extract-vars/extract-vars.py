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
           tocsv,
           setup_server,
           toneo,
           ppgraph],
    optional=[clean,
              remove_server,
              start_server,
              stop_server])
