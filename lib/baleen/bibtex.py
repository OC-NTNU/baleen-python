"""
process references in BibTex format
"""

import os.path as path
import logging

from pybtex.database import parse_file

log = logging.getLogger(__name__)



def parse_bibtex_file(fname):
    """
    Parse file containing a single BibTex entry

    Parameters
    ----------
    fname : str
        filename
    Returns
    -------
    dict:
        dict with BibTex entry's fields as keys

    """
    if not path.exists(fname):
        log.error('missing file ' + fname)
        return {}

    try:
        bib_data = parse_file(fname)
        entry = bib_data.entries.values().pop()
        return entry.fields
    except:
        log.error('failure in parsing file ' + fname)
        return {}