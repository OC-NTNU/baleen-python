"""
citations
"""

import logging
import dbm
from path import Path

import requests

from baleen.n4j import get_session
from neo4j.v1 import ResultError

log = logging.getLogger(__name__)

# silence request logging
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
    logging.WARNING)


def add_citations(warehouse_home, server_name, db_fname,
                  resume=False, password=None):
    """
    Add citation string to Article nodes

    Parameters
    ----------
    warehouse_home
    server_name
    db_fname
    resume
    password

    Returns
    -------

    """
    session = get_session(warehouse_home, server_name, password=password)

    if resume:
        query = """
            MATCH (a:Article)
            WHERE a.citation is NULL
            RETURN a.doi as doi"""
    else:
        query = """
            MATCH (a:Article)
            RETURN a.doi as doi"""

    records = session.run(query)

    try:
        records.peek()
    except ResultError:
        log.info('no Article nodes without citation property')
        return

    Path(db_fname).dirname().makedirs_p()

    log.info('reading/writing cached citations from/to ' + db_fname)

    with dbm.open(db_fname, 'c') as cit_db:
        for rec in records:
            doi = rec['doi']

            try:
                citation = cit_db[doi]
            except KeyError:
                citation = get_citation(doi)
                cit_db[doi] = citation

            session.run("""
            MATCH (a:Article)
            WHERE a.doi = {doi}
            SET a.citation = {citation}
            """, {'doi': doi, 'citation': citation})
            log.info('added citation for DOI ' + doi)


def get_citation(doi, style='chicago-fullnote-bibliography',
                 strip_doi=True):
    """
    Get citation string for DOI from CrossRef

    Parameters
    ----------
    doi
    style
    strip_doi

    Returns
    -------

    """
    headers = {'Accept': 'text/bibliography; style={}'.format(style)}
    attempts = 10

    for i in range(attempts):
        response = requests.get('http://dx.doi.org/{}'.format(doi),
                                headers=headers)
        if response.ok:
            break
    else:
        log.error('request for formated citation of {} returned {}: {}'.format(
            doi, response.status_code, response.reason))
        return ''

    log.info('request for formated citation of {} succeeded'.format(doi))

    citation = response.content.decode('utf-8')

    if strip_doi:
        # TODO: won't work for other styles
        citation = citation.split('doi:')[0]

    return citation
