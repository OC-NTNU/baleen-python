"""
citations
"""

import logging
import pickle
from path import Path
import json

import requests

from baleen.n4j import get_session
from neo4j.v1 import ResultError

log = logging.getLogger(__name__)

# silence request logging
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
    logging.WARNING)


def add_citations(warehouse_home, server_name, cache_fname,
                  resume=False, password=None):
    """
    Add citation string to Article nodes

    Parameters
    ----------
    warehouse_home
    server_name
    cache_fname
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

    cache_path = Path(cache_fname)

    if cache_path.exists():
        log.info('reading cached citations from ' + cache_path)
        cache = pickle.load(open(cache_path, 'rb'))
    else:
        log.info('no cached citations found at ' + cache_path)
        cache = {}

    cache_path.dirname().makedirs_p()
    modified = False

    # for rec in records:
    for rec in list(records)[:10]:
        doi = rec['doi']

        try:
            citation = cache[doi]
        except KeyError:
            citation = get_citation(doi)
            cache[doi] = citation
            modified = True

        session.run("""
        MATCH (a:Article)
        WHERE a.doi = {doi}
        SET a.citation = {citation}
        """, {'doi': doi, 'citation': citation})
        log.info('added citation for DOI ' + doi)

    if modified:
        log.info('writing cached citations to ' + cache_path)
        cache = pickle.dump(cache, open(cache_path, 'wb'))


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
        citation = citation.split(' doi:')[0]

    return citation


def add_metadata(warehouse_home, server_name, cache_fname,
                 resume=False, password=None):
    """
    Add citation string to Article nodes

    Parameters
    ----------
    warehouse_home
    server_name
    cache_fname
    resume
    password

    Returns
    -------

    """
    session = get_session(warehouse_home, server_name, password=password)

    if resume:
        query = """
            MATCH (a:Article)
            WHERE a.title is NULL
            RETURN a.doi as doi"""
    else:
        query = """
            MATCH (a:Article)
            RETURN a.doi as doi"""

    records = session.run(query)

    try:
        records.peek()
    except ResultError:
        log.info('no Article nodes without title property')
        return

    cache_path = Path(cache_fname)

    if cache_path.exists():
        log.info('reading cached metadata from ' + cache_path)
        cache = pickle.load(open(cache_path, 'rb'))
    else:
        log.info('no cached metadata found at ' + cache_path)
        cache = {}

    cache_path.dirname().makedirs_p()
    modified = False

    # for rec in records:
    for rec in list(records):
        doi = rec['doi']

        try:
            metadata = cache[doi]
        except KeyError:
            metadata = get_metadata(doi)
            cache[doi] = metadata
            modified = True

        # example fragment:
        # 'published-online': {'date-parts': [[2009, 8, 30]]},
        # 'published-print': {'date-parts': [[1998, 1, 22]]}

        published = (metadata.get('published-online') or
                      metadata.get('published-print') or
                      {})
        try:
            year = published['date-parts'][0][0]
        except:
            year = None

        session.run("""
                MATCH (a:Article)
                WHERE a.doi = {doi}
                SET a.title = {title},
                    a.container_title = {container_title},
                    a.year = {year}
                """, {'doi': doi,
                      'title': metadata.get('title'),
                      'container_title': metadata.get('container-title'),
                      'year': year})
        log.info('added metadata for DOI ' + doi)

    if modified:
        log.info('writing cached metadata to ' + cache_path)
        cache = pickle.dump(cache, open(cache_path, 'wb'))


def get_metadata(doi):
    """
    Get metadata for DOI from CrossRef
    """
    headers = {'Accept': 'application/vnd.citationstyles.csl+json'}
    attempts = 10

    for i in range(attempts):
        response = requests.get('http://dx.doi.org/{}'.format(doi),
                                headers=headers)
        if response.ok:
            break
    else:
        log.error('request for metadata of {} returned {}: {}'.format(
            doi, response.status_code, response.reason))
        return None

    log.info('request for metadata of {} succeeded'.format(doi))

    return response.json()
