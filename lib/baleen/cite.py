"""
citations
"""

import logging
import pickleshare
from path import Path

import requests

from baleen.n4j import get_session
from neo4j.v1 import ResultError

log = logging.getLogger(__name__)

# silence request logging
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
    logging.WARNING)


def add_citations(warehouse_home, server_name, cache_dir,
                  resume=False, password=None, online=True):
    """
    Add citation string to Article nodes

    Parameters
    ----------
    warehouse_home
    server_name
    cache_dir
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

    # The reason to use pickleshare is that it uses a separate pickle file
    # for each key (i.e. doi), so if the process crashes, at least most of the
    # looked up citations/metadata is saved to file
    cache_path = Path(cache_dir)
    cache_path.dirname().makedirs_p()
    log.info('reading cached citations from ' + cache_path)
    cache = pickleshare.PickleShareDB(cache_dir)

    for rec in records:
        doi = rec['doi']
        citation = get_citation(doi, cache, online)

        if citation:
            session.run("""
            MATCH (a:Article)
            WHERE a.doi = {doi}
            SET a.citation = {citation}
            """, {'doi': doi, 'citation': citation})
            log.info('added citation for DOI ' + doi)


def get_citation(doi, cache,
                 style='chicago-fullnote-bibliography',
                 strip_doi=True, online=True):
    """
    Get formatted citation string for DOI from CrossRef

    Parameters
    ----------
    doi
    cache
    style
    strip_doi

    Returns
    -------

    """
    try:
        return cache[doi]
    except KeyError:
        pass

    if not online:
        log.warn('skipping online lookup of citation for DOI {}'.format(doi))
        return

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

    cache[doi] = citation
    return citation


def add_metadata(warehouse_home, server_name, cache_dir,
                 resume=False, password=None, online=True):
    """
    Add article metadata to Article nodes

    Parameters
    ----------
    warehouse_home
    server_name
    cache_dir
    resume
    password

    Returns
    -------

    """
    session = get_session(warehouse_home, server_name, password=password)

    if resume:
        query = """
            MATCH (a:Article)
            WHERE ( a.title is NULL OR
                    a.journal is NULL OR
                    a.publisher is NULL OR
                    a.year is NULL OR
                    a.ISSN is NULL )
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

    cache_path = Path(cache_dir)
    cache_path.dirname().makedirs_p()
    log.info('reading cached metadata from ' + cache_path)
    cache = pickleshare.PickleShareDB(cache_dir)

    for rec in records:
        doi = rec['doi']
        metadata = get_all_metadata(doi, cache, online)

        session.run("""
                MATCH (a:Article)
                WHERE a.doi = {doi}
                SET a.title = {title},
                    a.journal = {journal},
                    a.year = {year},
                    a.month = {month},
                    a.day = {day},
                    a.ISSN = {ISSN},
                    a.publisher = {publisher}
                """, metadata)

        log.info('added metadata for DOI ' + doi)


def get_all_metadata(doi, cache, online=True):
    """
    Get metadata for DOI, including metadata from ISSN
    """
    metadata = get_doi_metadata(doi, cache, online)
    issn = metadata['ISSN']
    if issn:
        issn_metadata = get_issn_metadata(issn, cache, online)
        metadata.update(issn_metadata)
    return metadata


def get_doi_metadata(doi, cache, online=True):
    """
    Get metadata for DOI
    """
    doi_metadata = request_doi_metadata(doi, cache, online=online)
    metadata = {'doi': doi}

    for key in 'title', 'publisher':
        try:
            metadata[key] = doi_metadata[key]
        except KeyError:
            metadata[key] = None
            log.warn('no {} found for DOI {}'.format(key, doi))

    try:
        metadata['ISSN'] = doi_metadata['ISSN'][0]
    except (KeyError, IndexError):
        metadata['ISSN'] = None
        log.warn('no ISSN found for DOI {}'.format(doi))

    try:
        metadata['journal'] = doi_metadata['container-title']
    except KeyError:
        metadata['journal'] = None
        log.warn('no journal (container-title) found for DOI {}'.format(doi))

    try:
        # example fragment:
        # 'published-online': {'date-parts': [[2009, 8, 30]]},
        # 'published-print': {'date-parts': [[1998, 1, 22]]}
        published = (doi_metadata.get('published-online') or
                     doi_metadata.get('published-print') or
                     doi_metadata.get('issued'))
        parts = published['date-parts'][0]
    except:
        parts = []

    try:
        metadata['year'] = parts[0]
    except IndexError:
        metadata['year'] = None
        log.warn('no publication date found for DOI {}'.format(doi))

    try:
        metadata['month'] = parts[1]
    except IndexError:
        metadata['month'] = None

    try:
        metadata['day'] = parts[2]
    except IndexError:
        metadata['day'] = None

    return metadata


def request_doi_metadata(doi, cache, attempts=10, online=True):
    """
    Request metadata for DOI from CrossRef
    """
    try:
        return cache[doi]
    except KeyError:
        pass

    if not online:
        log.warn('skipping online lookup for DOI {}'.format(doi))
        return {}

    headers = {'Accept': 'application/vnd.citationstyles.csl+json'}

    for i in range(attempts):
        response = requests.get('http://dx.doi.org/{}'.format(doi),
                                headers=headers)
        if response.ok:
            break
    else:
        log.error('request for metadata of DOI {} returned {}: {}'.format(
            doi, response.status_code, response.reason))
        return {}

    log.info('request for metadata of DOI {} succeeded'.format(doi))
    metadata = response.json()
    cache[doi] = metadata
    return metadata


def get_issn_metadata(issn, cache, online=True):
    """
    Get metadata for ISSN
    """
    issn_metadata = request_issn_metadata(issn, cache, online=online)
    metadata = {}

    try:
        metadata['journal'] = issn_metadata['title']
    except KeyError:
        pass

    try:
        metadata['publisher'] = issn_metadata['publisher']
    except KeyError:
        pass

    return metadata


def request_issn_metadata(issn, cache, attempts=10, online=True):
    """
    Request metadata for ISSN from CrossRef
    """
    try:
        return cache[issn]
    except KeyError:
        pass

    if not online:
        log.warn('skipping online lookup for ISSN {}'.format(issn))
        return {}

    for i in range(attempts):
        response = requests.get(
            'http://api.crossref.org/journals/{}'.format(issn))
        if response.ok:
            break
    else:
        log.error('request for metadata of ISSN {} returned {}: {}'.format(
            issn, response.status_code, response.reason))
        return {}

    log.info('request for metadata of ISSN {} succeeded'.format(issn))
    message = response.json()['message']
    cache[issn] = message
    return message


def clean_metadata_cache(cache_dir):
    """
    Remove records with None values from metadata cache

    This means that on the next call to add_metadata(),
    new metadata will be requested for the removed records.
    """
    cache_path = Path(cache_dir)
    cache_path.dirname().makedirs_p()
    log.info('cleaning cached metadata from ' + cache_path)
    cache = pickleshare.PickleShareDB(cache_dir)
    to_delete = []

    for key in cache.keys():
        func = get_doi_metadata if '/' in key else get_issn_metadata
        metadata = func(key, cache)
        if None in metadata.values():
            to_delete.append(key)

    for key in to_delete:
        log.info('removing incomplete cached metadata for key {}'.format(key))
        del cache[key]
