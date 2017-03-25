"""
Graph post-processing
"""

import logging

from baleen.cite import get_cache, get_citation, get_all_metadata
from baleen.n4j.server import get_session

log = logging.getLogger(__name__)


def postproc_graph(warehouse_home, server_name, password=None):
    """
    Post-process graph after import

    Parameters
    ----------
    warehouse_home : str
        directory of neokit warehouse containing all neokit server instances
    server_name : str
        name of neokit server instance
    password : str
    """
    session = get_session(warehouse_home, server_name, password)
    prune_tentails(session)
    create_constraints(session)
    create_event_types(session)
    create_cooccurs_relations(session)
    create_causes_relations(session)
    session.close()


def prune_tentails(session):
    log.info('start pruning of tentailed variables')

    result = session.run("MATCH (v:VariableType) RETURN count(*) as Count")
    start_count = list(result)[0]['Count']

    log.info("delete TENTAILS_VAR edge duplicates")
    # TODO: fix the upstream cause of these duplicates

    query1 = """
    MATCH
        (v1)-[r:TENTAILS_VAR]->(v2)
    WITH
        v1, v2, TAIL (COLLECT (r)) as duplicates
    FOREACH
        (r IN duplicates | DELETE r)
    """

    run_write_query(session, query1)

    # Iteratively remove VariableType nodes at the end of a tentailment chain,
    # unless they occur in observed events (i.e. have a -[:HAS_VAR]- relation).
    query2 = """
    MATCH
        (v:VariableType)
    WHERE
        size((v)-->()) = 0 AND NOT (:EventInst)-[:HAS_VAR]->(v)
    WITH
        v LIMIT 25000
    DETACH DELETE
        v
    """

    # Iteratively delete non-branching tentailed variables nodes
    #
    # For example, in
    #
    #     (global marine primary production) -[:TENTAILS_VAR]->
    #     (marine primary production) -[:TENTAILS_VAR]->
    #     (primary production) -[:TENTAILS_VAR]->
    #     (production)
    #
    # The two nodes in the middle are deleted, unless they have tentailed
    # relations to other nodes (i.e. more than one -[:TENTAILS_VAR]- relation),
    # or occur in observed events (i.e. have a -[:HAS_VAR]- relation).

    query3 = """
    MATCH
        (v1:VariableType) -[:TENTAILS_VAR]-> (v2:VariableType) -[:TENTAILS_VAR]-> (v3:VariableType)
    WHERE
        size((v2)--()) = 2 AND (size((v1)--()) > 2 OR  (:EventInst)-[:HAS_VAR]->(v1))
    WITH
        DISTINCT v1, v2, v3 LIMIT 25000
    DETACH DELETE
            v2
    MERGE
        (v1) -[:TENTAILS_VAR]-> (v3)
    """

    # Pruning is performed iteratively because the result of one pruning operation often creates the
    # right context for another application of the same operation.
    deletion_count = None

    # Repeat queries until no more deletions occur.
    while deletion_count != 0:
        deletion_count = 0
        log.info('pruning tentailed VariableType nodes')
        deletion_count += iterative_deletion(session, query2)
        log.info('removing non-branching tentailed variables nodes')
        deletion_count += iterative_deletion(session, query3)

    result = session.run("MATCH (v:VariableType) RETURN count(*) as Count")
    end_count = list(result)[0]['Count']

    log.info('pruned {:,} VarType nodes, from {:,} to {:,} '.format(start_count - end_count, start_count, end_count))
    log.info('end pruning of tentailed variables')


def create_constraints(session):
    # --------------------------------------------------------------------------
    # Constraints
    # --------------------------------------------------------------------------
    # Create a unique property constraint on the label and property combination.
    # If any other node with that label is updated or created with a property
    # that already exists, the write operation will fail.
    # This constraint will create an accompanying index.
    # See http://neo4j.com/docs/stable/query-constraints.html

    constraints = {
        'Article(doi)',
        'Sentence(sentID)',
        'EventInst(eventID)',
        'ChangeInst(eventID)',
        'IncreaseInst(eventID)',
        'DecreaseInst(eventID)',
        'VariableType(subStr)'
    }

    for elem in constraints:
        node, rest = elem.split('(')
        prop, _ = rest.split(')')
        log.info('Creating uniqueness constraint on ' + elem)
        session.run("""
    CREATE CONSTRAINT ON (n:{node})
    ASSERT n.{prop} IS UNIQUE
    """.format(node=node, prop=prop))

    # TODO: replace by db.awaitIndex, once that plugin actually works
    online = None

    while online != constraints:
        result = session.run("CALL db.indexes")
        online = set(r['description'].split(':')[-1] for r in result if r['state'] == 'online')
        log.info('Constraints online: {}'.format(online))

    log.info('All constraints online')


def create_event_types(session):
    # For each changing/increasing/decreasing VariableType,
    # create a ChangeType/IncreaseType/DecreaseType node and
    # connect them with an HAS_VAR relation.
    # EventType nodes are therefore not unique.
    # The redundant "direction" property is to facilitate matching of
    # IncreaseInst to IncreaseType nodes, DecreaseInst to DecreaseType nodes, etc.
    # Python string formatting is used because labels can not be parametrized
    # in Cypher:
    # http://stackoverflow.com/questions/24274364/in-neo4j-how-to-set-the-label-as-a-parameter-in-a-cypher-query-from-java

    log.info('creating event aggregation nodes')

    for event in 'Change', 'Increase', 'Decrease':
        log.info('creating {}Type nodes'.format(event))
        query = """
            MATCH
                (v:VariableType) <-[:HAS_VAR]- (ei:{event}Inst)
            WITH DISTINCT v
            MERGE
                (v) <-[:HAS_VAR]- (:EventType:{event}Type {{direction: "{direction}" }})""".format(
            event=event, direction=event.lower())
        run_write_query(session, query)

        log.info('computing {}Inst counts'.format(event))
        query = """
            MATCH
                (et:{event}Type) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (:{event}Inst)
            WITH
                et, count(*) AS n
            SET
                et.n = n""".format(
            event=event, direction=event.lower())
        run_write_query(session, query)

        # The implementation above does not look like a natural solution in Cypher.
        # However, for reasons I don't understand, implementations like the one below are incredibly slow
        # (i.e. do not terminate even after running for several hours).

        # for event in 'Change', 'Increase', 'Decrease':
        #     run_write_query(session, """
        #     MATCH
        #         (v:VariableType) <-[:HAS_VAR]- (ei:{event}Inst)
        #     MERGE
        #         (v) <-[:HAS_VAR2]- (et:EventType:{event}Type)
        #     ON CREATE SET
        #         et.n = 1,
        #         et.direction = ei.direction
        #     ON MATCH SET
        #         et.n = et.n + 1
        #     """.format(event=event))


def create_cooccurs_relations(session):
    # Compute how many times a combination of ChangeType/IncreaseType/DecreaseType & VariableType
    # co-occurs in the same sentence.
    # The id(et1) < id(et2) statement prevents counting co-occurence twice (because matching is symmetrical).
    # Store co-occurrence count on a new COOCCURS relation between event types.
    log.info('creating COOCCURS relations')

    run_write_query(session, """
        MATCH
            (et1:EventType) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (ei1:EventInst)
            <-[:HAS_EVENT]- (s:Sentence) -[:HAS_EVENT]->
            (ei2:EventInst) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (et2:EventType)
        WHERE
            et1.direction = ei1.direction AND
            et2.direction = ei2.direction AND
            id(et1) < id(et2)
        WITH
            et1, et2, count(*) AS n
        MERGE
            (et1) -[:COOCCURS {n: n}]-> (et2)
    """)


def create_causes_relations(session):
    # Compute how many times a combination of ChangeType/IncreaseType/DecreaseType & VariableType
    # is connected by a CausationInst.
    # Store count on a new CAUSES relation between event types.
    log.info('creating CAUSES relations')

    session.run("""
         MATCH
             (et1:EventType) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (ei1:EventInst)
             <-[:HAS_CAUSE]- (:CausationInst) -[:HAS_EFFECT]->
             (ei2:EventInst) -[:HAS_VAR]-> (:VariableType) <-[:HAS_VAR]- (et2:EventType)
         WHERE
             et1.direction = ei1.direction AND
             et2.direction = ei2.direction
         WITH
             et1, et2, count(*) AS n
         MERGE
             (et1) -[:CAUSES {n: n}]-> (et2)
     """)


def iterative_deletion(session, query, counter_name='nodes_deleted'):
    deletion_count = None
    deletion_count_total = 0
    iteration_count = 0
    log.info('entering iterative deletion with statement ' + query)

    while deletion_count != 0:
        iteration_count += 1
        summary = run_write_query(session, query)
        deletion_count = getattr(summary.counters, counter_name)
        deletion_count_total += deletion_count
        log.info('{:,} deleted after iteration {}'.format(deletion_count_total, iteration_count))

    return deletion_count_total


def run_write_query(session, query):
    result = session.run(query)
    summary = result.consume()
    log.info(summary_report(summary))
    return summary


def summary_report(summary, prefix='\n', hide_attrib={'statement', 'statement_type'}):
    lines = []

    for attrib in dir(summary):
        if not attrib.startswith('__') and attrib not in hide_attrib:
            value = getattr(summary, attrib)
            if value:
                lines.append('{}: {}'.format(attrib, value))

    return prefix + '\n'.join(lines)


# Old code for adding metadata and citation directly to Article nodes in a graph.
# Superseded by csv import.

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
    online

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
    except:  # ResultError:
        log.info('no Article nodes without citation property')
        return

    cache = get_cache(cache_dir)

    for rec in records:
        doi = rec['doi']
        citation = get_citation(doi, cache, online=online)

        if citation:
            session.run("""
            MATCH (a:Article)
            WHERE a.doi = {doi}
            SET a.citation = {citation}
            """, {'doi': doi, 'citation': citation})
            log.info('added citation for DOI ' + doi)


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
    online

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
    except:  # ResultError:
        log.info('no Article nodes without title property')
        return

    cache = get_cache(cache_dir)

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
