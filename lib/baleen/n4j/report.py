"""
Report on Neo4j server and graph
"""

import subprocess
import time
from pathlib import Path

from tabulate import tabulate

from baleen.n4j.server import get_session


def graph_report(warehouse_home, server_name, password, top_n=50):
    """
    Report on graph database
    """
    session = get_session(warehouse_home, server_name, password)

    def tq(query):
        print_table(session.run(query))

    print(time.asctime() + '\n')

    print_section('Database')

    # TODO get info from py2neo
    p = Path(warehouse_home).resolve() / 'run' / server_name
    version = list(p.glob('neo4j*')).pop().name
    print('Neo4j version: ' + version)
    print('Warehouse home: {}'.format(Path(warehouse_home).resolve()))
    print('Server name: ' + server_name)
    print('Url: ' + session.driver.url)
    print('Password protected: {}'.format(True if password else False))
    print('Encrypted: {}'.format(session.driver.encrypted))
    db_path = (Path(warehouse_home).resolve() / 'run' /
               server_name / 'neo4j-community-*/data/databases')
    size = subprocess.check_output('du -hs {}'.format(db_path),
                                   shell=True).decode('utf-8').split('\t')[0]
    print('Database size: ' + size)
    print()

    print_section('Nodes')

    tq("""
        MATCH (n)
        WITH labels(n) AS labels
        UNWIND labels AS NodeLabel
        RETURN DISTINCT NodeLabel, count(*) AS Count
        ORDER BY NodeLabel
        """)

    tq("""
        MATCH (n)
        WITH labels(n) AS NodeLabels
        RETURN DISTINCT NodeLabels, count(*) AS Count
        ORDER BY Count
        """)

    total = list(session.run('MATCH (n) RETURN COUNT(*) AS Total'))[0]['Total']
    print('Total number or nodes: {}\n'.format(total))

    print_section('Relations')

    tq("""
        MATCH () -[r]-> ()
        RETURN DISTINCT TYPE(r) AS Relation, COUNT(*) AS Count
        ORDER BY Relation
        """)

    total = list(session.run('MATCH () -[r]- () RETURN COUNT(*) AS Total'))[0][
        'Total']
    print('Total number or relations: {}\n'.format(total))

    print_section('Articles')

    tq("""
        MATCH (a:Article)
        RETURN DISTINCT a.journal as Journal, COUNT(*) as ArticleCount
        ORDER BY ArticleCount DESC
        """)

    tq("""
        MATCH (a:Article)
        RETURN DISTINCT a.year as Year, COUNT(*) as ArticleCount
        ORDER BY Year DESC
        """)

    tq("""
        MATCH (a:Article)
        RETURN DISTINCT a.publisher as Publisher, COUNT(*) as ArticleCount
        ORDER BY Publisher
        """)

    print_section('Events')

    print('Top {} event types:\n'.format(top_n))

    tq("""
        MATCH (et:EventType) -[:HAS_VAR]-> (v:VariableType)
        WITH
            CASE
                WHEN "IncreaseType" IN labels(et) THEN "Increase"
                WHEN "DecreaseType" IN labels(et) THEN "Decrease"
                ELSE "Change"
            END AS Event,
            v.subStr as Variable,
            et.n as Count
        RETURN Event, Variable, Count
        ORDER BY Count DESC
        LIMIT {top_n}
        """.format(top_n=top_n))

    for event in 'Change', 'Increase', 'Decrease':
        print('\nTop {} {} event type:\n'.format(top_n, event))
        tq("""
            MATCH (et:{event}Type) -[:HAS_VAR]-> (v:VariableType)
            WITH
                v.subStr as Variable,
                et.n as Count
            RETURN Variable, Count
            ORDER BY Count DESC
            LIMIT {top_n}
            """.format(event=event, top_n=top_n))

    print_section('Co-occurrence')

    print('Top {} co-occurring event types:\n'.format(top_n))

    tq("""
        MATCH
            (v1:VariableType) <-[:HAS_VAR]- (ve1:EventType)
            -[r:COOCCURS]->
            (ve2:EventType) -[:HAS_VAR]-> (v2:VariableType)
        WITH
            CASE
                WHEN "IncreaseType" IN labels(ve1) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve1) THEN "Decrease"
                ELSE "Change"
            END AS Event1,
            v1.subStr AS Variable1,

            CASE
                WHEN "IncreaseType" IN labels(ve2) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve2) THEN "Decrease"
                ELSE "Change"
            END AS Event2,
            v2.subStr AS Variable2,

            r.n as Count
        RETURN
            Count,
            Event1,
            Variable1,
            Event2,
            Variable2
            ORDER BY Count DESC
            LIMIT {top_n}
        """.format(top_n=top_n))

    print_section('Causal relations')

    print('Top {} causally related event types:\n'.format(top_n))

    tq("""
        MATCH
            (v1:VariableType) <-[:HAS_VAR]- (ve1:EventType)
            -[r:CAUSES]->
            (ve2:EventType) -[:HAS_VAR]-> (v2:VariableType)
        WITH
            CASE
                WHEN "IncreaseType" IN labels(ve1) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve1) THEN "Decrease"
                ELSE "Change"
            END AS Event1,
            v1.subStr AS Variable1,

            CASE
                WHEN "IncreaseType" IN labels(ve2) THEN "Increase"
                WHEN "DecreaseType" IN labels(ve2) THEN "Decrease"
                ELSE "Change"
            END AS Event2,
            v2.subStr AS Variable2,

            r.n as Count
        RETURN
            Count,
            Event1,
            Variable1,
            Event2,
            Variable2
            ORDER BY Count DESC
            LIMIT {top_n}
        """.format(top_n=top_n))


def print_table(result, headers=None):
    if not headers:
        headers = result.keys()
    print(tabulate([r.values() for r in result], headers))
    print()


def print_section(title):
    print(80 * '=')
    print(title)
    print(80 * '=' + '\n')
