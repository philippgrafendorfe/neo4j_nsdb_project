# ----------------------------------------------------------------------------------------
# DESCRIPTION
# ----------------------------------------------------------------------------------------
# This file makes a suggestion to a person p of our twitter-like neo4j database
# which person u could be interesting for p to follow.
# The logic used is similar to the "customers who bought this product also bought.."-logic
# known from Amazon.
# ----------------------------------------------------------------------------------------

from py2neo.database import Graph, authenticate

# Module for processing command line parameters.
import argparse

# processing command line parameters
parser = argparse.ArgumentParser(
    description='Makes a suggestion whom a person might want to follow. '
                'Enter --help to get help.')
parser.add_argument(
    '--root', '-r',
    type=str,
    dest='root_id',
    help='id of the person you want to get a followee suggestion for (default: empty string).',
    default="")

parser.add_argument(
    '--limit', '-l',
    type=int,
    dest='limit',
    help='limit of suggestions (default: 10).',
    default=10)

args = parser.parse_args()

# set id of root
root_id = args.root_id

# set limit of suggestions
limit = args.limit

# authenticate and create the graph object that represents the neo4j db
authenticate("hobby-baeadknhlicigbkehaeddfal.dbs.graphenedb.com:24786"
             , "reader"
             , "b.zVpb0oSx8xsJ.HPPfr69GVWg33Ra7")
graph = Graph("bolt://hobby-baeadknhlicigbkehaeddfal.dbs.graphenedb.com:24786"
              , user="reader"
              , password="b.zVpb0oSx8xsJ.HPPfr69GVWg33Ra7"
              , bolt=True
              , secure=True
              , http_port=24789
              , https_port=24780
              )

# All the work is done in this cypher query.
result = []
for record in graph.run("MATCH (r:Person)-[:FOLLOWS]->(u:Person)<-[:FOLLOWS]-(s:Person)-[:FOLLOWS]->(t:Person) "
                        "WHERE r.id = {id} AND NOT (r)-[:FOLLOWS]->(t) "
                        "WITH t.id AS id, COUNT(*) AS number_of_connections "
                        "RETURN id, number_of_connections "
                        "ORDER BY number_of_connections DESC "
                        "LIMIT {limit} ", id=root_id, limit=limit):
    result.append(record)

print(result)
