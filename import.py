#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Philipp Grafendorfer
# start: 180517
# end:
"""
the first part is importing twitter data via twitter api to the neo4j database. the second part is importing simulated
data into the database.
unfortunately the twitter api only allows a very limited amount of request per hour. our neo4j database is hosted by
graphenedb.com and gives us 512 MB of storage, 1000 nodes and 10000 relations between those nodes.
"""

import twitter
import pandas as pd
import numpy as np

from py2neo.database import Graph, authenticate
from py2neo.packages.httpstream import http
http.socket_timeout = 9999


# import original data from twitter
# ====================================================================================================
# import twitter api credentials
with open("twitter_api.txt", "r") as file:
    credentials = file.read().splitlines()

credentials = {"consumer_key": credentials[0],
               "consumer_secret": credentials[1],
               "access_token_key": credentials[2],
               "access_token_secret": credentials[3]}

# create the api object
api = twitter.Api(consumer_key=credentials["consumer_key"],
                  consumer_secret=credentials["consumer_secret"],
                  access_token_key=credentials["access_token_key"],
                  access_token_secret=credentials["access_token_secret"])

# import the database credentials
with open("neo4j_pwd.txt", "r") as file:
    pwd = file.read().splitlines()

# authenticate and create the graph object that represents the neo4j db
authenticate("hobby-baeadknhlicigbkehaeddfal.dbs.graphenedb.com:24786"
             , "reader"
             , pwd[0])
graph = Graph("bolt://hobby-baeadknhlicigbkehaeddfal.dbs.graphenedb.com:24786"
              , user="reader"
              , password=pwd[0]
              , bolt=True
              , secure=True
              , http_port=24789
              , https_port=24780
              )

# get the primary node; as a possible starting point we need a user with some but not too much friends;
# a friend in twitter is the recepient of a "FOLLOWS" relation.
primary_user = api.GetUser(screen_name='elonmusk').AsDict()

# generate the depending nodes; all friend nodes from elon musk; 51 users
network_ids = api.GetFriendIDs(screen_name='elonmusk')
# add the primary id
network_ids.append(primary_user['id'])

# generate all twitter nodes in the graph db via cypher query.
for friend in network_ids:

    user_data = api.GetUser(user_id=friend).AsDict()
    user = {'id': "'{}'".format(user_data['id']),
            'name': "'{}'".format(user_data['name']),
            'data_origin': "'twitter'"}
    user_string = str(user).replace("'", "").replace("\\", "/")
    graph.run("CREATE (n:Person {})".format(user_string))

# generate all twitter FOLLOWS relations
# create an iterable so that we can continue the db push after 100 api calls are used
network_ids = iter(network_ids)
for user in network_ids:

    user_friends = api.GetFriendIDs(user_id=user)
    user_connections = iter(set(network_ids).intersection(set(user_friends)))

    for connection in user_connections:

        graph.run("MATCH (a:Person),(b:Person)"
                  "WHERE a.id = '{0}' AND b.id = '{1}'"
                  "CREATE (a)-[r:FOLLOWS]->(b)"
                  "RETURN type(r)".format(user, connection))


# import simulated data
# ====================================================================================================
# generate all nodes with simulated data
sim_data_raw = pd.read_csv("twitter_sim_data.csv", sep=";")
sim_data_raw.drop([sim_data_raw.columns[0], 'firstname', 'lastname'], axis=1, inplace=True)
sim_data = sim_data_raw.iloc[0:500, :]

name = sim_data.iloc[0, 1]

name.lower().replace(",", "_").replace(" ", "")


def adapt_name(string):
    """
    The idea is to map that function on the combined name column of the simulated input data
    :param string: standard string with whitespace like "Elon Musk"
    :return: normalized string like "elon_musk"
    """
    return string.lower().replace(",", "_").replace(" ", "")


# map the adapt_name function on the test.names column
sim_data['name'] = sim_data['test.names'].transform(adapt_name)
sim_data.drop(['test.names'], axis=1, inplace=True)

# create the database nodes from random data
for index, row in sim_data.iterrows():

    user = {'id': "'{}'".format(row['id']),
            'name': "'{}'".format(row['name']),
            'data_origin': "'sim'"}
    user_string = str(user).replace("'", "").replace("\\", "/")
    graph.run("CREATE (n:Person {})".format(user_string))


# generate random edges between twitter and simulated users
# ====================================================================================================
# query all users in db
ids = pd.DataFrame(graph.data("MATCH (n:Person) RETURN n.id"))['n.id']

# sample the origins of the relation
a = np.random.choice(ids, size=5000, replace=True)
# sample the targets of the relation
b = np.random.choice(ids, size=5000, replace=True)

# create 5000 relations; at least thats the idea. in our db we got only 1289 relations for now but thats ok for
# initial testing
for i in range(5000):

    graph.run("MATCH (a:Person),(b:Person)"
              "WHERE a.id = '{0}' AND b.id = '{1}'"
              "CREATE (a)-[r:FOLLOWS]->(b)"
              "RETURN type(r)".format(a[i], b[i]))
