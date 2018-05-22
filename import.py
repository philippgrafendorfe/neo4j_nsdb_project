#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Philipp Grafendorfer
# start:
# end:
"""
import twitter data
"""

import twitter
from py2neo.database import Node, Relationship, Graph, authenticate, NodeSelector

from py2neo.packages.httpstream import http
http.socket_timeout = 9999

with open("twitter_api.txt", "r") as file:
    credentials = file.read().splitlines()

credentials = {"consumer_key": credentials[0],
               "consumer_secret": credentials[1],
               "access_token_key": credentials[2],
               "access_token_secret": credentials[3]}


api = twitter.Api(consumer_key=credentials["consumer_key"],
                  consumer_secret=credentials["consumer_secret"],
                  access_token_key=credentials["access_token_key"],
                  access_token_secret=credentials["access_token_secret"])

# print(api.VerifyCredentials())

with open("neo4j_pwd.txt", "r") as file:
    pwd = file.read().splitlines()

authenticate("hobby-baeadknhlicigbkehaeddfal.dbs.graphenedb.com:24786"
             , "reader"
             , pwd[0])
graph = Graph("bolt://hobby-baeadknhlicigbkehaeddfal.dbs.graphenedb.com:24786",
              user="reader",
              password=pwd[0]
              , bolt=True
              , secure=True
              , http_port=24789
              , https_port=24780
              )

## get the primary node
primary_user = api.GetUser(screen_name='elonmusk').AsDict()


## generate the depending nodes
network_ids = api.GetFriendIDs(screen_name='elonmusk')
## add the primary id
network_ids.append(primary_user['id'])

## generate all nodes
for friend in network_ids:

    user_x_data = api.GetUser(user_id=friend).AsDict()
    user_x = {'id': "'{}'".format(user_x_data['id']), 'name': "'{}'".format(user_x_data['name'])}
    user_x_string = str(user_x).replace("'", "").replace("\\", "/")
    graph.run("CREATE (n:Person {})".format(user_x_string))

## generate all FOLLOWS relations
for user in network_ids:

    user_x_friends = api.GetFriendIDs(user_id=user)
    user_x_connections = list(set(network_ids).intersection(set(user_x_friends)))

    for connection in user_x_connections:

        graph.run("MATCH (a:Person),(b:Person)"
                  "WHERE a.id = '{0}' AND b.id = '{1}'"
                  "CREATE (a)-[r:FOLLOWS]->(b)"
                  "RETURN type(r)".format(user, connection))
