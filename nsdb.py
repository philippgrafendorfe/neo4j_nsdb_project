#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Philipp Grafendorfer
# start: 180517
# end:

import twitter
import pandas as pd
import numpy as np

from py2neo.database import Graph, authenticate


def get_all_friend_ids_from_screen_name(twitter_api, screen_name):
    """
    a friend in twitter is the recepient of a "FOLLOWS" relation.
    :param screen_name: string; should be fetched via twitter web interface
    :param twitter_api: api object
    :return: list of ids from a network with one person in the 'center' of that network
    """
    primary_user = twitter_api.GetUser(screen_name=screen_name).AsDict()
    network_ids = twitter_api.GetFriendIDs(screen_name=screen_name)
    network_ids.append(primary_user['id'])

    return network_ids


def create_twitter_user(user_id, neo4j_graph, twitter_api):
    """
    creates twitter user as node in neo4j
    :param user_id: twitter user id
    :param neo4j_graph: neo4j graph db object
    :param twitter_api: api object
    :return: reates twitter user as node in neo4j
    """
    user_data = twitter_api.GetUser(user_id=user_id).AsDict()
    user = {'id': "'{}'".format(user_data['id']),
            'name': "'{}'".format(user_data['name']),
            'data_origin': "'twitter'"}
    user_string = str(user).replace("'", "").replace("\\", "/")
    neo4j_graph.run("CREATE (n:Person {})".format(user_string))


def create_twitter_friend_relations(user_id, network, neo4j_graph, twitter_api):
    """
    creates all twitter friend relations for a given user in the neo4j graph database
    :param user_id:
    :param neo4j_graph:
    :param twitter_api:
    :param network: list with ids
    :return:
    """
    user_friends = twitter_api.GetFriendIDs(user_id=user_id)
    user_connections = iter(set(network).intersection(set(user_friends)))

    for connection in user_connections:

        neo4j_graph.run("MATCH (a:Person),(b:Person)"
                        "WHERE a.id = '{0}' AND b.id = '{1}'"
                        "CREATE (a)-[r:FOLLOWS]->(b)"
                        "RETURN type(r)".format(user_id, connection))

