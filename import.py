#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Philipp Grafendorfer
# start:
# end:
"""
import twitter data
"""

import twitter
from py2neo.database import Node, Relationship

api = twitter.Api(consumer_key='Ievh0b33YTmj0ozWPle0nkGpv',
                  consumer_secret='f9JOjYhCWkLV0ZcmhsRS8gteKWebP8OPHyFxLCSAc3sYDy5gzR',
                  access_token_key='16929023-PBlYF3rWKksJVfrdMU0r4jhQyruVLqo74rovQLjiO',
                  access_token_secret='ggADQp1PErXNmClabhbrs1nxxR2gorwtl0h8202V9ipqQ')

print(api.VerifyCredentials())

user = 'Philipp_G'
statuses = api.GetUserTimeline(screen_name=user)

print([s.text for s in statuses])

users = api.GetFriends()
users[0]
users[0].name

