from neo4j.v1 import GraphDatabase


class FolloweeFinder:
    def __init__(self, uri, authname, password):
        self.uri = uri
        self.driver = GraphDatabase.driver(uri, auth=(authname, password))

    # returns a list of n records consisting of user names and number of links to those users
    # ordered descending by number of links.
    def thirdLevelFollowees(self, name, n):
        result = []
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                for record in tx.run("MATCH (r:User)-[:FOLLOWS]->(u:User)<-[:FOLLOWS]-(s:User)-[:FOLLOWS]->(t:User) "
                                     "WHERE r.name = {name} AND NOT (r)-[:FOLLOWS]->(t) "
                                     "WITH t.name AS name, COUNT(*) AS num "
                                     "RETURN name, num "
                                     "ORDER BY num DESC "
                                     "LIMIT {limit} ", name=name, limit=n):
                    result.append(record)
            session.close()
        return result

    # returns the name of a user, that has similar followers as that user, that 'name' is following
    def makeSuggestion(self, name):
        suggestionRecord = self.thirdLevelFollowees(name, 1)[0]
        return suggestionRecord["name"]

# ------------------------------------------
# usage on my machine:
#
# f = FolloweeFinder('bolt://localhost:7687', 'neo4j', 'twitter')
#
# print(f.thirdLevelFollowees("root", 10))
# print(f.makeSuggestion("root"))
# ------------------------------------------
