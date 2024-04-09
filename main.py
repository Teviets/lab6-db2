from neo4j import GraphDatabase
from py2neo import Graph, Node, Relationship
import fake as fk
import random as rm

uri = "neo4j+s://ff628d69.databases.neo4j.io"
username = "neo4j"
password = "RAAmJuO5zwL3JdWn52oMcLcHrQya8xswbcvxBhIBcsY"

driver = GraphDatabase.driver(uri, auth=(username, password))
graph = Graph(uri, auth=(username, password))

fake = fk.Faker()

lstmvName = ["The Matrix", "The Matrix Reloaded", "The Matrix Revolutions", "The Matrix Resurrections", "Piratas del caribe"]
lstmvId = ["tt0133093", "tt0234215", "tt0242653", "tt10838180", "tt0325980"]
lstmvYear = [1999, 2003, 2003, 2021, 2003]
lstmvPlot = ["A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.",
            "Neo and his allies", "The human city of Zion defends itself against the massive invasion of the machines as Neo fights to end the war at another front while also opposing the rogue Agent Smith.",
            "Return to a world of two realities: one, everyday life; the other, what lies behind it. To find out if his reality is a physical or mental construct, to truly know himself, Mr. Anderson will have to choose to follow the white rabbit once more.",
            "Blacksmith Will Turner teams up with eccentric pirate 'Captain' Jack Sparrow to save his love, the governor's daughter, from Jack's former pirate allies, who are now undead."]
lstusid = ["user1", "user2", "user3", "user4", "user5"]

def runQuery(tx, query):
    result = tx.run(query)
    return result

def createMovie(tx, title, movieid, year, plot):
    tx.run("CREATE (movie:Movie {title: $title, movieid: $movieid, year: $year, plot: $plot})", title=title, movieid=movieid, year=year, plot=plot)

def createUser(tx, name, userid):
    tx.run("CREATE (user:User {name: $name, userid: $userid})", name=name, userid=userid)

def rateMovie(tx, userid, movieid, rating, timestamp):
    tx.run("MATCH (user:User {userid: $userid}) MATCH (movie:Movie {movieid: $movieid}) CREATE (user)-[:RATED {rating: $rating, timestamp: $timestamp}]->(movie)", userid=userid, movieid=movieid, rating=rating, timestamp=timestamp)


def createGraph(tx):
    for i in range (5):
        createMovie(tx, lstmvName[i], lstmvId[i], lstmvYear[i], lstmvPlot[i])
        createUser(tx, fake.name(), lstmvId[i])
        rateMovie(tx, lstmvId[i], lstmvId[i], rm.randint(0, 5), rm.randint(100, 300))


def pushData():
    with driver.session() as session:
        session.write_transaction(createGraph)
        
    print("Graph created")

    driver.close()


def getUser(user):
    query = "MATCH (n:User {name: '" + user + "'}) RETURN n"

    result = graph.run(query)
    return result

def getMovie(movie):
    query = "MATCH (n:Movie {title: '" + movie + "'}) RETURN n"

    result = graph
    return result

def getMovieRatedByUser(user):
    query = "MATCH (u:User {name: '" + user + "'})-[r:RATED]->(m:Movie) RETURN m"

    result = graph.run(query)
    return result

#pushData()

#print(getUser("Guido Warsaw"))
#print(getMovie("The Matrix")
#print(getMovieRatedByUser("Guido Warsaw"))
