from neo4j import GraphDatabase
import fake as fk
import random as rm

uri = "neo4j+s://ff628d69.databases.neo4j.io"
username = "neo4j"
password = "RAAmJuO5zwL3JdWn52oMcLcHrQya8xswbcvxBhIBcsY"

driver = GraphDatabase.driver(uri, auth=(username, password))

fake = fk.Faker()

lstmvName = ["The Matrix", "The Matrix Reloaded", "The Matrix Revolutions", "The Matrix Resurrections"]
lstmvId = ["tt0133093", "tt0234215", "tt0242653", "tt10838180"]
lstmvYear = [1999, 2003, 2003, 2021]
lstmvPlot = ["A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.",
            "Neo and his allies", "The human city of Zion defends itself against the massive invasion of the machines as Neo fights to end the war at another front while also opposing the rogue Agent Smith.",
            "Return to a world of two realities: one, everyday life; the other, what lies behind it. To find out if his reality is a physical or mental construct, to truly know himself, Mr. Anderson will have to choose to follow the white rabbit once more."]
lstusid = ["user1", "user2", "user3", "user4", "user5"]

def runQuery(tx, query):
    result = tx.run(query)
    return result

"""
necesito crear un grafo en el que 
esten los siguientes nodos:

- User:
    name: string
    userid: string

- Movie:
    title: string
    movieid: string
    year: int
    plot: string

en los que user rated movie con las siguientes propiedades
rating: int de 0-5
timestamp: int
"""

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

with driver.session() as session:
    session.write_transaction(createGraph)
    result = session.read_transaction(runQuery, "MATCH (n) RETURN n")
    for record in result:
        print(record)




driver.close()

