from neo4j import GraphDatabase
from py2neo import Graph, Node, Relationship


uri = "neo4j+s://c129e070.databases.neo4j.io"
username = "neo4j"
password = "2hke53i8ss-TGNiwVHdyvqjFUI9gFqwH-F0tG8BF-Oo"

driver = GraphDatabase.driver(uri, auth=(username, password))
graph = Graph(uri, auth=(username, password))



def runQuery(tx, query):
    result = tx.run(query)
    return result

def createPerson(tx, name, tmdbid, born, died, bio, poster):
    tx.run("CREATE (p:Person {name: $name, tmdbid: $tmdbid, born: $born, died: $died, bio: $bio, poster: $poster})", 
           name=name, tmdbid=tmdbid, born=born, died=died, bio=bio, poster=poster)

def createActor(tx, name, tmdbid, born, died, bio, poster):
    tx.run("CREATE (a:Actor {name: $name, tmdbid: $tmdbid, born: $born, died: $died, bio: $bio, poster: $poster})", 
           name=name, tmdbid=tmdbid, born=born, died=died, bio=bio, poster=poster)

def createDirector(tx, name, tmdbid, born, died, bio, poster):
    tx.run("CREATE (d:Director {name: $name, tmdbid: $tmdbid, born: $born, died: $died, bio: $bio, poster: $poster})", 
           name=name, tmdbid=tmdbid, born=born, died=died, bio=bio, poster=poster)

def createUser(tx, name, userid):
    tx.run("CREATE (u:User {name: $name, userid: $userid})", name=name, userid=userid)
def createMovie(tx, title, tmdbid, released, imdbRating, movieid, year, imdbid, runtime, countries, imdbVotes, url, revenue, plot, poster, budget, languages):
    tx.run("CREATE (m:Movie {title: $title, tmdbid: $tmdbid, released: $released, imdbRating: $imdbRating, movieid: $movieid, year: $year, imdbid: $imdbid, runtime: $runtime, countries: $countries, imdbVotes: $imdbVotes, url: $url, revenue: $revenue, plot: $plot, poster: $poster, budget: $budget, languages: $languages})", 
           title=title, tmdbid=tmdbid, released=released, imdbRating=imdbRating, movieid=movieid, year=year, imdbid=imdbid, runtime=runtime, countries=countries, imdbVotes=imdbVotes, url=url, revenue=revenue, plot=plot, poster=poster, budget=budget, languages=languages)

def createGenre(tx, name):
    tx.run("CREATE (:Genre {name: $name})", name=name)

def createActedInRelation(tx, person_name, movie_title, role):
    tx.run("MATCH (p:Person {name: $person_name}), (m:Movie {title: $movie_title}) "
           "CREATE (p)-[:ACTED_IN {role: $role}]->(m)", 
           person_name=person_name, movie_title=movie_title, role=role)

def createDirectedRelation(tx, person_name, movie_title, role):
    tx.run("MATCH (p:Person {name: $person_name}), (m:Movie {title: $movie_title}) "
           "CREATE (p)-[:DIRECTED {role: $role}]->(m)", 
           person_name=person_name, movie_title=movie_title, role=role)

def createRatedRelation(tx, user_name, movie_title, rating, timestamp):
    tx.run("MATCH (u:User {name: $user_name}), (m:Movie {title: $movie_title}) "
           "CREATE (u)-[:RATED {rating: $rating, timestamp: $timestamp}]->(m)", 
           user_name=user_name, movie_title=movie_title, rating=rating, timestamp=timestamp)

def createInGenreRelation(tx, movie_title, genre_name):
    tx.run("MATCH (m:Movie {title: $movie_title}), (g:Genre {name: $genre_name}) "
           "CREATE (m)-[:IN_GENRE]->(g)", 
           movie_title=movie_title, genre_name=genre_name)

def createGraph(tx):
    # Crear nodos de personas
    createPerson(tx, "Keanu Reeves", 6384, "1964-09-02", None, "Keanu Charles Reeves is a Canadian actor, musician, film producer and director.", "keanu.jpg")
    createPerson(tx, "Lana Wachowski", 7879, "1965-06-21", None, "Lana Wachowski is an American film director, screenwriter, and producer.", "lana.jpg")
    createUser(tx, "User1", 1)
    # Crear nodos de actores
    createActor(tx, "Keanu Reeves", 6384, "1964-09-02", None, "Keanu Charles Reeves is a Canadian actor, musician, film producer and director.", "keanu.jpg")

    # Crear nodos de directores
    createDirector(tx, "Lana Wachowski", 7879, "1965-06-21", None, "Lana Wachowski is an American film director, screenwriter, and producer.", "lana.jpg")

    # Crear nodos de películas y géneros (ya definidos anteriormente)
    createMovie(tx, "The Matrix", 603, "1999-03-31", 8.7, 603, 1999, 603, 136, ["USA", "Australia"], 17813333, "http://www.whatisthematrix.com", 463517383, "A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.", "matrix.jpg", 63000000, ["English"])

    createGenre(tx, "Action")
    createGenre(tx, "Sci-Fi")

    # Establecer relaciones entre personas (actores y directores) y películas
    createActedInRelation(tx, "Keanu Reeves", "The Matrix", "Neo")
    createDirectedRelation(tx, "Lana Wachowski", "The Matrix", "Director")

    # Establecer relaciones entre usuarios y películas
    createRatedRelation(tx, "User1", "The Matrix", 5, 1642034512)

    # Establecer relaciones entre películas y géneros
    createInGenreRelation(tx, "The Matrix", "Action")
    createInGenreRelation(tx, "The Matrix", "Sci-Fi")


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

pushData()
