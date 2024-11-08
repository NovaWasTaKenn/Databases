from cassandra.cluster import Cluster
import json

# Connexion à Cassandra
cluster = Cluster(['127.0.0.2'])  # Assurez-vous que l'adresse IP est correcte
session = cluster.connect('basket')  # Assurez-vous d'utiliser la bonne keyspace

# Lecture du fichier JSON des données des joueurs de basketball
with open('NoSql/TP2-Cassandra/Basketball_women.json', 'r') as f:
    players_data = json.load(f)

# Suppression des tables si elles existent
drop_table_player = "DROP TABLE IF EXISTS players"
session.execute(drop_table_player)

drop_table_performance = "DROP TABLE IF EXISTS player_performance"
session.execute(drop_table_performance)

drop_table_award = "DROP TABLE IF EXISTS player_awards"
session.execute(drop_table_award)

drop_table_performance_with_name = "DROP TABLE IF EXISTS player_performance_with_name"
session.execute(drop_table_performance_with_name)




# Création des tables
create_players_table_query = """
CREATE TABLE IF NOT EXISTS players (
    player_id text PRIMARY KEY,
    first_name text,
    middle_name text,
    last_name text,
    full_given_name text,
    pos text,
    height double,
    weight int,
    college text,
    birth_date date,
    birth_city text,
    birth_country text,
    high_school text,
    hs_city text,
    hs_state text,
    hs_country text
)
"""

create_player_performance_table_query = """
CREATE TABLE IF NOT EXISTS player_performance (
    player_id text,
    year int,
    team_id text,
    games int,
    minutes int,
    points int,
    steals int,
    blocks int,
    PRIMARY KEY (player_id, year, team_id),
)
"""

create_player_awards_table_query = """
CREATE TABLE IF NOT EXISTS player_awards (
    player_id text,
    award text,
    year int,
    PRIMARY KEY (player_id, award, year),
)
"""

#Creation d'un table perfomance avec les colonnes player_id,full_given_name,year,team_id,games,minutes,points,steals,blocks
create_player_performance_table_query2 = """
CREATE TABLE IF NOT EXISTS player_performance_with_name (
    player_id text,
    full_given_name text,
    year int,
    team_id text,
    games int,
    minutes int,
    points int,
    steals int,
    blocks int,
    PRIMARY KEY (player_id, year, team_id),
)
"""

# Execute table creation queries
session.execute(create_players_table_query)
session.execute(create_player_performance_table_query)
session.execute(create_player_awards_table_query)
session.execute(create_player_performance_table_query2)

# Insertion des données des joueurs de basketball dans la base de données
for player in players_data:
    # Insertion dans la table des joueurs
    query_player = """
    INSERT INTO players (player_id, first_name, middle_name, last_name, full_given_name, pos, height, weight, college, birth_date, birth_city, birth_country, high_school, hs_city, hs_state, hs_country)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query_player, (player['_id'], player['firstName'], player.get('middleName', None), player['lastName'], player['fullGivenName'], player['pos'], player['height'], player['weight'], player['college'], player['birthDate'], player['birthCity'], player['birthCountry'], player['highSchool'], player['hsCity'], player.get('hsState', None), player['hsCountry']))
    
    # Insertion dans la table des performances des joueurs
    for performance in player.get('players_teams', []):
        query_performance = """
        INSERT INTO player_performance (player_id, year, team_id, games, minutes, points, steals, blocks)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query_performance, (player['_id'], performance['year'], performance['tmID'], performance['games'], performance['minutes'], performance['points'], performance['steals'], performance['blocks']))

    # Insertion dans la table des récompenses des joueurs
    for award in player.get('awards_players', []):
        query_award = """
        INSERT INTO player_awards (player_id, award, year)
        VALUES (%s, %s, %s)
        """
        session.execute(query_award, (player['_id'],award['award'], award['year']))

    # Insertion dans la table des performances des joueurs avec le nom complet
    for performance in player.get('players_teams', []):
        query_performance = """
        INSERT INTO player_performance_with_name (player_id, full_given_name, year, team_id, games, minutes, points, steals, blocks)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query_performance, (player['_id'], player['fullGivenName'], performance['year'], performance['tmID'], performance['games'], performance['minutes'], performance['points'], performance['steals'], performance['blocks']))

    
 
    

print("Données des joueurs de basketball insérées avec succès.")
