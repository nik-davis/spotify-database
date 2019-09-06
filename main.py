# imports
import time
import os

import requests
import pandas as pd
import sqlite3

DB = "./data/spotify.db"

# database helper functions
def run_query(q):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql(q, conn)

def run_command(c):
    with sqlite3.connect(DB) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.isolation_level = None
        conn.execute(c)

def show_tables():
    q = """
    SELECT
        name,
        type
    FROM sqlite_master
    WHERE type IN ("table","view");
    """
    return run_query(q)

# create database if required
def recreate_database():
    print("Recreating database...")
    table_names = ['playlist_track', 'track', 'album', 'artist', 'playlist']

    for table_name in table_names:
        run_command(f"DROP TABLE IF EXISTS {table_name};")

    run_command("""
        CREATE TABLE artist (
            artist_id INTEGER PRIMARY KEY,
            name TEXT,
            uri TEXT UNIQUE
        );"""
    )

    run_command("""
        CREATE TABLE album (
            album_id INTEGER PRIMARY KEY,
            name TEXT,
            release_date TEXT,
            artist_id INTEGER,
            uri TEXT UNIQUE,
            FOREIGN KEY (artist_id) REFERENCES artist(artist_id)
        );"""
    )

    run_command("""
        CREATE TABLE track (
            track_id INTEGER PRIMARY KEY,
            name VARCHAR,
            album_id INTEGER,
            track_number INTEGER,
            composer TEXT,
            duration_ms INTEGER,
            popularity FLOAT,    
            explicit BOOL,
            uri TEXT UNIQUE,
            FOREIGN KEY (album_id) REFERENCES album(album_id)
        );"""
    )

    run_command("""
    CREATE TABLE playlist (
        playlist_id INTEGER PRIMARY KEY,
        name VARCHAR,
        uri TEXT UNIQUE
    );""")

    run_command("""
    CREATE TABLE playlist_track (
        playlist_id INTEGER,
        track_id INTEGER,
        PRIMARY KEY (playlist_id, track_id)
        FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id),
        FOREIGN KEY (track_id) REFERENCES track(track_id)
    );""")

    print("Recreating database complete")


# retrieve key, eventually generate key
def get_local_auth_key(auth_key_path, auth_key_file):
    """Return authentification key from local text file inside keys/auth.txt"""
    path_to_file = os.path.join(auth_key_path, auth_key_file)
    
    try:
        with open(path_to_file, 'r') as f:
            key = f.readline()
            if isinstance(key, str):
                print('  Found auth key, returning')
                return key
            else:
                raise Exception('Error retrieving key from file. Check key exists.')
    except FileNotFoundError:
        print('Error finding auth key. Check key exists and check path')

# retrieve playlist tracks
def get_playlist_tracks(playlist_id, key):

    # this only gets called at the start of the generator
    print("  Setting initial variables")
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    params = {"offset": 0, "limit": 5} # defaults
    headers = {"Authorization": "Bearer " + key}
    # i = 0

    while True:
        # print("Start of while loop for i =", i)
        # i += 1
        # try:
        #     url
        #     params = None
        # except UnboundLocalError:
        #     # no url, assuming starting from beggining
        #     url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        #     params = {"offset": 2800, "limit": 10} # temp for testing a few

        response = requests.get(url=url, headers=headers, params=params)
        
        if response.status_code == 200:
            if response.json()['next']:
                url = response.json()['next']
                params = None
            else:
                yield response
                return # raise StopIteration
            yield response
            # return # temporary, so just yields once
        else:
            code = response.status_code
            msg = response.json()['error']['message']
            print(f'Bad response: [{code}] {msg}')
            raise Exception(f"Bad response: [{code}] {msg}\n")

def get_playlist_name(playlist_id, key):
    url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    headers = {"Authorization": "Bearer " + key}
    params = {"fields": "name"} # defaults

    response = requests.get(url=url, headers=headers, params=params)
    
    if response.status_code == 200:
        playlist_name = response.json()['name']
    else:
        code = response.status_code
        msg = response.json()['error']['message']
        print(f'Bad response: [{code}] {msg}')
        raise Exception(f"Bad response: [{code}] {msg}\n")

    return playlist_name

# insert into database
def playlist_logic(playlist_id):
    print(f"\nStarting retrieval for playlist: {playlist_id}")

    auth_key_path = './keys'
    auth_key_file = 'auth.txt'

    key = get_local_auth_key(auth_key_path, auth_key_file)

    playlist_name = get_playlist_name(playlist_id, key)
        
    run_command(f"""
    INSERT OR IGNORE INTO playlist (name, uri)
    VALUES ("{playlist_name}", "{playlist_id}")""")

    print(f"  Added {playlist_name} to playlist table")
    
    print("  Starting to acquire response data.")

    for response in get_playlist_tracks(playlist_id, key):
        print("  Retrieved from:", response.json()['href'][37:])
        print("  Inserting into database...", end='\r')

        # insert track data into track table
        tracks = [item['track'] for item in response.json()['items']]

        for track in tracks:
            album_artist = [artist for artist in track['album']['artists']]
            
            artist_uri = album_artist[0]['id']
            artist_name = album_artist[0]['name']
            
            if len(album_artist) > 1:
                print("More than one album artist exists, using the first listed. Album name:", track['album']['name'])
                # raise Exception("More than one album artist:\n", album_artist, track['album']['name'])

            # insert artist data into artist table (if unique)
            run_command(f"""
            INSERT OR IGNORE INTO artist (name, uri)
            VALUES ("{artist_name}", "{artist_uri}")""")

            album_uri = track['album']['id']
            album_name = track['album']['name']
            album_release = track['album']['release_date']

            q1 = f"SELECT artist_id FROM artist WHERE uri = '{artist_uri}'"
            artist_id = run_query(q1)
            artist_id = int(artist_id.values[0])

            # insert album data into album table (if unique)
            run_command(f"""
            INSERT OR IGNORE INTO album
            (
                name,
                release_date,
                artist_id,
                uri
            )
            VALUES
            (
                "{album_name}",
                "{album_release}",
                {artist_id},
                "{album_uri}"
            );
            """)

            track_uri = track['id']
            name = str(track['name']).replace('"', '')
            track_number = track['track_number']
            composer = ', '.join(artist['name'] for artist in track['artists'])
            duration = track['duration_ms']
            popularity = track['popularity']
            explicit = track['explicit']

            q2 = f"SELECT album_id FROM album WHERE uri = '{album_uri}'"
            album_id = run_query(q2)
            # print(album_id)
            album_id = int(album_id.values[0])

            # print(
            #     track_uri, name, track_number, composer, duration,
            #     popularity, explicit, album_id
            # )

            # insert track data into track table (if unique)
            run_command(f"""
            INSERT OR IGNORE INTO track 
            (
                name, 
                album_id,
                track_number, 
                composer, 
                duration_ms, 
                popularity, 
                explicit,
                uri
            )
            VALUES
            (
                "{name}", 
                {album_id},
                {track_number}, 
                "{composer}",
                {duration},
                {popularity},
                {explicit},
                "{track_uri}"
            );
            """)

            q3 = f"SELECT playlist_id FROM playlist WHERE uri = '{playlist_id}'"
            internal_playlist_id = run_query(q3)
            internal_playlist_id = int(internal_playlist_id.values[0])
            
            q4 = f"SELECT track_id FROM track WHERE uri = '{track_uri}'"
            track_id = run_query(q4)
            track_id = int(track_id.values[0])
            
            run_command(f"""
            INSERT OR IGNORE INTO playlist_track
            VALUES ({internal_playlist_id}, {track_id});
            """)

        print("  Successfully inserted response data. Continuing...")
        time.sleep(1)
        
    # retrieve full artist data

    # retrieve full album data

    print("  Operation complete. All playlist data retrieved.")

def run_sample_queries():
    print(run_query("SELECT * FROM artist LIMIT 5"))
    print(run_query("SELECT * FROM album LIMIT 5"))
    print(run_query("SELECT * FROM track LIMIT 5"))
    print(run_query("SELECT * FROM playlist LIMIT 5"))
    print(run_query("SELECT * FROM playlist_track LIMIT 5"))

    print(
        run_query("SELECT COUNT(*) AS 'Number of artists' FROM artist"),
        run_query("SELECT COUNT(*) AS 'Number of albums' FROM album"),
        run_query("SELECT COUNT(*) AS 'Number of tracks' FROM track"),
        run_query("SELECT COUNT(*) AS 'Number of playlists' FROM playlist"),
        sep='\n'
    )

def run():
    # todo: all inside run or start function?
    # todo: own function
    if (input("Recreate database? (y/n) ") in ['y', 'Y']):
        user_input = input("WARNING: This will completely wipe the database. Proceed? (yes/no) ")
        while user_input not in ['no', 'NO']:
            if (user_input == "yes"):
                recreate_database()
                print("Current database structure:\n")
                print(show_tables())
                print()
                break
            else:
                print("Unknown input. 'yes' or 'no' only please.")
                user_input = input("WARNING: This will completely wipe the database. Proceed? (yes/no) ")

    # just here for reference
    # Saved Songs playlist
    playlist_id = '0VLaP8vVXSIi1c11Jln1AT'
    # test playlist 1:
    # 0raoJZs73KPIdO2dhbed7z
    # test playlist 2:
    # 3jW9hviT2RIPWP1zDgud5N

    # todo: own function
    user_input = input("Download playlist data? (y/n) ")
    while user_input not in ['n', 'N']:
        if (user_input in ['y', 'Y']):
            playlist_id = input("Input playlist ID: ")
            if playlist_id == 'default':
                playlist_id = '0VLaP8vVXSIi1c11Jln1AT'
            assert len(playlist_id) == 22, "Supplied playlist ID is incorrect length"
            playlist_logic(playlist_id)
            break
        else:
            user_input = input("Download playlist data? (y/n) ")

    # test/sample cases
    # todo: own function
    if input("Show sample? (y/n) ") in ['y', 'Y']:
        run_sample_queries()

def run_test_playlists():
    recreate_database()
    print(show_tables())
    playlist_logic('0raoJZs73KPIdO2dhbed7z')
    playlist_logic('3jW9hviT2RIPWP1zDgud5N')
    run_sample_queries()


if __name__ == "__main__":
    print("Starting")

    # run_test_playlists()
    run()

    print("Finished")

    # todo: separate python files for db creation and downloading data
    # todo: token generation a bit more automated