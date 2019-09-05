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
    table_names = ['track', 'album', 'artist']

    for table_name in table_names:
        run_command(f"DROP TABLE IF EXISTS {table_name};")

    run_command("""
        CREATE TABLE artist (
            artist_id TEXT PRIMARY KEY,
            name TEXT
        );"""
    )

    run_command("""
        CREATE TABLE album (
            album_id TEXT PRIMARY KEY,
            name TEXT,
            release_date TEXT,
            artist_id TEXT,
            FOREIGN KEY (artist_id) REFERENCES artist(artist_id)
        );"""
    )

    run_command("""
        CREATE TABLE track (
            track_id TEXT PRIMARY KEY,
            name VARCHAR,
            album_id TEXT,
            track_number INTEGER,
            composer TEXT,
            duration INTEGER,
            popularity FLOAT,    
            explicit BOOL,
            FOREIGN KEY (album_id) REFERENCES album(album_id)
        );"""
    )

    print("Recreating database complete")


# retrieve key, eventually generate key
def get_local_auth_key(auth_key_path, auth_key_file):
    """Return authentification key from local text file inside keys/auth.txt"""
    path_to_file = os.path.join(auth_key_path, auth_key_file)
    
    try:
        with open(path_to_file, 'r') as f:
            key = f.readline()
            if isinstance(key, str):
                print('Found auth key, returning')
                return key
            else:
                print('Error retrieving key from file. Check key exists.')
    except FileNotFoundError:
        print('Error finding auth key. Check key exists and check path')

# retrieve playlist tracks
def get_playlist_tracks(playlist_id, key):
    while True:
        time.sleep(1)
        
        try:
            url
            params = None
        except UnboundLocalError:
            # no url, assuming starting from beggining
            url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
            params = {"offset": 2800, "limit": 10} # temp for testing a few

        headers = {"Authorization": "Bearer " + key}

        response = requests.get(url=url, headers=headers, params=params)
        
        if response.status_code == 200:
            if response.json()['next']:
                url = response.json()['next']
            else:
                yield response
                return # raise StopIteration
            yield response
            return # temporary, so just yields once
        else:
            code = response.status_code
            msg = response.json()['error']['message']
            print(f'Bad response: [{code}] {msg}')
            raise Exception(f"Bad response: [{code}] {msg}\n")

# insert into database
def playlist_logic():
    auth_key_path = './keys'
    auth_key_file = 'auth.txt'

    key = get_local_auth_key(auth_key_path, auth_key_file)

    # set playlist id for now
    playlist_id = '0VLaP8vVXSIi1c11Jln1AT'

    print("Beginning to acquire response data.")
    for response in get_playlist_tracks(playlist_id, key):
        print("Retrieved from:", response.json()['href'][37:])
        print("Inserting into database...")

        # insert track data into track table
        tracks = [item['track'] for item in response.json()['items']]

        for track in tracks:
            album_artist = [artist for artist in track['album']['artists']]
            if len(album_artist) == 1:
                album_artist_id = album_artist[0]['id']
                album_artist_name = album_artist[0]['name']
            else:
                raise Exception("More than one album artist:", album_artist)
            
            # print(
            #     type(album_artist_id), album_artist_id,
            #     type(album_artist_name), album_artist_name
            # )

            # insert artist data into artist table (if unique)
            run_command(f"""
            INSERT OR IGNORE INTO artist VALUES
            (
                '{album_artist_id}',
                '{album_artist_name}'
            )
            """)

            album_id = track['album']['id']
            album_name = track['album']['name']
            album_release = track['album']['release_date']

            # print(
            #     type(album_id), album_id,
            #     type(album_name), album_name,
            #     type(album_release), album_release,
            #     # type(album_artist), album_artist,
            #     type(album_artist_id), album_artist_id
            # )

            # insert album data into album table (if unique)
            run_command(f"""
            INSERT OR IGNORE INTO album VALUES
            (
                '{album_id}',
                '{album_name}',
                '{album_release}',
                '{album_artist_id}'
            );
            """)

            track_id = track['id']
            name = track['name']
            track_number = track['track_number']
            composer = ', '.join(artist['name'] for artist in track['artists'])
            duration = track['duration_ms']
            popularity = track['popularity']
            explicit = track['explicit']

            # print(
            #     type(track_id), track_id,
            #     type(name), name,
            #     type(album_id), album_id,
            #     type(track_number), track_number,
            #     type(composer), composer,
            #     type(duration), duration,
            #     type(popularity), popularity,
            #     type(explicit), explicit
            #     )

            run_command(f"""
            INSERT OR IGNORE INTO track VALUES
                ('{track_id}', 
                '{name}', 
                '{album_id}',
                {track_number}, 
                '{composer}',
                {duration},
                {popularity},
                {explicit});
            """)

        print("Successfully inserted response data. Continuing...")
        time.sleep(1)
        
    # retrieve full artist data

    # retrieve full album data

    print("Operation complete. All playlist data retrieved.")


if __name__ == "__main__":
    print("Starting")

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

    playlist_logic()

    print(run_query("SELECT * FROM artist LIMIT 5"))
    print(run_query("SELECT * FROM album LIMIT 5"))
    print(run_query("SELECT * FROM track LIMIT 10"))

    print("Finished")