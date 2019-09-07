import os
import time
import sqlite3
import pandas as pd
import requests
from tools.keyhandler import get_local_auth_key

class DatabaseHelper:
    """
    Handle the opening, printing, querying and command execute of a database.
    
    db : string path and name of database (e.g. './data/mydata.db')
    """

    def __init__(self, db):
        self._check_and_create(db)
        self.db = db
        

    def __repr__(self):
        q0 = "SELECT name FROM sqlite_master WHERE type = 'table';"
        table_names = self.run_query(q0)
        
        for table_name in table_names['name'].values:
            q1 = f"SELECT * FROM {table_name} LIMIT 5;"
            print(self.run_query(q1))

        for table_name in table_names['name'].values:
            q2 = f"SELECT COUNT(*) FROM {table_name};"
            
            print(
                f"Number of {table_name}s:",
                self.run_query(q2).iloc[0, 0]
            )

        return "Printed database: " + self.db
        

    # def __str__(self):
    #     return self.__repr__


    def _check_and_create(self, db):
        """Check if database file exists, and attempt creation if not.
        """
        if os.path.isfile(db):
            print("Database file exists, no further action required.")
        else:
            print("Database file not found. Attempting to create")

            head, tail = os.path.split(db)
            
            if head == '':
                print("No directory supplied. ", end='')
            elif os.path.isdir(head):
                print("Directory exists. ", end='')
            else:
                print("Directory not found. Attempting to create path")
                os.makedirs(head)

        print("Attempting to create database")
        conn = sqlite3.connect(db)
        conn.close()


    def run_query(self, q):
        with sqlite3.connect(self.db) as conn:
            return pd.read_sql(q, conn)


    def run_command(self, c):
        with sqlite3.connect(self.db) as conn:
            conn.execute('PRAGMA foreign_keys = ON;')
            conn.isolation_level = None
            conn.execute(c)


    def show_tables(self):
        q = """
            SELECT
                name,
                type
            FROM sqlite_master
            WHERE type IN ("table","view");
        """
        return self.run_query(q)


class PlaylistDatabase(DatabaseHelper):
    """Create or open database, creating tables if they don't exist. Can
    optionally wipe the database first.
    
    wipe_database : bool
        drops all tables in database and recreates
    """
    def __init__(self, db, wipe_database=False):
        super().__init__(db)

        if wipe_database:
            self._wipe_database()
        
        self._create_database()


    def _create_database(self):

        print("Creating tables in database...")

        self.run_command("""
            CREATE TABLE IF NOT EXISTS artist (
                artist_id INTEGER PRIMARY KEY,
                name TEXT,
                uri TEXT UNIQUE
            );
        """)

        self.run_command("""
            CREATE TABLE IF NOT EXISTS album (
                album_id INTEGER PRIMARY KEY,
                name TEXT,
                release_date TEXT,
                artist_id INTEGER,
                uri TEXT UNIQUE,
                FOREIGN KEY (artist_id) REFERENCES artist(artist_id)
            );
        """)

        self.run_command("""
            CREATE TABLE IF NOT EXISTS track (
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
            );
        """)

        self.run_command("""
            CREATE TABLE IF NOT EXISTS playlist (
                playlist_id INTEGER PRIMARY KEY,
                name VARCHAR,
                uri TEXT UNIQUE
            );
        """)

        self.run_command("""
            CREATE TABLE IF NOT EXISTS playlist_track (
                playlist_id INTEGER,
                track_id INTEGER,
                PRIMARY KEY (playlist_id, track_id)
                FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id),
                FOREIGN KEY (track_id) REFERENCES track(track_id)
            );
        """)

        print("Table creation complete")


    def _wipe_database(self):
        """Drop all tables found in database."""

        print("Wiping database...")

        q = "SELECT name FROM sqlite_master WHERE type = 'table';"
        table_names = self.run_query(q)
        # table_names = ['playlist_track', 'track', 'album', 'artist', 'playlist']
        
        with sqlite3.connect(self.db) as conn:
            # not using run_command so not enforce foreign key constraint
            for table_name in table_names['name'].values:
                conn.isolation_level = None
                conn.execute(f"DROP TABLE {table_name};")

        print("Wiping database complete")


    def _get_playlist_name(self, playlist_id, key):
        """Return just the playlist name from playlist_id."""
        url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
        headers = {"Authorization": "Bearer " + key}
        params = {"fields": "name"}

        response = requests.get(url=url, headers=headers, params=params)
        
        if response.status_code == 200:
            playlist_name = response.json()['name']
        else:
            code = response.status_code
            msg = response.json()['error']['message']
            print(f'Bad response: [{code}] {msg}')
            raise Exception(f"Bad response: [{code}] {msg}\n")

        return playlist_name

    
    def _get_playlist_tracks(self, playlist_id, key):
        """Generator to fetch all tracks from a Spotify playlist."""
        # this only gets called at the start of the generator
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        params = {"offset": 0, "limit": 100} # defaults
        headers = {"Authorization": "Bearer " + key}

        while True:
            response = requests.get(url=url, headers=headers, params=params)
            
            if response.status_code == 200:
                if response.json()['next']:
                    url = response.json()['next']
                    params = None
                else:
                    yield response
                    return # if no next, assume end of playlist
                yield response
            else:
                code = response.status_code
                msg = response.json()['error']['message']
                print(f'Bad response: [{code}] {msg}')
                raise Exception(f"Bad response: [{code}] {msg}\n")


    def _insert_artist(self, track):
        # get relevant artist data
        album_artist = [artist for artist in track['album']['artists']]
        artist_uri = album_artist[0]['id']
        artist_name = album_artist[0]['name']
        
        if len(album_artist) > 1:
            print("More than one album artist exists, using the first listed.",
            "Album name:", track['album']['name'])

        # insert artist data into artist table (if unique)
        self.run_command(f"""
            INSERT OR IGNORE INTO artist (name, uri)
            VALUES ("{artist_name}", "{artist_uri}");
        """)

        # todo: add and return duplicates for albums with multiple artists,
        # so only logged once?

        return artist_uri


    def _insert_album(self, track, artist_uri):
        # get relevant album data
        album_uri = track['album']['id']
        album_name = track['album']['name']
        album_release = track['album']['release_date']

        q1 = f"SELECT artist_id FROM artist WHERE uri = '{artist_uri}'"
        artist_id = self.run_query(q1)
        
        # ensuring new method same as old
        assert artist_id.iloc[0, 0] == int(artist_id.values[0])
        
        artist_id = artist_id.iloc[0, 0]

        # insert album data into album table (if unique)
        self.run_command(f"""
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

        return album_uri


    def _insert_track(self, track, album_uri):
        # get relevant album data
        track_uri = track['id']
        name = str(track['name']).replace('"', '')
        track_number = track['track_number']
        composer = ', '.join(artist['name'] for artist in track['artists'])
        duration = track['duration_ms']
        popularity = track['popularity']
        explicit = track['explicit']

        q2 = f"SELECT album_id FROM album WHERE uri = '{album_uri}'"
        album_id = self.run_query(q2)
        assert album_id.iloc[0, 0] == int(album_id.values[0])
        album_id = album_id.iloc[0, 0]

        # insert track data into track table (if unique)
        self.run_command(f"""
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

        return track_uri


    def _insert_playlist_track(self, track, playlist_id, track_uri):
        # Get ids for playlist_track
        q3 = f"SELECT playlist_id FROM playlist WHERE uri = '{playlist_id}'"
        internal_playlist_id = self.run_query(q3).iloc[0, 0]
        
        q4 = f"SELECT track_id FROM track WHERE uri = '{track_uri}'"
        track_id = self.run_query(q4).iloc[0, 0]
        
        # insert ids into playlist_track
        self.run_command(f"""
            INSERT OR IGNORE INTO playlist_track
            VALUES ({internal_playlist_id}, {track_id});
        """)


    def add_playlist_data(self, playlist_id):
        """Retrieve playlist data from Spotify for playlist_id and insert into
        the connected database.

        playlist_id : str
            can be full uri ('spotify:playlist:3jW9hviT2RIPWP1zDgud5N')
            or just the id ('3jW9hviT2RIPWP1zDgud5N')
        """
        out = None
        if ':' in playlist_id:
            out = playlist_id + ' => '
            playlist_id = playlist_id.split(':')[-1]
            out += playlist_id

        print(f"\nStarting retrieval for playlist: {playlist_id}")
        if out:
            print(f"  Converting playlist uri:", out)

        auth_key_path = './keys'
        auth_key_file = 'auth.txt'

        key = get_local_auth_key(auth_key_path, auth_key_file)

        playlist_name = self._get_playlist_name(playlist_id, key)

        self.run_command(f"""
            INSERT OR IGNORE INTO playlist (name, uri)
            VALUES ("{playlist_name}", "{playlist_id}");
        """)

        print(f"  Added {playlist_name} to playlist table")
        print("  Starting to acquire response data.")

        for response in self._get_playlist_tracks(playlist_id, key):
            print("  Retrieved from:", response.json()['href'][37:])
            print("  Inserting into database...", end='\r')

            tracks = [item['track'] for item in response.json()['items']]

            for track in tracks:
                artist_uri = self._insert_artist(track)
                album_uri = self._insert_album(track, artist_uri)
                track_uri = self._insert_track(track, album_uri)
                self._insert_playlist_track(track, playlist_id, track_uri)

            print("  Successfully inserted response data. Continuing...")
            time.sleep(1)


    def add_multiple_playlists(self, playlist_ids):
        """Retrieve playlist data from Spotify for multiple playlists and
        insert into the connected database.

        playlist_ids : list of str
            strings can be full uri or just the id.
            example:

            playlist_ids = [
                '0raoJZs73KPIdO2dhbed7z',
                'spotify:playlist:3jW9hviT2RIPWP1zDgud5N'
            ]
        """
        for playlist_id in playlist_ids:
            self.add_playlist_data(playlist_id)