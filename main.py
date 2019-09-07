# local imports
from tools.database import PlaylistDatabase

def run_from_user_input():
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


    # todo: own function
    user_input = input("Download playlist data? (y/n) ")
    while user_input not in ['n', 'N']:
        if user_input in ['y', 'Y']:
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

def run_test_individual():

    db = PlaylistDatabase("./data/test.db", wipe_database=True)
    print(db.show_tables())
    
    # TestPlaylist1
    db.add_playlist_data('0raoJZs73KPIdO2dhbed7z')
    # TestPlaylist2
    db.add_playlist_data('spotify:playlist:3jW9hviT2RIPWP1zDgud5N')
    
    print(db)

def run_test_many():
    playlist_ids = [
        '0raoJZs73KPIdO2dhbed7z',
        'spotify:playlist:3jW9hviT2RIPWP1zDgud5N'
    ]

    db = PlaylistDatabase("./data/testmany.db", wipe_database=True)
    db.add_multiple_playlists(playlist_ids)

def main():
    playlist_ids = [
        '57kWvG8nshZ2GhZAxdueLV', # metal mondays
        '0CtvRplUSsxBCO7WmkYESy', # productivity
        '0Z8nSkyBHd40u0R9NkbA8w', # relaxing calm
        '5S7lrzPTK9VaX9kNpsrdeS', # tv & movies
        '5WmaeUCYm7s6WKpj7LKXpd', # game soundtracks
        '0VLaP8vVXSIi1c11Jln1AT'  # saved songs
    ]

    db = PlaylistDatabase("./data/spotify.db")
    db.add_multiple_playlists(playlist_ids)


if __name__ == "__main__":
    
    print("Running tests")
    run_test_individual()
    run_test_many()
    # run_from_user_input()
    print("Tests complete")

    print("Main: Starting")
    main()
    print("Main: Finished")

    # todo: token generation a bit more automated
    
    # todo: comments. comments everywhere