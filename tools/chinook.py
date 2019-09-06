from database import DatabaseHelper

if __name__ == "__main__":

    print("Hello Chinook")
    
    db = DatabaseHelper('./data/chinook.db')
    
    print(db.show_tables())
    print(db.run_query('SELECT * FROM playlist'))
