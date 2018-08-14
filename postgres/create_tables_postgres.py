import configparser
import psycopg2
import sys
import os


def create_tables():
    '''
    Create tables in the PostgreSQL database
    '''
    commands = (

        "DROP TABLE IF EXISTS real_time;",
        "DROP TABLE IF EXISTS flagged_users;",
        "CREATE TABLE IF NOT EXISTS real_time (rev_id INT PRIMARY KEY,article_id int,article_title varchar(100));",
"CREATE TABLE IF NOT EXISTS wikieditevents (revision varchar(20), article_id int, rev_id int, article_title varchar(1000), edit_timestamp timestamp, username varchar(100), userid int, primary key(rev_id));",
"CREATE TABLE IF NOT EXISTS flagged_users (username varchar(100), edit_timestamp timestamp, editcount int , PRIMARY KEY(username,edit_timestamp));"

    )

    # Read in configuration file

    config = configparser.ConfigParser()
    config.read('configfile.txt')
    print(config)
    postgres_url = 'postgresql://'\
                   + config["postgres"]["user"] + ':' + config["postgres"]["password"]\
                   + '@' + config["postgres"]["host"] + ':' + config["postgres"]["port"] + '/' + config["postgres"]["db"]

    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        # create table one by one
        print("got connection")
        for command in commands:
	    print (command)
            cur.execute(command)
            print("executed command")
        # close communication with the PostgreSQL database server
        cur.close()
        print("closed the cursor")
        # commit the changes
        conn.commit()
        print("committed the connection")
    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            conn.close()
            print("closed the connection")


if __name__ == '__main__':
    create_tables()
