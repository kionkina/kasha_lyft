import csv, sqlite3

#create lyft.db 

def populate_driver_ids():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS  driver_ids (driver_id, driver_onboard_date);") # use your column names here

    with open('driver_ids.csv','rb') as fin: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['driver_id'], i['driver_onboard_date']) for i in dr]

    cur.executemany("INSERT INTO driver_ids (driver_id, driver_onboard_date) VALUES (?, ?);", to_db)
    con.commit()
    con.close()


def populate_ride_timestamps():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ride_timestamps (ride_id, event, timestamp);")
    with open('ride_timestamps.csv','rb') as fin: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['ride_id'], i['event'], i['timestamp']) for i in dr]

    print(to_db[0])   

    cur.executemany("INSERT INTO ride_timestamps (ride_id, event, timestamp) VALUES (?, ?, ?);", to_db)
    con.commit()
    con.close()


def populate_ride_ids():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ride_ids (driver_id, ride_id);")
    with open('ride_ids.csv','rb') as fin: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['driver_id'], i['ride_id']) for i in dr]

    print(to_db[0])   

    cur.executemany("INSERT INTO ride_ids (driver_id, ride_id) VALUES (?, ?);", to_db)
    con.commit()
    con.close()

#CREATES DRIVER_RIDES TABLE
def join_tables():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = '''SELECT driver_id, event, timestamp
                 FROM ride_ids d
                 LEFT JOIN ride_timestamps rt
                 ON d.ride_id = rt.ride_id
             '''
    #generates dataset: (driver_id, event, timestamp)
    result = cur.execute(command).fetchall()
    cur.execute("CREATE TABLE IF NOT EXISTS  driver_rides (driver_id, event, timestamp);")
    for res in result:
        to_db = (res[0], res[1], res[2])
        cur.execute("INSERT INTO driver_rides (driver_id, event, timestamp) VALUES(?, ?, ?)", to_db)
    print("done")
    con.commit()
    con.close()


#populate_driver_ids()
#populate_ride_timestamps()
#populate_ride_ids()
#join_tables()
