import sqlite3
from datetime import datetime

def most_recent_entry():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = "SELECT max(timestamp) FROM driver_rides"
    ret = cur.execute(command).fetchone()
    con.commit()
    con.close()
    return ret[0]

#assigning MOST RECENT DATATIME ENTRY IN DATA SET to MOST_RECENT variable
MOST_RECENT = most_recent_entry()
#print MOST_RECENT


#returns number of days between most recent entry and given time
def time_difference(time):
    d1 = datetime.strptime(MOST_RECENT, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return(d1 - d2).days

#serves the same purpose as time_difference
def td2(t1, t2):
    d1 = datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")
    return(d1 - d2).days

def seconds_elapsed(t1, t2):
    d1 = datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")
    return(d1 - d2).seconds


#get most recent timestamp of a driver
def driver_recent_ride(driver_id):
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = "SELECT max(timestamp) FROM driver_rides WHERE driver_id=? AND event='picked_up_at' "
    ret = cur.execute(command, (str(driver_id),)).fetchone()
    con.commit()
    con.close()
    return ret[0]

#returns array of driver employment duration up to date of latest entry
def all_recent_rides():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = "SELECT driver_id FROM driver_ids"
    driver_ids = cur.execute(command).fetchall()
    con.commit()
    con.close()
    all_differences = []
    for driver_id in driver_ids:
        #print(str(driver_id[0]))
        recent_ride = driver_recent_ride(str(driver_id[0]))
        #adding number of days prior to last entry to list "all_differences"
        if (recent_ride != None):
            difference = int(time_difference(recent_ride))
            all_differences.append(difference)
    '''
    this creates a dictionary to store frequences of days prior to last entry
    but using a set(array) of values was easier for observing data
    
    retention = {}
    print "MAX NUM ="
    print all_differences
    for i in range(len(all_differences)):
        days = all_differences[i]
        if days in retention.keys():
            retention[days] = retention[days]+1
        else:
            retention[days] = 1
    '''
    return all_differences

def driver_first_ride(driver_id):
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = "SELECT min(timestamp) FROM driver_rides WHERE driver_id=? AND event='picked_up_at' "
    ret = cur.execute(command, (str(driver_id),)).fetchone()
    con.commit()
    con.close()
    return ret[0]


#returns array of days driver has been with Lyft since onboarding -- up to driver's lastest entry 
#returns array of days driver has been with Lyft since onboarding -- up to driver's lastest entry 
def driver_total_days():
    #days = []
    dict1 = {}
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = "SELECT driver_id FROM driver_ids"
    driver_ids = cur.execute(command).fetchall()

    for driver_id in driver_ids:
        recent_ride = driver_recent_ride(str(driver_id[0]))
        command = "SELECT driver_onboard_date FROM driver_ids WHERE driver_id='" + driver_id[0] + "'"
        onboard_date = cur.execute(command).fetchall()[0]
     
        #adding number of days prior to last entry to list "all_differences"
        if (recent_ride != None):
            if(onboard_date == None):
                difference = int(td2(recent_ride, driver_first_ride(str(driver_id[0]))))
            else:
                difference = int(td2(recent_ride, onboard_date[0]))
            dict1[driver_id[0]] = difference
            #days.append(difference)
    con.commit()
    con.close()
    return dict1
    #return days

#print driver_total_days()

#RUNNING all_recent_rides() to create retention.csv file

#retention = all_recent_rides()
'''
for i in range(89):
    if i in retention.keys():
        print str(i) +  " : " + str(retention[i])

import csv
with open('retention.csv', 'w') as f:
    for key in retention.keys():
        f.write("%s,%s\n"%(key,retention[key]))
'''


def avg_acceptance_time():
    con = sqlite3.connect("lyft.db")
    cur = con.cursor()
    command = "SELECT driver_id FROM driver_ids"
    driver_ids = cur.execute(command).fetchall()
    acceptance_time = []
    dic = {}
    for driver_id in driver_ids:
        driver_id = driver_id[0]
        command = "SELECT rowid, timestamp FROM driver_rides WHERE driver_id=? AND event='accepted_at'"
        #(rowid, accepted timestamp) list per driver
        result = cur.execute(command, (driver_id,)).fetchall()
        dur_sum = 0
        if (result != []):
            #each (rowid, accepted timestamp) item
            for res in result:
                row = res[0]
                accepted_at = res[1]
                command = "SELECT timestamp FROM driver_rides WHERE rowid=?"
                requested_row = int(row)+4
                requested_at = cur.execute(command, (requested_row,)).fetchone()[0]
                acceptance_time.append(seconds_elapsed(accepted_at, requested_at))
                #dur_sum += seconds_elapsed(accepted_at, requested_at)
        #if (result != []):
            #dic[driver_id] = dur_sum
    con.commit()
    con.close()
    return acceptance_time

#print avg_acceptance_time()
    
    
    

    
