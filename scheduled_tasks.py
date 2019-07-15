#!/usr/bin/python
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import os
import sqlite3
import smtplib
import ssl

miseq_path = r"Fake_MiSeq"
t_drive_path = r"Fake_T_Drive"

# if Run_DB.sqlite isn't in the DB folder the db_create.py script must be run first in order for this path to work
db_path = r"DB\Run_DB.sqlite"

# initializing the scheduler
scheduler = BlockingScheduler()

# port, server and other info for sending email
port = 465
smtp_server = "smtp.gmail.com"
sender_email = "emailtestforkm@gmail.com"
recipient_email = "krystal.manke@gmail.com"
password = "pink92cap"
# context for sending email
context = ssl.create_default_context()


# Sends an email when a completed run isn't present in the incomplete table
def send_email(run):
    message = """Subject: Error in Transfer
    
    An error has occurred when transferring a completed run to the completed table. Run """ + str(run) + """ is not 
    present in the incomplete table and therefore will not be transferred to the complete table."""

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, recipient_email, message)

    return print(str(run) + " isn't in the incomplete table! It will not be added to the complete table or the T drive!")


# Adds runs to the MiSeq
def add_miseq():
    # generate run id - run ids are a combination of the current date and time (down to the minute) since I didn't want
    # to risk random generation producing two of the same ids
    # Since runs are only added once per minute each id is unique
    date = "".join(str(datetime.datetime.now()).split(" ")[0].split("-"))
    time = "".join(str(datetime.datetime.now()).split(" ")[-1].split(".")[0].split(":")[0:2])
    id_num = date + time
    # new runs are automatically labelled as incomplete
    new_date = id_num + "_incomplete"
    # create new run file
    with open(os.path.join(miseq_path, new_date + ".txt"), 'w') as new_run:
        new_run.write("new run\n" + id_num)
    new_run.close()
    return print("New run added!")


# Completes old runs on MiSeq
def complete_miseq():
    # get the current date and time for comparison to the run ids
    date = "".join(str(datetime.datetime.now()).split(" ")[0].split("-"))
    time = "".join(str(datetime.datetime.now()).split(" ")[-1].split(".")[0].split(":")[0:2])
    # traverses miseq directory and completes old runs
    for _,_,files in os.walk(miseq_path):
        for file in files:
            id_num = file.split("_")[0]
            status = file.split("_")[-1].split(".")[0]
            # criteria for completion: if the run id does not match the current date and time
            if (id_num != date + time) and (status == "incomplete"):
                # file is renamed with a "complete" label
                new_name = id_num + "_complete.txt"
                os.rename(os.path.join(miseq_path, file), os.path.join(miseq_path, new_name))
    return print("Old runs completed!")


# Checks MiSeq for runs
# Sorts runs into complete and incomplete
def check_miseq():
    # traverses miseq directory for complete/incomplete runs
    complete_runs = []
    incomplete_runs = []
    for _, _, files in os.walk(miseq_path):
        for file in files:
            # checks whether the file has the "incomplete" or the "complete" label
            status = file.split("_")[-1].split(".")[0]
            if status == "complete":
                complete_runs.append(file.split("_")[0])
            elif status == "incomplete":
                incomplete_runs.append(file.split("_")[0])
    return [complete_runs, incomplete_runs]


# Adds incomplete runs to incomplete table
def incomplete_store():
    # establishes connection to database file
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # lists the ids of the incomplete files found
    incomp = check_miseq()[1]
    print("incomplete: " + str(incomp))
    for i in incomp:
        # check if id is already in table
        c.execute('''SELECT * FROM incomplete WHERE run_id=?''', (i,))
        results = c.fetchall()
        # Add run if not already in table
        if len(results) == 0:
            # adds id, and the date and time at which the run was added to the table
            today = str(datetime.date.today())
            now = str(datetime.datetime.now()).split(" ")[-1].split(".")[0]
            c.execute('''INSERT INTO incomplete
                        VALUES (?, ?, ?)''',
                      (i, today, now))
            print(str(i) + " was added to the incomplete table!")
    # commits changes to database and closes connection
    conn.commit()
    conn.close()
    return print("Incomplete run processing finished!")


# Checks if complete runs id is in incomplete (notifies if not)
# Moves complete run id from incomplete table to complete table
# Transfers complete run from MiSeq to T Drive
def complete_store():
    # established connection to database file
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # lists the ids of the complete files found
    comp = check_miseq()[0]
    print("complete: " + str(comp))
    for s in comp:
        # check if run is in incomplete
        c.execute('''SELECT * FROM incomplete WHERE run_id=?''', (s,))
        results = c.fetchall()
        # if id isn't in incomplete table, email is sent, loop moves to next id
        if len(results) == 0:
            send_email(s)
            continue
        # Removes run from incomplete and adds it to complete along with date and time at which it was moved
        today = str(datetime.date.today())
        now = str(datetime.datetime.now()).split(" ")[-1].split(".")[0]
        c.execute('''DELETE FROM incomplete WHERE run_id=?''', (s,))
        c.execute('''INSERT INTO complete VALUES (?, ?, ?)''',
                  (s, today, now))
        print(str(s) + " was transferred to the complete table!")
        # Moves run to T drive
        current_loc = os.path.join(miseq_path, s + "_complete.txt")
        new_loc = os.path.join(t_drive_path, s + "_complete.txt")
        os.rename(current_loc, new_loc)
    # finds the number of incomplete runs in the incomplete table of the database
    c.execute('''SELECT * FROM incomplete''')
    num_incomplete = c.fetchall()
    print("Number of incomplete in database: " + str(len(num_incomplete)))
    # finds the number of complete runs in the complete table of the database
    c.execute('''SELECT * FROM complete''')
    num_complete = c.fetchall()
    print("Number of complete in database: " + str(len(num_complete)))
    # commits changes to database and closes connection
    conn.commit()
    conn.close()
    return print("Complete run processing finished!")

# Below describes the timing of the functions above
# When the script is started the first job won't start until the number of minutes in the interval
# value has elapsed (i.e. if the add-miseq job is first, it won't start until a minute has passed)
# Depending on when the script is started, the forward calculation of start times can cause
# different jobs to start off the process, but no matter which job starts off the process
# the application runs fine - it will only impact the pattern of job accumulation

# adds a new run to the miseq every 20th second of every minute
# forward calculation of job start-time means this will happen first if the script is started
# prior to the 20th second of any minute
scheduler.add_job(add_miseq, 'interval', minutes=1, start_date='2019-07-02 9:51:20')

# checks for and stores incomplete runs every 25th second of every minute
# forward calculation of job start-time means this will happen first if the script is started
# between the 20th and 25th second of any minute
scheduler.add_job(incomplete_store, 'interval', minutes=1, start_date='2019-07-02 9:51:25')

# completes a run on the miseq every 30th second of every two minutes
# The increased interval was because I wanted to observe behavior when incomplete runs were accumulated
# forward calculation of job start-time means this will happen first if the script is started
# prior to the 30th second of any even minute and after the 25th second of any even minute
scheduler.add_job(complete_miseq, 'interval', minutes=2, start_date='2019-07-02 9:52:30')

# checks for, stores, and moves complete runs 35th second of every two minutes
# The increased interval was because I wanted to observe behavior when incomplete runs were accumulated
# forward calculation of job start-time means this will happen first if the script is started
# prior to the 35th second of any even minute and after the 30th second of any even minute
scheduler.add_job(complete_store, 'interval', minutes=2, start_date='2019-07-02 9:52:35')

# starts the jobs
scheduler.start()


