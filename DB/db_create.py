#!/usr/bin/python
import sqlite3

conn = sqlite3.connect('Run_DB.sqlite')
c = conn.cursor()

c.execute('''CREATE TABLE incomplete
            (run_id text, date_added text, time_added text)''')

c.execute('''CREATE TABLE complete
            (run_id text, date_added text, time_added text)''')

conn.commit()
conn.close()

