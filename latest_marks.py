import mysql.connector
import pandas as pd
from datetime import datetime
from config import config

data = pd.read_csv('/Users/sainivasrangaraju/Desktop/ETL/Marks.csv', header=None, names=['name','marks'])

log_file = open('/Users/sainivasrangaraju/Desktop/ETL/marks_update_log.txt','a')
st = '-'*100

log_file.write(st)


try:
    connection = mysql.connector.connect(**config)

    cursor = connection.cursor()
    update_flag = 0
    for index, row in data.iterrows():
        name = row['name']
        marks = row['marks']
        cursor.execute('select * from ETL_data where firstname = (%s)', (name,))
        result = cursor.fetchone()
        if result is None:
            cursor.execute('insert into etl_data values(%s, %s)', (name, marks))
            update_flag = 1
            log_file.write(f'\n{datetime.now()} : Insertion done: {name} -> {marks}')
        else:
            try:
                cursor.execute('select mid_percent from ETL_data where firstname = (%s)',(name,))
                pre_marks = cursor.fetchone()
                if pre_marks is not None:
                    pre_marks_value = float(pre_marks[0])

                    if pre_marks_value != marks:
                        update_flag = 1
                        cursor.execute('UPDATE etl_data SET mid_percent = %s WHERE firstname = %s', (marks, name))
                        log_file.write(f'\n{datetime.now()} : Updation done: {name} -> {pre_marks_value} to {marks}\n') 
            except mysql.connector.Error as err:
                log_file.write(f'\nFailed to insert {name}: {err}\n')
    if update_flag == 0:
        log_file.write(f'\n{datetime.now()} :All the data is up-to-date\n')
    
except mysql.connector.Error as err:
    log_file.write('\nFailed connection\n')