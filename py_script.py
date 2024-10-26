import mysql.connector
import pandas as pd
from config import config
data = pd.read_csv('./Marks.csv', header = None, names=['Names','marks'])

try:
    connection = mysql.connector.connect(**config)

    if connection.is_connected():
        print("Connected to ETL database")

    cursor = connection.cursor()

    for index, row in data.iterrows():
        name = row['Names']
        marks = row['marks']
        cursor.execute('Insert into ETL_data values(%s, %s)',(name, marks))
    
    cursor.execute('Select * from ETL_data')
    output = cursor.fetchall()

    for out in output:
        print(out)




except mysql.connector.Error as err:
    print(f"Error: {err}")