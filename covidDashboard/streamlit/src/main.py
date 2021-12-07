import streamlit as st
import mysql.connector as mysql
import pandas as pd

st.title("Datos desde Mysql provenientes del ETL")

db = mysql.connect(
    host = "db",
    user = "covid",
    passwd = "covid123",
    database = "dm_covid"
)


print(db) # it will print a connection object if everything is fine

cursor = db.cursor()

## defining the Query
query = "select id,name from covid"

## getting records from the table
cursor.execute(query)

## fetching all records from the 'cursor' object
df = pd.DataFrame(cursor.fetchall(), columns=['id', 'name'])

df