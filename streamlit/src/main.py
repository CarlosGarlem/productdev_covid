import streamlit as st
import mysql.connector as mysql
import pandas as pd
from datetime import datetime
import sys




db = mysql.connect(
    host = "db",
    user = "covid",
    passwd = "covid123",
    database = "dm_covid"
)
print(db) # it will print a connection object if everything is fine
cursor = db.cursor()

def run_query(query):
    cursor.execute(query)
    return cursor.fetchall()


st.set_page_config(page_title="Streamlit COVID Dashboard", layout="wide")
st.title("COVID-19 Dashboard")

st.sidebar.header('Covid Dashboard filters')
#st.sidebar.markdown("## Dashboard sidebar ")
query = "SELECT MIN(lat) FROM d_region"
lat_min = run_query(query)
lat_max = run_query(query.replace("MIN(", "MAX("))
latitude_range = st.sidebar.slider("Latitud:", value=[lat_min[0][0], lat_max[0][0]])

query = "SELECT MIN(r.long) FROM d_region r"
long_min = run_query(query)
long_max = run_query(query.replace("MIN(", "MAX("))
longitude_range = st.sidebar.slider("Longitud:", value=[long_min[0][0], long_max[0][0]])

query = "SELECT DISTINCT(country_region) FROM d_region"
countries = run_query(query)
countries = [country[0] for country in countries]
country = st.sidebar.selectbox('País:', countries)

query = "SELECT MIN(date) FROM d_date"
start_date = run_query(query)
end_date = run_query(query.replace("MIN(", "MAX("))
range_dates = st.sidebar.slider('Fechas:', value=[start_date[0][0], end_date[0][0]])



col1, col2, col3 = st.columns(3)

with col1:
    query = "SELECT FORMAT(SUM(t.cc), 0) FROM (SELECT sk_region, MAX(confirmed_cases) cc FROM f_covid GROUP BY sk_region) t"
    result = run_query(query)
    st.markdown("**Casos confirmados**")
    st.markdown(f"<h1 style='text-align:right; color:#ffde24;'>{result[0][0]}</h1>", unsafe_allow_html=True)

with col2:
    query = "SELECT FORMAT(SUM(t.rc), 0) FROM (SELECT sk_region, MAX(recovered_cases) rc FROM f_covid GROUP BY sk_region) t"
    result = run_query(query)
    st.markdown("**Casos recuperados**")
    st.markdown(f"<h1 style='text-align:right; color:#00ad00;'>{result[0][0]}</h1>", unsafe_allow_html=True)


with col3:
    query = "SELECT FORMAT(SUM(t.dc), 0) FROM (SELECT sk_region, MAX(death_cases) dc FROM f_covid GROUP BY sk_region) t"
    result = run_query(query)
    st.markdown("**Fallecimientos**")
    st.markdown(f"<h1 style='text-align:right; color:red;'>{result[0][0]}</h1>", unsafe_allow_html=True)


data1, data2 = st.columns(2)

with data1:
    query = "SELECT r.country_region, r.sk_region, MAX(f.confirmed_cases) cc FROM f_covid f, d_region r WHERE f.sk_region = r.sk_region GROUP BY r.country_region, r.sk_region"
    query = "SELECT t.country_region, FORMAT(SUM(t.cc), 0) confirmed, SUM(t.cc) FROM (" + query + ") t GROUP BY t.country_region ORDER BY 3 DESC LIMIT 10"
    result = run_query(query)
    result = [item[:2] for item in result]
    st.subheader("Top 10 casos confirmados")
    df = pd.DataFrame(result, columns=['País', 'Casos'])
    df

with data2:
    st.subheader("Casos confirmados")
    chart_data = None
    if (range_dates and country):
        query = "SELECT date_format(d.date, '%y-%m-%d'), f.confirmed_cases, f.death_cases, f.recovered_cases FROM f_covid f \
                INNER JOIN d_date d ON f.sk_date = d.sk_date AND d.date between '{}' and '{}' \
                INNER JOIN d_region r ON f.sk_region = r.sk_region AND r.country_region = '{}'".format(range_dates[0].isoformat(), range_dates[1].isoformat(), country)
        chart_data = run_query(query)
    df = pd.DataFrame(chart_data, columns=['date', 'confirmed', 'death', 'recovered'])
    df = df.set_index('date')
    st.line_chart(df)



