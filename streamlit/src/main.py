import streamlit as st
import plotly.express as px
import time
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime
import sys



#region Global Constants
SQL_INIT_SCRIPT = 'SELECT * FROM dm_covid.covid_view;'
#endregion


#region Functions
@st.cache(allow_output_mutation = True)
def get_connection():
    return create_engine('mysql+mysqlconnector://covid:covid123@db/dm_covid')


@st.cache(suppress_st_warning = True)
def load_data():
    with st.spinner('Cargando datos...'):
        time.sleep(0.2)
        df = (pd.read_sql_query(SQL_INIT_SCRIPT, get_connection())
            .assign(date = lambda df: pd.to_datetime(df.date))
        )
    return df



def getMap(df):
   
    fig = px.choropleth(df
                    ,locations = 'country_region'
                    ,color='confirmed'
                    ,color_continuous_scale = 'sunset'
                    ,hover_data = {'confirmed': True, 'deaths': True, 'recovered': True, 'country_region': False}
                    ,hover_name = 'country_region'
                    ,title = 'Mapa de Calor - Casos de Covid'
                    ,locationmode='country names'
                  )

    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig


def getLinePlot(df):
    fig = px.line(df
                ,x = 'date'
                ,y = 'value'
                ,color = 'variable'
                ,hover_name = 'variable'
                ,hover_data = {'date': True, 'value': True, 'variable': False}
                ,title = 'Estadistísticas Acumuladas en el Tiempo'
                ,template = 'none')
    return fig

def getKPIs(df, metric):
    kpi_res = df.sum(axis=0)
    return kpi_res[metric]

#endregion




#region General Data
st.set_page_config(page_title = 'Streamlit COVID Dashboard', layout = 'wide')
st.title('COVID-19 Dashboard')

covid_df = load_data()
countries = ['Todos']
countries.extend(covid_df['country_region'].unique().tolist())

#endregion


#region SidebarFilters
st.sidebar.header('Filtros')
with st.sidebar:
    country_selector = st.selectbox('Seleccione un País/Región:', countries)

    start_date = st.date_input('Fecha Inicio:'
                            , value = covid_df['date'].min()
                            , min_value = covid_df['date'].min()
                            , max_value = covid_df['date'].max()
    )

    end_date = st.date_input('Fecha Fin:'
                            , value = covid_df['date'].max()
                            , min_value = covid_df['date'].min()
                            , max_value = covid_df['date'].max()
    )

#endregion


#region Streamlit Dash
'''
### KPIs
'''
region_df = (covid_df.loc[lambda df: (df.date.dt.date >= start_date) & (df.date.dt.date <= end_date)]
        .groupby(['country_region'], as_index = False)
        .agg(confirmed = ('confirmed_cases', np.sum), recovered = ('recovered_cases', np.sum), deaths = ('death_cases', np.sum))
        .reset_index(drop = True)
)
region_df.index = region_df.index + 1
if country_selector != 'Todos':
    region_df = region_df.loc[lambda df: (df.country_region == country_selector)]



col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"<h1 style='text-align:center; font-weight: bold; font-size: 20px;'>Casos Confirmados</h1>", unsafe_allow_html=True)
    st.markdown(f"<h1 style='text-align:center; color:#ffde24;'>{getKPIs(region_df, 'confirmed'):,.0f}</h1>", unsafe_allow_html=True)

with col2:
    st.markdown(f"<h1 style='text-align:center; font-weight: bold; font-size: 20px;'>Casos Recuperados</h1>", unsafe_allow_html=True)
    st.markdown(f"<h1 style='text-align:center; color:#00ad00;'>{getKPIs(region_df, 'recovered'):,.0f}</h1>", unsafe_allow_html=True)


with col3:
    st.markdown(f"<h1 style='text-align:center; font-weight: bold; font-size: 20px;'>Muertes</h1>", unsafe_allow_html=True)
    st.markdown(f"<h1 style='text-align:center; color:red;'>{getKPIs(region_df, 'deaths'):,.0f}</h1>", unsafe_allow_html=True)



'''
### Mapa de Calor
'''

st.plotly_chart(getMap(region_df), use_container_width=True)
rcol1, rcol2, rcol3 = st.columns([1,2,1])
with rcol2:
    region_df



'''
### Estadísticas generales
'''
line_graph_df = (covid_df.groupby(['country_region', 'date'], as_index = False)
                .agg(confirmed = ('confirmed_cases', np.sum), recovered = ('recovered_cases', np.sum), deaths = ('death_cases', np.sum))
                .assign(confirmed_acu = lambda df: df.confirmed.cumsum(), deaths_acu = lambda df: df.deaths.cumsum(), recovered_acu = lambda df: df.recovered.cumsum())
                .loc[lambda df: (df.date.dt.date >= start_date) & (df.date.dt.date <= end_date)]
                .drop(labels = ['confirmed', 'deaths', 'recovered'], axis = 1)
                .melt(id_vars = ['country_region', 'date'], value_vars = ['confirmed_acu', 'recovered_acu', 'deaths_acu'])
                .sort_values(by = ['country_region', 'date', 'variable'])
                .reset_index(drop = True)
)

if country_selector != 'Todos':
    line_graph_df = line_graph_df.loc[lambda df: (df.country_region == country_selector)]
else:
    line_graph_df = (line_graph_df.drop(labels = 'country_region', axis = 1).groupby(['date', 'variable'], as_index = False).sum())

st.plotly_chart(getLinePlot(line_graph_df), use_container_width = True)


#endregion