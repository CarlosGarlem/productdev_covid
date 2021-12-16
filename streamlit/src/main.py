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
def load_data(SQL_script):
    with st.spinner('Cargando datos...'):
        time.sleep(0.2)
        df = (pd.read_sql_query(SQL_script, get_connection())
            .assign(date = lambda df: pd.to_datetime(df.date))
        )
    return df



def getMap(region_df):

    '''
    fig = px.scatter_geo(plot_region
                        ,lat = 'lat'
                        ,lon = 'long'
                        ,color = 'confirmed'
                        ,size='confirmed' 
                        ,template = 'ggplot2'
                        ,color_continuous_scale='blugrn'
                        ,range_color = (0,10)
                        ,hover_name = 'province_state'
                        #,size_max = 20
                        ,title = 'Covid Confirmed Cases WorldWide'
                        ,width = 1800
                        ,scope = 'world'
                        ,hover_data = ['confirmed', 'deaths', 'recovered']
                        ,projection = 'robinson'
                        #,animation_frame='sk_month'
                        )
    '''

    
    fig = px.choropleth(region_df
                    ,locations = 'country_region'
                    ,color='confirmed'
                    ,color_continuous_scale = 'sunset'
                    ,hover_data = {'confirmed': True, 'deaths': True, 'recovered': True, 'country_region': False}
                    ,hover_name = 'country_region'
                    ,title = 'Covid Confirmed Cases WorldWide'
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
                ,title = 'Stadistísticas vs Tiempo'
                ,template = 'none')
    return fig


#endregion


#region General Data
st.set_page_config(page_title = 'Streamlit COVID Dashboard', layout = 'wide')
st.title('COVID-19 Dashboard')

covid_df = load_data(SQL_INIT_SCRIPT)

countries = ['All']
countries.extend(covid_df['country_region'].unique().tolist())
#endregion


#region SidebarFilters
st.sidebar.header('Covid Dashboard filters')
with st.sidebar:
    #province_selector = st.selectbox('Choose a Province/State', covid_df['province_state'].unique())
    country_selector = st.selectbox('Seleccione un Pais/Region:', countries)
    date_range = st.date_input('Seleccione una fecha:'
                        , value = (covid_df['date'].min(), covid_df['date'].max())
                        , min_value=covid_df['date'].min()
                        , max_value=covid_df['date'].max()
    )
    #date_range = st.sidebar.slider('Fechas:', value=[covid_df['date'].min(), covid_df['date'].max()])
#endregion




#region Streamlit Dash
'''
### KPIs
'''



'''
### Mapa
'''
region_df = (covid_df.loc[lambda df: (df.date.dt.date >= date_range[0]) | (df.date.dt.date <= date_range[1])]
            .groupby(['country_region'], as_index = False)
            .agg(confirmed = ('confirmed_cases', np.max), deaths = ('death_cases', np.max), recovered = ('recovered_cases', np.max))
            .reset_index(drop = True)
)
if country_selector != 'All':
    region_df = region_df.loc[lambda df: (df.country_region == country_selector)]

st.plotly_chart(getMap(region_df), use_container_width=True)
region_df






'''
### Estadisticas generales
'''
line_graph_df = (covid_df.groupby(['country_region', 'date'], as_index = False)
                .agg(confirmed = ('confirmed_cases', np.max), deaths = ('death_cases', np.max), recovered = ('recovered_cases', np.max))
                .loc[lambda df: (df.date.dt.date >= date_range[0]) | (df.date.dt.date <= date_range[1])]
                .melt(id_vars = ['country_region', 'date'], value_vars = ['confirmed', 'deaths', 'recovered'])
                .sort_values(by = ['country_region', 'date', 'variable'])
                .reset_index(drop = True)
)
if country_selector != 'All':
    line_graph_df = line_graph_df.loc[lambda df: (df.country_region == country_selector)]
else:
    line_graph_df = (line_graph_df.drop(labels = 'country_region', axis = 1).groupby(['date', 'variable'], as_index = False).sum())

st.plotly_chart(getLinePlot(line_graph_df), use_container_width = True)




#endregion