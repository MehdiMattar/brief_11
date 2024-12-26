import pandas as pd
import streamlit as st
import numpy as np
import pydeck as pdk
import plotly.express as px
import psycopg2
from sqlalchemy import create_engine


##########################################################################################################################
host='postgres_container'  #pgAdmin -> Servers -> name_server right click -> Properties -> Connection -> Host name/adress
database='rapport_travail'  #pgAdmin database name
user='admin'  # docker-compose.yml -> sevices -> postgres -> enviroment -> POSTGRES_USER
password='password'  # docker-compose.yml -> sevices -> postgres -> enviroment -> POSTGRES_PASSWOED
port='5432' #pgAdmin -> Servers -> name_server right click -> Properties -> Connection -> Port
##########################################################################################################################

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(connection_string)

query = "SELECT * FROM job_offers;"
df = pd.read_sql(query, engine)
###########################################################################################################################


############         La Visualisation        #############################################################################################

st.set_page_config( layout="wide" )
st.title("France Travail")

#lecture à partir d'un csv
#df= pd.read_csv("rapport_travail.csv", delimiter=',',encoding='utf-8')

st.sidebar.header("Options de filtrage")
result = df

process = st.sidebar.checkbox("filtrer le dataframe")
if process:

    #ville
    ville = df["lieu_de_travail"].unique()
    ville_new = st.sidebar.multiselect("Choisir une ville", ville)
    ville_filter = df["lieu_de_travail"].isin(ville_new) if ville_new else pd.Series([True] * len(df))

    #fonction
    fonction = df["metier"].unique()
    fonction_new = st.sidebar.multiselect("Choisir une fonction", fonction)
    fonction_filter = df["metier"].isin(fonction_new) if fonction_new else pd.Series([True] * len(df))

    #experience
    experience = df["experience_exige"].unique()
    experience_new = st.sidebar.multiselect("Choisir une experience exigé", experience)
    experience_filter = df["experience_exige"].isin(experience_new) if experience_new else pd.Series([True] * len(df))

    #type de contrat
    type_de_contrat = df["type_de_contrat"].unique()
    type_de_contrat_new = st.sidebar.multiselect("Choisir un type de contrat", type_de_contrat)
    type_de_contrat_filter = df["type_de_contrat"].isin(type_de_contrat_new) if type_de_contrat_new else pd.Series([True] * len(df))

    #alternace
    alternance = st.sidebar.checkbox("Afficher uniquement les offres en alternance")
    alternance_filter = df["alternance"] if alternance else pd.Series([True] * len(df))

    #main filter
    main_filter = ville_filter & fonction_filter & experience_filter  & experience_filter & alternance_filter
    
    #main dataframe
    result = df.loc[main_filter,:]

st.write(result)




#visulisation
st.sidebar.header("Options de visualization")

#map
map = st.sidebar.checkbox("map")
if map:
    st.map(result,latitude=df["latitude"], longitude=df["longitude"],size=len(result)*10,color = "#0044ff")

#map 3D
map3D = st.sidebar.checkbox("map3D")
if map3D:
    chart_data = result[["latitude","longitude"]]
    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=48.86,
                longitude=2.33,
                zoom=11,
                pitch=50),
            layers=[
                pdk.Layer(
                    "HexagonLayer",
                    data=chart_data,
                    get_position=["longitude", "latitude"],
                    radius=2000,
                    elevation_scale=40,
                    elevation_range=[0, 1000],
                    pickable=True,
                    extruded=True),
                pdk.Layer(
                    "ScatterplotLayer",
                    data=chart_data,
                    get_position=["longitude", "latitude"],
                    get_color="[200, 30, 0, 160]",
                    get_radius=2000)]))

#value count
experience_counts = result['experience_exige'].value_counts()
plot_data_e = pd.DataFrame({
    'Experience': experience_counts.index,
    'Count': experience_counts.values})

type_de_contrat_counts = result['type_de_contrat'].value_counts()
plot_data_t = pd.DataFrame({
    'type_de_contrat': type_de_contrat_counts.index,
    'Count': type_de_contrat_counts.values})

metier_counts = result['metier'].value_counts()
plot_data_m = pd.DataFrame({
    'metier': metier_counts.index,
    'Count': metier_counts.values})


#bar chartt
bar = st.sidebar.checkbox("bar chart metier")
if bar:

    fig = px.bar(
            plot_data_m, 
            x='metier', 
            y='Count', 
            title="Distribution des metiers",
            labels={'metier': 'metiers', 'Count': 'Nombre'}
        )

        # Adjust layout for better label visibility
    fig.update_layout(
        xaxis_tickangle=-45,  # Rotate x-axis labels
        height=600,           # Set the height of the chart
        margin=dict(l=20, r=20, t=50, b=100),  # Adjust margins
    )

    st.plotly_chart(fig)

#pie
pie = st.sidebar.checkbox("pie chart experience")
if pie:
    fig = px.pie(result, values=experience_counts.values, names=experience_counts.index, title='experience')
    st.plotly_chart(fig)


#pie 2
pie = st.sidebar.checkbox("pie chart type de contrat")
if pie:
    fig = px.pie(result, values=type_de_contrat_counts.values, names=type_de_contrat_counts.index, title='type de contrat')
    st.plotly_chart(fig)
