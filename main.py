import streamlit as st
import pandas as pd
import pydeck as pdk
from pyproj import Transformer
import random

transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326")


def convert_coords(x1, y1):
    x2, y2 = transformer.transform(x1, y1)
    return pd.Series([x2, y2])


@st.cache
def get_colors():
    colors = {}
    for party in (list(votes.loc[:, 'אמת':])):
        colors[party] = [random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)]
    return colors


def rea_results_file(elections_num):
    try:
        temp_df = pd.read_csv("{}.csv".format(elections_num))
    except UnicodeDecodeError:
        temp_df = pd.read_csv("{}.csv".format(elections_num), encoding="iso8859_8")
    temp_df["elections"] = elections_num

    return temp_df


votes = pd.concat([rea_results_file(elec) for elec in [19, 20, 21, 22, 23, 25]], ignore_index=True)
votes = votes.fillna(0)
cities = pd.read_csv("yeshuvim.csv")

votes["leader"] = (votes.loc[:, 'אמת':]).idxmax(axis=1)

party_colors = get_colors()
votes["color"] = votes.apply(lambda x: party_colors[x["leader"]], axis=1)
cities[["lat", "lon"]] = cities.apply(lambda x: convert_coords(x["X"], x["Y"]), axis=1)

df = pd.merge(cities, votes, left_on='SETL_CODE', right_on='סמל ישוב')
df = df.rename(columns={"מצביעים": "voters", "שם ישוב": "name", "בזב": "num_eligible_to_vote"})
df["percentage"] = (100 * df["voters"] / df["num_eligible_to_vote"]).astype(int)
percent_df = df[["voters", "elections", "num_eligible_to_vote"]].groupby(["elections"]).sum()
percent_df["percent"] = percent_df["voters"] / percent_df["num_eligible_to_vote"]
st.bar_chart(percent_df["percent"], use_container_width=True)
elections = st.slider("elections", 19, 25)
df = df[df["elections"] == elections]

st.pydeck_chart(
    pdk.Deck(tooltip={"html": "<b>{name}</b>: <b>{leader}</b> "},
             map_style=None,
             initial_view_state=pdk.ViewState(latitude=31.8,
                                              longitude=34.2,
                                              zoom=7),

             layers=[pdk.Layer(type='ScatterplotLayer',
                               data=df,
                               get_position='[lon, lat]',
                               get_fill_color='color',
                               get_radius='1000+voters/100',
                               pickable=True)], ))

top_voters = df[["name", "percentage"]].sort_values(ascending=False, by="percentage", ignore_index=True)
top_voters['percentage'] = top_voters['percentage'].astype(str) + "%"
col1, col2 = st.columns(2)

with col1:
    st.write("highest voters")
    st.table(top_voters.head(10))

with col2:
    st.write("lowest voters")
    st.table(top_voters.tail(10))
