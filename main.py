import streamlit as st
import pandas as pd
import pydeck as pdk
from pyproj import Transformer
import random
import altair as alt
import math

def b_percent(x):
    d = {'אמת': 'nb', 'ב': 'nb', 'ג': 'b', 'ד': 'a', 'ו': 'a', 'ודעם': 'a', 'ום': 'a', 'ט': 'b', 'טב': 'nb', 'כ': 'b',
         'כן': 'nb', 'ל': 'nb', 'מחל': 'b', 'מרץ': 'nb', 'מרצ': 'nb', 'נ': 'nb', 'עם': 'a', 'פה': 'nb', 'צפ': 'nb',
         'שס': 'b', 'ת': 'nb',
         }
    b_count = 0
    nb_count = 0
    for col in x.index:
        if col in d:
            if d[col] == "b":
                b_count += x[col]
            else:
                nb_count += x[col]

    return b_count/(b_count+nb_count)


def epsg3857_to_epsg4326(x, y):
    x = (x * 180) / 20037508.34
    y = (y * 180) / 20037508.34
    y = (math.atan(math.pow(math.e, y * (math.pi / 180))) * 360) / math.pi - 90
    return x, y

@st.cache
def get_colors(votes):
    colors = {}
    for party in (list(votes.loc[:, 'אמת':])):
        colors[party] = [random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)]
    return colors


def rea_results_file(elections_num):
    try:
        temp_df = pd.read_csv("{}.csv".format(elections_num))
    except UnicodeDecodeError:
        print(elections_num)
        temp_df = pd.read_csv("{}.csv".format(elections_num), encoding="iso8859_8")
    temp_df["elections"] = elections_num

    return temp_df


@st.cache(allow_output_mutation=True)
def preprocess_data():
    votes = pd.concat([rea_results_file(elec) for elec in [19, 20, 21, 22, 23,24, 25]], ignore_index=True)
    votes = votes.fillna(0)
    cities = pd.read_csv("yeshuvim.csv")

    votes["leader"] = (votes.loc[:, 'אמת':]).idxmax(axis=1)

    party_colors = get_colors(votes)
    votes["color"] = votes.apply(lambda x: party_colors[x["leader"]], axis=1)
    cities[["lat", "lon"]] = cities.apply(lambda x: convert_coords(x["X"], x["Y"]), axis=1)

    df = pd.merge(cities, votes, left_on='SETL_CODE', right_on='סמל ישוב')
    df = df.rename(columns={"מצביעים": "voters", "שם ישוב": "name", "בזב": "num_eligible_to_vote"})
    df["percentage"] = (100 * df["voters"] / df["num_eligible_to_vote"]).astype(int)
    df["b_percent"] = df.apply(lambda x: b_percent(x), axis=1)
    df["bet_percent"] = df["ב"] / df["num_eligible_to_vote"]

    return df


@st.cache
def create_comparison_df():
    df_largest_cities = df[df["num_eligible_to_vote"] > 100000]
    df_largest_cities = df_largest_cities[["name", "percentage", "b_percent", "bet_percent", "elections", "SETL_CODE"]]
    df_24 = df_largest_cities[df_largest_cities["elections"] == 24]

    df_25 = df_largest_cities[df_largest_cities["elections"] == 25]
    df_24_25 = pd.merge(df_24, df_25, on='SETL_CODE', suffixes=("_24", "_25"))
    df_24_25["voting_diff"] = df_24_25['percentage_25'] - df_24_25['percentage_24']
    df_24_25["b_diff"] = 100 * (
                df_24_25['b_percent_25'] - df_24_25['b_percent_24'] - df_24_25['bet_percent_24'])
    return df_24_25


def draw_comparison_chart():
    a = alt.Chart(df_compare, title="voting participation vs conservative percentage").mark_line(opacity=1).encode(
        x='name_24', y='voting_diff')

    b = alt.Chart(df_compare).mark_line(color='red').encode(x='name_24', y='b_diff')
    c = alt.layer(a, b, )

    st.altair_chart(c, use_container_width=True)


def draw_voting_tables():
    top_voters = chosen_elections_df[["name", "percentage"]].sort_values(ascending=False, by="percentage",
                                                                         ignore_index=True)
    top_voters['percentage'] = top_voters['percentage'].astype(str) + "%"
    col1, col2 = st.columns(2)
    with col1:
        st.write("highest voters election {}".format(elections))
        st.table(top_voters.head(10))

    with col2:
        st.write("lowest voters election {}".format(elections))
        st.table(top_voters.tail(10))


def draw_percentage_chart():
    percent_df = df.groupby(["elections"]).sum().reset_index()
    percent_df["percent"] = percent_df["voters"] / percent_df["num_eligible_to_vote"]

    chart = alt.Chart(percent_df, title="voting participation").mark_line() \
        .encode(x=alt.X('elections:Q', axis=alt.Axis(tickCount=percent_df.shape[0], grid=False)),
                y=alt.Y('percent', scale=alt.Scale(domain=[0.6, 0.7])))
    st.altair_chart(chart, use_container_width=True)


def draw_voting_map():
    st.pydeck_chart(
        pdk.Deck(tooltip={"html": "<b>{name}</b>: <b>{leader}</b> "},
                 map_style=None,
                 initial_view_state=pdk.ViewState(latitude=31.8,
                                                  longitude=34.2,
                                                  zoom=7),

                 layers=[pdk.Layer(type='ScatterplotLayer',
                                   data=chosen_elections_df,
                                   get_position='[lon, lat]',
                                   get_fill_color='color',
                                   get_radius='1000+voters/100',
                                   pickable=True)], ))


df = preprocess_data()
df_compare = create_comparison_df()
elections = st.slider("elections", 19, 25)
chosen_elections_df = df[df["elections"] == elections]


draw_voting_map()
draw_percentage_chart()
draw_voting_tables()
draw_comparison_chart()
