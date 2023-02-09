import streamlit as st
import random
import pydeck as pdk

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import lit, when

import math
from pyspark.sql.types import FloatType, StringType, ArrayType, IntegerType
import pyspark.sql.functions as psf


def epsg3857toEpsg4326(x, y):
    x = (x * 180) / 20037508.34
    y = (y * 180) / 20037508.34
    y = (math.atan(math.pow(math.e, y * (math.pi / 180))) * 360) / math.pi - 90
    return x, y

@st.cache
def get_colors(party_name_list):
    colors = {}
    for party in party_name_list:
        colors[party] = [random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)]
    return colors


def translate(mapping):
    def translate_(col):
        return mapping.get(col)

    return functions.udf(translate_, ArrayType(IntegerType()))


def argmax(cols, *args):
    return [c for c, v in zip(cols, args) if v == max(args)][0]


def read_data(elections):
    elections.sort(reverse=True)
    df1 = spark.read.csv("{}.csv".format(elections[0]), inferSchema=True, header=True)
    df1 = df1.withColumn("elections",lit(elections[0]))
    for election in elections[1:]:
        df2 = spark.read.csv("{}.csv".format(election), inferSchema=True, header=True, encoding="iso8859_8")
        df2 = df2.withColumn("elections",lit(election))
        for column in [column for column in df2.columns if column not in df1.columns]:
            df1 = df1.withColumn(column, lit(None))

        for column in [column for column in df1.columns if column not in df2.columns]:
            df2 = df2.withColumn(column, lit(None))

        df1 = df1.unionByName(df2)
    return df1

spark = SparkSession.builder.appName('spark-dataframe-demo').getOrCreate()

@st.cache(allow_output_mutation=True)
def create_df():
    merged_df = read_data([25, 23,22,21,20])
    merged_df = merged_df.withColumnRenamed("סמל ישוב", "SETL_CODE")
    merged_df = merged_df.withColumnRenamed("מצביעים", "voters")

    merged_df = merged_df.withColumnRenamed("בזב", "num_eligible_to_vote")
    merged_df = merged_df.withColumnRenamed("שם ישוב", "name")

    merged_df = merged_df.fillna(0)

    argmax_udf = lambda cols: psf.udf(lambda *args: argmax(cols, *args), StringType())
    merged_df = merged_df.withColumn("leader", argmax_udf(merged_df.columns[7:])(*merged_df.columns[7:]))

    party_colors = get_colors(merged_df.columns[7:-1])

    merged_df = merged_df.withColumn("color", translate(party_colors)("leader"))

    cities = spark.read.csv("yeshuvim.csv", inferSchema=True, header=True)
    cities = cities.drop("OBJECTID_1", "OBJECTID", "MGLSDE_LOC", "MGLSDE_L_1", "MGLSDE_L_2", "MGLSDE_L_3", "MGLSDE_L_4")
    merged_df = merged_df.join(cities, ['SETL_CODE'], how="inner")

    calc_lat = functions.udf(lambda x, y: epsg3857toEpsg4326(x, y)[0], FloatType())
    calc_lon = functions.udf(lambda x, y: epsg3857toEpsg4326(x, y)[1], FloatType())

    merged_df = merged_df.withColumn("lat", calc_lat(merged_df["X"], merged_df["Y"]))
    merged_df2 = merged_df.withColumn("lon", calc_lon(merged_df["X"], merged_df["Y"]))
    return merged_df2

merged_df= create_df()
election_num = st.slider("elections", 19, 25)

df = merged_df.filter(merged_df.elections ==election_num)
st.pydeck_chart(
    pdk.Deck(
        map_style=None, tooltip={"html": "<b>{name}</b>: <b>{leader}</b> "},
        initial_view_state=pdk.ViewState(latitude=31.8,
                                         longitude=34.2,
                                         zoom=7),

        layers=[pdk.Layer(type='ScatterplotLayer',
                          data=df.toPandas(),
                          get_position='[lat, lon]',
                          get_radius='1000+voters/100', get_fill_color='color',
                          pickable=True)], ))
