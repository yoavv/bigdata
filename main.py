import streamlit as st
import random
import math
import pydeck as pdk
import altair as alt

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import lit, col
from pyspark.sql.types import FloatType, StringType, ArrayType, IntegerType
import pyspark.sql.functions as psf
from pyspark.sql import functions as f

party_to_wing_dict = {'אמת': 'nb', 'ב': 'nb', 'ג': 'b', 'ד': 'a', 'ו': 'a', 'ודעם': 'a', 'ום': 'a', 'ט': 'b',
                      'טב': 'nb', 'כ': 'b', 'כן': 'nb', 'ל': 'nb', 'מחל': 'b', 'מרץ': 'nb', 'מרצ': 'nb',
                      'נ': 'nb', 'עם': 'a', 'פה': 'nb', 'צפ': 'nb', 'שס': 'b', 'ת': 'nb',
                      }


def epsg3857_to_epsg4326(x, y):
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
    def translate_(column):
        return mapping.get(column)

    return functions.udf(translate_, ArrayType(IntegerType()))


def argmax(cols, *args):
    return [c for c, v in zip(cols, args) if v == max(args)][0]


def read_data(elections):
    elections.sort(reverse=True)
    df1 = spark.read.csv("{}.csv".format(elections[0]), inferSchema=True, header=True)
    df1 = df1.withColumn("elections", lit(elections[0]))
    for election in elections[1:]:
        if election != 19:
            encoding = "iso8859_8"
        else:
            encoding = None

        df2 = spark.read.csv("{}.csv".format(election), inferSchema=True, header=True, encoding=encoding)
        df2 = df2.withColumn("elections", lit(election))
        df2.drop("_c46")
        for column in [column for column in df2.columns if column not in df1.columns]:
            df1 = df1.withColumn(column, lit(None))

        for column in [column for column in df1.columns if column not in df2.columns]:
            df2 = df2.withColumn(column, lit(None))

        df1 = df1.unionByName(df2)

    return df1


@st.cache(allow_output_mutation=True)
def create_df():
    merged_dfs = read_data([25, 24, 23, 22, 21, 20, 19])
    merged_dfs = merged_dfs.withColumnRenamed("סמל ישוב", "SETL_CODE")
    merged_dfs = merged_dfs.withColumnRenamed("מצביעים", "voters")

    merged_dfs = merged_dfs.withColumnRenamed("בזב", "num_eligible_to_vote")
    merged_dfs = merged_dfs.withColumnRenamed("שם ישוב", "name")

    merged_dfs = merged_dfs.fillna(0)

    argmax_udf = lambda cols: psf.udf(lambda *args: argmax(cols, *args), StringType())
    merged_dfs = merged_dfs.withColumn("leader", argmax_udf(merged_dfs.columns[7:])(*merged_dfs.columns[7:]))

    party_colors = get_colors(merged_dfs.columns[7:-1])

    merged_dfs = merged_dfs.withColumn("color", translate(party_colors)("leader"))

    cities = spark.read.csv("yeshuvim.csv", inferSchema=True, header=True)
    cities = cities.drop("OBJECTID_1", "OBJECTID", "MGLSDE_LOC", "MGLSDE_L_1", "MGLSDE_L_2", "MGLSDE_L_3", "MGLSDE_L_4")
    merged_dfs = merged_dfs.join(cities, ['SETL_CODE'], how="inner")

    calc_lat = functions.udf(lambda x, y: epsg3857_to_epsg4326(x, y)[0], FloatType())
    calc_lon = functions.udf(lambda x, y: epsg3857_to_epsg4326(x, y)[1], FloatType())

    merged_dfs = merged_dfs.withColumn("lat", calc_lat(merged_dfs["X"], merged_dfs["Y"]))
    merged_dfs = merged_dfs.withColumn("lon", calc_lon(merged_dfs["X"], merged_dfs["Y"]))
    merged_dfs = merged_dfs.withColumn('percentage', 100 * (merged_dfs.voters / merged_dfs.num_eligible_to_vote))
    merged_dfs = merged_dfs.withColumn('percentage', merged_dfs.percentage.cast('int'))

    merged_dfs.withColumn('benet_percent', 100 * (merged_dfs["ב"] / merged_dfs.num_eligible_to_vote))
    return merged_dfs


def draw_voting_tables():
    chosen_elections_df.sort("percentage", ascending=False)
    col1, col2 = st.columns(2)

    with col1:
        st.write("highest voters election {}".format(election_num))
        st.table(chosen_elections_df.toPandas()[["name", "percentage"]].head(10))

    with col2:
        st.write("lowest voters election {}".format(election_num))
        st.table(chosen_elections_df.toPandas()[["name", "percentage"]].tail(10))


def create_comparison_df():
    df = merged_df.filter(merged_df.num_eligible_to_vote > 100000)
    df = df.withColumn("benet_percent", df["ב"] / df["num_eligible_to_vote"])

    numeric_cols = []
    for k, v in party_to_wing_dict.items():
        if v == 'b':
            numeric_cols.append(k)
    from operator import add
    from functools import reduce
    df = df.withColumn('bibi_sum', reduce(add, [col(x) for x in numeric_cols]))

    df = df.withColumn("bibi_percent", df["bibi_sum"] / df["voters"])
    df_24 = df.filter(df.elections == 24)
    df_25 = df.filter(df.elections == 25)

    df_24 = df_24.select([f.col(c).alias(c + "_24") for c in df.columns])

    df_25 = df_25.select([f.col(c).alias(c + "_25") for c in df.columns])
    df_24_25 = df_24.join(df_25, df_24.SETL_CODE_24 == df_25.SETL_CODE_25)
    df_24_25 = df_24_25.withColumn("voting_diff", df_24_25['percentage_25'] - df_24_25['percentage_24'])
    df_24_25 = df_24_25.withColumn("bibi_diff", 100 * (
            df_24_25['bibi_percent_25'] - df_24_25['bibi_percent_24'] - df_24_25['benet_percent_24']))
    return df_24_25


def draw_comparison_chart():
    a = alt.Chart(df_compare.toPandas(), title="voting participation vs conservative percentage").mark_line(
        opacity=1).encode(
        x='name_24', y='voting_diff')

    b = alt.Chart(df_compare.toPandas()).mark_line(color='red').encode(x='name_24', y='b_diff')
    c = alt.layer(a, b, )

    st.altair_chart(c, use_container_width=True)


def draw_percentage_chart():
    percent_df = merged_df.groupBy("elections").sum("num_eligible_to_vote", "voters")

    percent_df = percent_df.withColumn("percent", percent_df["sum(voters)"] / percent_df["sum(num_eligible_to_vote)"])

    chart = alt.Chart(percent_df.toPandas(), title="voting participation").mark_line() \
        .encode(x=alt.X('elections:Q', axis=alt.Axis(tickCount=percent_df.count(), grid=False)),
                y=alt.Y('percent', scale=alt.Scale(domain=[0.6, 0.7])))
    st.altair_chart(chart, use_container_width=True)


def draw_voting_map():
    st.pydeck_chart(
        pdk.Deck(
            map_style=None, tooltip={"html": "<b>{name}</b>: <b>{leader}</b> "},
            initial_view_state=pdk.ViewState(latitude=31.8,
                                             longitude=34.2,
                                             zoom=7),

            layers=[pdk.Layer(type='ScatterplotLayer',
                              data=chosen_elections_df.toPandas(),
                              get_position='[lat, lon]',
                              get_radius='1000+voters/100', get_fill_color='color',
                              pickable=True)], ))


spark = SparkSession.builder.appName('spark-dataframe-demo').getOrCreate()
merged_df = create_df()
st.title("Election Trends in Israel")
st.title("Yoav Valinsky & Daniel Neimark")
election_num = st.slider("elections", 19, 25)
chosen_elections_df = merged_df.filter(merged_df.elections == election_num)
df_compare = create_comparison_df()

draw_voting_map()
draw_percentage_chart()
draw_voting_tables()
draw_comparison_chart()
