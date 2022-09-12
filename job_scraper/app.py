import streamlit as st
import pandas as pd
import plotly.express as px

df = pd.read_csv("all.csv")

jobs = st.sidebar.multiselect("Job?", df.clean.unique().tolist())
if len(jobs) != 0:
    df = df[df.clean.isin(jobs)]

country = st.sidebar.multiselect("Country?", df.country.unique().tolist())
if len(country) != 0:
    df = df[df.country.isin(country)]

level = st.sidebar.multiselect("Level?", df.level.unique().tolist())
if len(level) != 0:
    df = df[df.level.isin(level)]

job_type = st.sidebar.multiselect("job_type?", df.job_type.unique().tolist())
if len(job_type) != 0:
    df = df[df.job_type.isin(job_type)]

city = st.sidebar.multiselect("city?", df.city.unique().tolist())
if len(city) != 0:
    df = df[df.city.isin(city)]


st.title("Data Related Jobs in Europe")
st.dataframe(df[['title', 'company', 'place', 'level']][:30])

df['count'] = 1

fig = px.histogram(df, x='clean', y='count', color='country', barmode='group', text_auto=True)
st.plotly_chart(fig, use_container_width=True)