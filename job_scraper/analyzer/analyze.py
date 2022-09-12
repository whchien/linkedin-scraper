import os
from datetime import datetime
from langdetect import detect
import pandas as pd

from job_scraper.config.core import config


def merge_files_to_df() -> pd.DataFrame:
    files = []
    for f in os.listdir("./data"):
        if f.endswith('csv'):
            tmp = pd.read_csv(os.path.join("./data", f))
            files.append(tmp)
    merged = pd.concat(files)
    merged = merged.drop_duplicates(subset='uuid').reset_index()
    merged = merged.drop(['Unnamed: 0', 'index'], axis=1)
    return merged


def load_data() -> pd.DataFrame:
    df = merge_files_to_df()
    df = preprocess(df)
    return df


def preprocess(df) -> pd.DataFrame:
    df['clean'] = df.title.map(clean_title)
    df['country'] = df.place.map(detect_country)
    df['city'] = df.place.map(clean_city)
    df['lang'] = df.descrip.map(detect)
    # df['date'] = df.post_since.map(clean_time).dt.date
    return df


def clean_title(x):
    for k, v in config.job_title[0].items():
        for _v in v:
            if x.lower().find(_v) != -1 or x.lower().find(_v.replace(" ", "")) != -1:
                return k
    return "other"


def clean_city(x):
    split = x.split(',')
    if len(split) > 1:
        return split[0]
    else:
        return "na"


def detect_country(x):
    # TODO: refract with config file
    if x.lower().find("netherlands") != -1:
        return "NL"
    elif x.lower().find("ireland") != -1:
        return "IR"
    elif x.lower().find("united kingdom") != -1:
        return "UK"
    else:
        return "CH"


def clean_time(x):
    if x.find('hour') != -1:
        return datetime.datetime.now()
    elif x.find('day') != -1:
        day = int(x.split(" ")[0])
        dt = datetime.timedelta(days=day)
        return datetime.datetime.now() - dt
    elif x.find('week') != -1:
        week = int(x.split(" ")[0])
        dt = datetime.timedelta(weeks=week)
        return datetime.datetime.now() - dt
    elif x.find('month') != -1:
        month = int(x.split(" ")[0])
        day = month * 28
        dt = datetime.timedelta(days=day)
        return datetime.datetime.now() - dt
    else:
        return None
