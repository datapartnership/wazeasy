import pandas as pd
import geopandas as gpd
from datetime import timedelta
from datetime import datetime as dt
import dask.dataframe as dd
import itertools
from shapely import wkt

def load_data_parquet(main_path, year, month, storage_options):
    path = main_path + 'year={}/month={}/*.parquet'.format(year, month)
    df = dd.read_parquet(path, storage_options=storage_options, engine = 'pyarrow')
    return df

def handle_time(df, utc_region, parquet = False):
    '''Handle time column so that it is in the correct utc and calculate useful time attributes'''
    if parquet:
        df['ts'] = df.ts.dt.tz_localize('UTC')
    else:
        df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['local_time'] = df['ts'].dt.tz_convert(utc_region)
    time_attributes(df)

def time_attributes(df):
    '''Calculate year, month, date and hour for each jam'''
    df['year'] = df['local_time'].dt.year
    df['month'] = df['local_time'].dt.month
    df['date'] = df['local_time'].dt.date
    df['hour'] = df['local_time'].dt.hour

def tci_by_period_geography(ddf, period, geography, agg_column):
    tci = ddf.groupby([period, geography])[[agg_column]].sum().compute()
    tci.rename(columns = {'length': 'tci'}, inplace = True)
    return tci

# def get_h3

def mean_tci_geog(ddf, date, geog, agg_column, working_days):
    daily_tci = tci_by_period_geography(ddf, date, geog, agg_column)
    geogs = list(daily_tci.reset_index(level = 0).index)
    idxs = pd.MultiIndex.from_tuples(list(itertools.product(working_days, geogs)))
    daily_tci = daily_tci.reindex(idxs, fill_value = 0)
    daily_tci.reset_index(inplace = True)
    daily_tci.rename(columns = {'level_0': 'date', 'level_1': geog}, inplace = True)
    return daily_tci.groupby(geog)['tci'].mean()

def create_gdf(data, epsg, col):
    geometry = data[col].apply(wkt.loads)
    data_geo = gpd.GeoDataFrame(data, crs="EPSG:{}".format(epsg), geometry=geometry)
    return data_geo

def get_summary_statistics_street(df, street_names, year, working_days):
    streets = df[df['street'].isin(street_names)].copy()
    table = (streets.groupby('street')['uuid']
             .nunique()
             .to_frame('number_of_jams')
             .compute())
    table['total_jam_length'] = (streets.groupby('street')['length']
                                 .sum()
                                 .compute()) / 1000

    by_levels = (streets.groupby(['street', 'level'])[['length']]
                 .sum()
                 .compute()).unstack(level=1)

    for level in range(1, 5):
        table['total_jam_length_level_{}'.format(level)] = by_levels[('length', level)]
    table['tci'] = mean_tci_geog(streets, 'date', 'street', 'length', working_days)
    return table.add_suffix(year)


def get_summary_statistics_city(ddf, year, working_days):
    table = (ddf.groupby('city')['uuid']
             .nunique()
             .to_frame('number_of_jams')
             .compute())
    table['total_jam_length'] = (ddf.groupby('city')['length']
                                 .sum()
                                 .compute()) / 1000
    by_levels = (ddf.groupby(['city', 'level'])[['length']]
                 .sum()
                 .compute()).unstack(level=1)

    for level in range(1, 5):
        table['total_jam_length_level_{}'.format(level)] = by_levels[('length', level)]
    table['tci'] = mean_tci_geog(ddf, 'date', 'city', 'length', working_days)

    return table.add_suffix(year)

def line_to_segments(x):
    '''Break linestrings into individual segments'''
    l = x[11:-1].split(', ')
    l1 = l[:-1]
    l2 = l[1:]
    points = list(zip(l1, l2))
    return ['LineString('+', '.join(elem)+')' for elem in points]

def get_jam_count_per_segment(df):
    '''Count how many jams occured in one segment'''
    df['segments'] = df['geoWKT'].apply(lambda x: line_to_segments(x))
    df_exp = df.explode('segments')
    segment_count = df_exp.groupby('segments').size().reset_index()
    segment_count.rename(columns={0: 'jam_count'}, inplace=True)
    segment_count['geometry'] = segment_count['segments'].apply(wkt.loads)
    segment_count_gdf = gpd.GeoDataFrame(segment_count, crs='epsg:4326', geometry=segment_count['geometry'])
    return segment_count_gdf

def remove_last_comma(name):
    if name[-2:] == ', ':
        return name[:-2]
    else:
        return name
def harmonize_data(table):
    table.reset_index(inplace=True)
    table['city'] = table['city'].apply(lambda x: remove_last_comma(x))
    table.set_index('city', inplace=True)