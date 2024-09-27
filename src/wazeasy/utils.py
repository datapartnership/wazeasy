import pandas as pd
import geopandas as gpd
from datetime import timedelta
from datetime import datetime as dt
import dask.dataframe as dd
import itertools
from shapely import wkt


def tci_by_period_geography(ddf, period, geography, agg_column):
    '''Returns Traffic Congestion Index'''
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
    daily_tci.rename(columns = {'level_0': 'date', 'level_1': 'city'}, inplace = True)
    return daily_tci.groupby(geog)['tci'].mean()

def time_attributes(data, utc_loc):
    '''Converts time to datetime

        Args:
            utc_loc (pytz.BaseTzInfo): The local timezone region'''
    data['date_utc'] = data.ts.dt.tz_localize('UTC')
    data['date_utc'] = data.date_utc.dt.tz_convert(utc_loc)
    data['year'] = data['date_utc'].dt.year
    data['month'] = data['date_utc'].dt.month
    data['date'] = data['date_utc'].dt.date
    # data['date'] = data['date_utc'].dt.normalize()

def create_gdf(data, epsg, col):
    geometry = data[col].apply(wkt.loads)
    data_geo = gpd.GeoDataFrame(data, crs="EPSG:{}".format(epsg), geometry=geometry)
    return data_geo

def get_summary_statistics_street(df, street_name, year, working_days):
    street = df[df['street'].str.contains(street_name)]
    street = street[street['date'].isin(working_days)].copy()
    col_name = street_name + str(year)
    table = pd.DataFrame(index = ['number_of_jams', 'total_jam_length',
                           'total_jam_length_level_1', 'total_jam_length_level_2',
                           'total_jam_length_level_3', 'total_jam_length_level_4',
                                                  ],
                     columns = [col_name])
    table.loc['number_of_jams', col_name] = len(street.uuid.unique())
    table.loc['total_jam_length', col_name] = street.length.sum()/1000
    for level in range(1, 5):
        table.loc['total_jam_length_level_{}'.format(level), col_name] = (street[street['level']==level].length.sum())/1000

    daily_tci = tci_by_period_geography(street, 'date', 'street', 'lenght')
    table.loc['tci', col_name] = daily_tci.reset_index(level = 1).reindex(working_days)['tci'].mean()
    return table


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

def convert_to_local_time(df, utc_region):
    '''Convert date stored in string to datetime type
        Args:
            utc_loc (pytz.BaseTzInfo): The local timezone region'''
    df['ts'] = pd.to_datetime(df['ts'], utc = True)
    df['local_time'] = df['ts'].dt.tz_convert(utc_region)
    df['hour'] = df['local_time'].dt.hour

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