import pandas as pd
import geopandas as gpd
from datetime import timedelta
from datetime import datetime as dt
import dask.dataframe as dd
import dask_geopandas
import itertools
from shapely import wkt, Polygon
import json 
import shapely
import h3
from dask import delayed, compute


def load_data(main_path, year, month, sotrage_options = None, file_type = 'csv'):
    # TODO: add documentation to the function
    if file_type == 'csv':
        load_data_csv(main_path, year, month)
    elif file_type == 'parquet':
        load_data_parquet(main_path, year, month, sotrage_options)

#TODO write function for reading cvs files into dask data frame
# def load_data_csv(main_path, year, month):
#     # TBD
#     return 

def load_data_parquet(main_path, year, month, storage_options):
    # TODO: add documentation to the function
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

def assign_geography_to_jams(ddf):
    '''Assign a geography to each jam'''
    ddf['region'] = 'region'
    # TODO: Assign different geographies to each jam. Geographies will come from a config file. 

def remove_level5(ddf):
    return ddf[ddf['level']!=5]

def time_attributes(df):
    '''Calculate year, month, date and hour for each jam record'''
    df['year'] = df['local_time'].dt.year
    df['month'] = df['local_time'].dt.month
    df['date'] = df['local_time'].dt.date
    df['hour'] = df['local_time'].dt.hour

def tci_by_period_geography(ddf, period, geography, agg_column, dow = None, custom_dates = None):
    '''Returns Traffic Congestion Index'''
    if dow is not None:
        unique_dates = ddf[["date"]].drop_duplicates().compute()['date'].values
        filtered_dates = filter_date_range_by_dow(unique_dates, dow)        
        ddf = ddf[ddf['date'].isin(filtered_dates)]
    elif custom_dates is not None:
        ddf = ddf[ddf['date'].isin(cutom_dates)]

    tci = ddf.groupby(period + geography)[[agg_column]].sum().compute()  
    tci.rename(columns = {agg_column: 'tci'}, inplace = True)    
    return tci

def mean_hourly_tci(ddf, period, geog, agg_column, dates_of_interest):
    '''Returns the mean TCI across the dates of interest'''
    daily_tci = tci_by_period_geography(ddf, period, geog, agg_column)
    geogs = list(set(daily_tci.reset_index()[geog])) 
    idxs = pd.MultiIndex.from_tuples(list(itertools.product(dates_of_interest, list(range(24)), geogs)),
                                     names = period + geog)
    daily_tci = daily_tci.reindex(idxs, fill_value = 0)
    daily_tci.reset_index(inplace = True)
    return daily_tci.groupby(geog + ['hour'])['tci'].mean()

def mean_tci_geog(ddf, period, geog_id, dates, geogs, agg_column, projected_crs):
    '''Averages the TCI for each geography across a period of time '''
    ddf_filtered = ddf[ddf['date'].isin(dates)].copy()
    unique_jams_over_agg_geom = parallelized_overlay(ddf_filtered, geogs)
    jams_over_agg_geom = distribute_jams_over_aggregation_geom(unique_jams_over_agg_geom, ddf_filtered, projected_crs)
    tci = tci_by_period_geography(jams_over_agg_geom, period, [geog_id], agg_column)
    geog_ids = list(set(geogs[geog_id]))
    idxs = pd.MultiIndex.from_tuples(list(itertools.product(dates, geog_ids)),
                                     names = period + [geog_id])
    tci = tci.reindex(idxs, fill_value = 0)
    tci.reset_index(inplace = True)
    return tci.groupby(geog_id)['tci'].mean()

def filter_date_range_by_dow(date_range, dow):
    '''Filter a date range by days of the wee
        dow: list of integers representing the days of the week to consider (0 = Monday, 6 = Sunday)'''
    filtered_dates = []
    for date in date_range:
        if date.weekday() in dow:
            filtered_dates.append(date)
    return filtered_dates

def monthly_hourly_tci(ddf, geog, period, year, month, agg_column, dow = None):
    '''Returns the monthly TCI    
        dow: list of integers representing the days of the week to consider (0 = Monday, 6 = Sunday)'''
    start_date = dt(year, month, 1)
    if month == 12:
        end_date = dt(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = dt(year, month + 1, 1) - timedelta(days=1)
    date_range = pd.date_range(start_date, end_date)
    dates_of_interest = filter_date_range_by_dow(date_range, dow)    
    return mean_hourly_tci(ddf, period, geog, agg_column, dates_of_interest)

   
def create_gdf(ddf):
    '''Create a dask-geopandas GeoDataFrame from a dask DataFrame'''
    ddf['geometry'] = dask_geopandas.from_wkt(ddf['geoWKT'], crs='epsg:4326')
    gddf = dask_geopandas.from_dask_dataframe(ddf, geometry='geometry')
    gddf = gddf.set_crs("EPSG:4326")
    return gddf

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

def obtain_hexagons_for_area(area, resolution):
    '''Given an Area of Operation, create a georeferrenced layer of h3 hexagons'''
    geo_json = json.loads(shapely.to_geojson(area))
    hexagons = list(h3.polyfill(geo_json, resolution))
    hex_geometries = [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in hexagons]
    hex_ids = [h for h in hexagons]
    hex_gdf = gpd.GeoDataFrame({'hex_id': hex_ids, 'geometry': hex_geometries}, crs="EPSG:4326")
    return hex_gdf

def obtain_unique_jams_linestrings(ddf):
    '''Get unique jams linestrings to avoid overlaying the same linestring several times'''
    unique_geo = ddf[["geoWKT"]].drop_duplicates().reset_index(drop=True).reset_index()
    unique_geo = create_gdf(unique_geo)
    return unique_geo

def overlay_group(group, hexagons):
    '''Overlay between layers to be used when delaying processes'''
    result = gpd.overlay(group, hexagons, how = 'intersection')
    return result

def parallelized_overlay(ddf, aggregation_geog):
    '''Prallelize overlay by groups over some geometry'''
    unique_geo = obtain_unique_jams_linestrings(ddf).persist()
    delayed_process_group = delayed(overlay_group)
    groups = [unique_geo.get_partition(i) for i in range(unique_geo.npartitions)]
    tasks = [delayed_process_group(group, aggregation_geog) for group in groups]
    results = compute(*tasks)
    final_result = gpd.GeoDataFrame(pd.concat(results, ignore_index=True))
    return final_result

def distribute_jams_over_aggregation_geom(gddf, ddf, projected_crs):
    gddf = gddf.to_crs(projected_crs)
    gddf['length_in_geom'] = gddf['geometry'].length
    df = dd.from_pandas(gddf)
    merge = ddf.merge(df, left_on = 'geoWKT', right_on = 'geoWKT', how = 'left')   
    return merge

