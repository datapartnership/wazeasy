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


# def calculate_daily_tci(data):
#     return data.groupby(['date', 'poly_idx'])[['length_in_polygon']].sum()
#
# def calculate_daily_delay(data):
#     return data.groupby(['date', 'poly_idx'])[['delay']].sum()
#
# def mean_tci_period(data, start, end, days, path_polygons, city, name):
#     data_period = filter_period(data.copy(), start, end, days)
#     data_period = get_polygons(data_period.copy(), path_polygons, city)
#     tci = calculate_daily_tci(data_period.copy())
#     tci = all_idx_date_poly(start, end, tci, path_polygons, days)
#     tci_mean = tci.groupby(['poly_idx'])[['length_in_polygon']].mean()
#     tci_mean.rename(columns = {'length_in_polygon':'TCI_{}'.format(name)}, inplace = True)
#     tci_mean['TCI_share_{}'.format(name)] = tci_mean['TCI_{}'.format(name)]/tci_mean['TCI_{}'.format(name)].sum()
#     tci_mean['TCI_median_{}'.format(name)] = tci.groupby(['poly_idx'])[['length_in_polygon']].median()
#     return tci_mean
#
# def mean_delay_period(data, start, end, days, path_polygons, city, name):
#     data_period = filter_period(data.copy(), start, end, days)
#     data_period = get_polygons(data_period.copy(), path_polygons, city)
#     delay = calculate_daily_delay(data_period.copy())
#     delay = all_idx_date_poly(start, end, delay, path_polygons, days)
#     delay_mean = delay.groupby(['poly_idx'])[['delay']].mean()
#     delay_mean.rename(columns = {'delay':'delay_{}'.format(name)}, inplace = True)
#     delay_mean['delay_share_{}'.format(name)] = delay_mean['delay_{}'.format(name)]/delay_mean['delay_{}'.format(name)].sum()
#     delay_mean['delay_median_{}'.format(name)] = delay.groupby(['poly_idx'])[['delay']].median()
#     return delay_mean
#
# def calculate_hourly_tci(data, city, path_results, start, end, path_polygons, name_poly, days):
#     data_period = filter_period(data.copy(), start, end, days)
#     data_period = get_polygons(data_period.copy(), path_polygons, city)
#     tci = data_period.groupby(['date', 'hour'])[['length_in_polygon']].sum()
#     tci.rename(columns = {'length_in_polygon': 'TCI'}, inplace = True)
#     tci = all_idx_date_hour_poly(start, end, tci, path_polygons, days)
#     df = tci.groupby(['hour'])[['TCI']].mean()
#     df.to_csv(path_results + 'hourly/hourly_tci_{}_{}_{}_{}.csv'.format(start, end, city, name_poly))
#     if name_poly == 'city':
#         df.plot(kind = 'bar')
#         plt.title('Hourly mean TCI between {} and {}'.format(start, end))
#         plt.xlabel('Hour')
#         plt.ylabel('TCI')
#         plt.savefig(path_results + 'images/hourly_tci_{}_{}_{}_{}.png'.format(start, end, city, name_poly), bbox_inches = 'tight')
#         plt.close()
#
# def calculate_delta_tci(df, col_start, col_end):
#     df['delta_{}-{}'.format(col_end, col_start)] = (df[col_end]/df[col_start]- 1) * 100
#     df['delta_{}-{}'.format(col_end, col_start)].fillna(0, inplace = True)
#     df.replace(np.inf, 1000000000000, inplace = True)
#
# def calculate_abs_delta_tci(df, col_start, col_end):
#     if df[[col_start, col_end]].isnull().values.any():
#         raise ValueError('There are null_values')
#     df['delta_abs_{}-{}'.format(col_end, col_start)] = df[col_end] - df[col_start]
#
# def all_idx_date_poly(start, end, df, path_polygons,days):
#     working_days = pd.read_excel('/home/sol/Documents/wb/waze/data/Dias habiles.xlsx', dtype = {'Fecha':'str'})
#     working_days['Fecha'] = working_days['Fecha'].apply(lambda x: x[:10])
#     working_days = working_days[working_days['Día Hábil'] == 1].copy()
#     all_dates = working_days[(working_days['Fecha']>=start)&(working_days['Fecha']<=end)&(working_days['DíaSemana'].isin(days))]['Fecha']
#     polygons = gpd.read_file(path_polygons)
#     all_polygons = list(range(len(polygons)))
#     idx = pd.MultiIndex.from_tuples(list(itertools.product(all_dates,all_polygons)), names = ['date', 'poly_idx'])
#     df = df.reindex(idx, fill_value = 0)
#     df.reset_index(inplace = True)
#     df = pd.merge(df, working_days[['Fecha', 'DíaSemana']], left_on = 'date', right_on = 'Fecha', how = 'left')
#     df.rename(columns = {'DíaSemana':'dow'}, inplace = True)
#     return df
#
# def all_idx_date_hour(start, end, df):
#     working_days = pd.read_excel('/home/sol/Documents/wb/waze/data/Dias habiles.xlsx', dtype = {'Fecha':'str'})
#     working_days['Fecha'] = working_days['Fecha'].apply(lambda x: x[:10])
#     working_days = working_days[working_days['Día Hábil'] == 1].copy()
#     all_dates = working_days[(working_days['Fecha']>=start)&(working_days['Fecha']<=end)]['Fecha']
#     all_hours = list(range(0, 24))
#     idx = pd.MultiIndex.from_tuples(list(itertools.product(all_dates,all_hours)), names = ['date', 'hour'])
#     df = df.reindex(idx, fill_value = 0)
#     df.reset_index(inplace = True)
#     df = pd.merge(df, working_days[['Fecha', 'DíaSemana']], left_on = 'date', right_on = 'Fecha', how = 'left')
#     df.rename(columns = {'DíaSemana':'dow'}, inplace = True)
#     return df
#
# def all_idx_date_hour_poly(start, end, df, path_polygons, days):
#     working_days = pd.read_excel('/home/sol/Documents/wb/waze/data/Dias habiles.xlsx', dtype = {'Fecha':'str'})
#     working_days['Fecha'] = working_days['Fecha'].apply(lambda x: x[:10])
#     working_days = working_days[working_days['Día Hábil'] == 1].copy()
#     all_dates = working_days[(working_days['Fecha']>=start)&(working_days['Fecha']<=end)&(working_days['DíaSemana'].isin(days))]['Fecha']
#     all_hours = list(range(0, 24))
#     polygons = gpd.read_file(path_polygons)
#     all_polygons = list(range(len(polygons)))
#     idx = pd.MultiIndex.from_tuples(list(itertools.product(all_dates, all_hours,all_polygons)), names = ['date', 'hour', 'poly_idx'])
#     df= df.reindex(idx, fill_value = 0)
#     df.reset_index(inplace = True)
#     df = pd.merge(df, working_days[['Fecha', 'DíaSemana']], left_on = 'date', right_on = 'Fecha', how = 'left')
#     df.rename(columns = {'DíaSemana':'dow'}, inplace = True)
#     return df
#
# def jams_by_day(data, path_results, city, days):
#     '''Plot the number of jams per day'''
#     data = data[data['DíaSemana'].isin(days)].copy()
#     data.drop_duplicates(['uuid', 'date'], inplace = True)
#     number_jams = data.groupby('date').size().reset_index()
#     plt.figure(figsize=(15, 7))
#     plt.plot(number_jams['date'], number_jams[0])
#     plt.xticks(rotation = 45)
#     # plt.xlabel('Fecha')
#     # plt.ylabel('Número de Embotellamientos')
#     # plt.title('Número de embotellamientos por día')
#     plt.xlabel('Date')
#     plt.ylabel('Number of Jams')
#     plt.title('Number of Jams per day')
#     plt.savefig(path_results + 'images/jams_by_day_{}.png'.format(city), bbox_inches = 'tight')
#     plt.close()
#     number_jams.to_csv(path_results + 'jams/numer_jams_{}.csv'.format(city))
#
# def mean_jams(data, days, periods, path_results, city):
#     df = pd.DataFrame(index = periods.keys(), columns = ['mean_number_jams'])
#     for name, period in periods.items():
#         start = period['start']
#         end = period['end']
#         data_period = filter_period(data.copy(), start, end, days)
#         jams_per_day = data_period.groupby(['date','uuid']).size().reset_index().groupby('date').size()
#         df.loc[name, 'mean_number_jams'] = jams_per_day.mean()
#     plt.bar(df.index, df['mean_number_jams'])
#     for index, row in df.iterrows():
#         plt.text(index,row['mean_number_jams'], int(round(row['mean_number_jams'], 0)))
#     plt.title('Número medio de embotellamientos por período')
#     plt.ylabel('Número medio de embotellamientos')
#     plt.savefig(path_results + 'images/mean_jams_{}.png'.format(city), bbox_inches = 'tight')
#     plt.close()
#
# def calculate_percentage_speed_reduction(data):
#     data['ff_speed'] = data.apply(lambda row: 1/(-((row['delay']/(60*60))/(row['length']/1000)) + (1/row['speedKMH'])), axis = 1)
#     data['percentage_speed_reduction'] = - data.apply(lambda row: (row['speedKMH']-row['ff_speed'])/row['ff_speed'], axis = 1) #give a higer reduction to lower levels, reduction will always be negative
#     if len(data[data['speedKMH']>data['ff_speed']]) > 1:
#         print('There are {} registers with issues with the data, we are imputing the reduction for them'.format(len(data[data['speedKMH']>data['ff_speed']])))
#         mapping = {1: 0.295, 2: 0.495, 3: 0.695, 4: 0.895}
#         data.loc[data['speedKMH']>data['ff_speed'], 'percentage_speed_reduction'] = data.loc[data['speedKMH']>data['ff_speed']]['level'].apply(lambda x: mapping[x])
#     return data
#
# def calculate_daily_tci_speed_adjusted(data):
#     data['weighted_length'] = data['length_in_polygon']*data['percentage_speed_reduction']
#     return data.groupby(['date', 'poly_idx'])[['weighted_length']].sum()
#
#
# def mean_tci_period_speed_reduction(data, start, end, days, path_polygons, city, name):
#     data_period = filter_period(data.copy(), start, end, days)
#     data_period = get_polygons(data_period.copy(), path_polygons, city)
#     data_period = calculate_percentage_speed_reduction(data_period.copy())
#     tci = calculate_daily_tci_speed_adjusted(data_period.copy())
#     tci = all_idx_date_poly(start, end, tci, path_polygons, days)
#     tci_mean = tci.groupby(['poly_idx'])[['weighted_length']].mean()
#     tci_mean.rename(columns = {'weighted_length':'TCIw_{}'.format(name)}, inplace = True)
#     return tci_mean
#
# def mean_tci_by_day(data, path_results, city, days, path_polygons, start, end, smoothing_factor):
#     data_period = filter_period(data.copy(), start, end, days)
#     data_period = get_polygons(data_period.copy(), path_polygons, city)
#     tci = calculate_daily_tci(data_period.copy())
#     tci.reset_index(inplace = True)
#     if len(tci.poly_idx.unique()) != 1:
#         raise ValueError('Problem with poly indexes')
#     tci_max = tci.length_in_polygon.max()
#     plt.figure(figsize=(15, 7))
#     plt.plot(tci['date'], tci['length_in_polygon'])
#     plt.plot([datetime(2019,11,4),datetime(2019,11,4)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2019,11,30),datetime(2019,11,30)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2020,11,4),datetime(2020,11,4)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2020,11,30),datetime(2020,11,30)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2021,11,4),datetime(2021,11,4)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2021,11,30),datetime(2021,11,30)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2022,11,4),datetime(2022,11,4)],[0,tci_max], c = 'orange')
#     plt.plot([datetime(2022,11,30),datetime(2022,11,30)],[0,tci_max], c = 'orange')
#     plt.xticks(rotation = 45)
#     plt.xlabel('Fecha')
#     plt.ylabel('TCI')
#     plt.title('TCI por día')
#     plt.savefig(path_results + 'images/tci_by_day_{}.png'.format(city), bbox_inches = 'tight')
#     plt.close()
#
#     add_spline(tci, start, end, smoothing_factor, tci_max, path_results, city)
#
# def average_delay(data, city, path_results, path_polygons, name_poly, start_baseline, end_baseline, start, end):
#     delay_baseline = utils.calculate_daily_delay(utils.filter_period(data, start_baseline, end_baseline))
#     delay = utils.calculate_daily_delay(utils.filter_period(data, start, end))
#     delay_baseline = utils.all_idx_date_poly(start_baseline, end_baseline, delay_baseline, path_polygons)
#     delay = utils.all_idx_date_poly(start, end, delay, path_polygons)
#
#     df = delay[delay['dow'].isin([2,3,4])].groupby(['poly_idx'])[['delay']].mean()
#     df.rename(columns = {'delay':'delay_f'}, inplace = True)
#     df['delay_b'] = delay_baseline[delay_baseline['dow'].isin([2,3,4])].groupby(['poly_idx'])['delay'].mean()
#     df.reset_index(level = 0, inplace = True)
#     df['delta'] = (df['delay_f']/df['delay_b']- 1) * 100
#     df['delta'].fillna(0, inplace = True)
#     df = df.replace(np.inf, 1000000000000)
#     df = df[(df['delay_b']>0)&(df['delay_f']>0)].copy()
#     df.to_csv(path_results + 'delta/deltadelay_{}_{}_{}_{}_{}_{}.csv'.format(start, end, start_baseline, end_baseline, city, name_poly), index = False)
#     shp_name = path_results + 'delta/deltadelay_{}_{}_{}_{}_{}_{}.gpkg'.format(start, end, start_baseline, end_baseline, city, name_poly)
#     columns_to_transfer = ['delta', 'delay_f', 'delay_b']
#     utils.create_shp(path_polygons, df, city, columns_to_transfer, shp_name)