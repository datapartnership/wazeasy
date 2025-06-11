from datetime import datetime as dt
import itertools
import pandas as pd
from wazeasy import utils
import seaborn as sns
import matplotlib.pyplot as plt
sns.set()
import plotly.graph_objects as go

def jams_per_day(data, save_fig = False):
    '''
    Plot the number of unique traffic jams per day.

    Parameters:
    - data (DataFrame): A Dask DataFrame containing 'date' and 'uuid' columns.
    - save_fig (bool): If True, saves the plot as a PNG file. Default is False.

    Returns:
    - None: Displays the plot and optionally saves it to a file.
    '''
    jams_per_day = data.groupby('date')['uuid'].nunique().compute().reset_index()
    jams_per_day.sort_values('date', inplace = True)
    plt.figure(figsize=(15, 7))
    plt.plot(jams_per_day['date'], jams_per_day['uuid'])
    plt.xticks(rotation=45)
    plt.xlabel('Date')
    plt.ylabel('Number of Jams')
    plt.title('Number of Jams per day')
    plt.show()
    if save_fig:
        plt.savefig('./images/jams_by_day.png', bbox_inches='tight')
    plt.close()

def jams_per_day_per_level(data, save_fig = False):
    '''
    Plot the number of unique traffic jams per day, grouped by congestion level.

    Parameters:
    - data (DataFrame): A Dask DataFrame containing 'date', 'level', and 'uuid' columns.
    - save_fig (bool): If True, saves the plot as a PNG file. Default is False.

    Returns:
    - None: Displays the plot and optionally saves it to a file.
    '''
    jams_per_day_per_level = (data.groupby(['date', 'level'])['uuid'].nunique().compute()).reset_index()
    jams_per_day_per_level.sort_values('date', inplace = True)
    plt.figure(figsize=(15, 7))
    colors_by_level = {1: '#FFD700', 2: '#FFA500', 3: '#FF4500', 4: '#FF0000', 5: '#4DFF00'}
    # colors_by_level = {1: '#0000FF', 2: '#8A2BE2', 3: '#FF00FF', 4: '#FF0000'}
    for level, color in colors_by_level.items():
        data = jams_per_day_per_level[jams_per_day_per_level['level']==level].copy()
        data.set_index('date', inplace = True)
        plt.plot(data.index, data['uuid'], color = color, label = level)
    plt.xticks(rotation=45)
    plt.xlabel('Date')
    plt.ylabel('Number of Jams')
    plt.title('Number of Jams per day per level')
    plt.legend()
    plt.show()
    if save_fig:
        plt.savefig('./images/jams_by_day_by_level.png', bbox_inches='tight')
    plt.close()


def jams_monthly_aggregated(data, save_fig = False):
    '''
    Plot the number of unique traffic jams aggregated by month.

    Parameters:
    - data (DataFrame): A Dask DataFrame containing 'year', 'month', and 'uuid' columns.
    - save_fig (bool): If True, saves the plot as a PNG file. Default is False.

    Returns:
    - None: Displays the plot and optionally saves it to a file.
    '''
    jams_per_month = data.groupby(['year', 'month'])['uuid'].nunique().compute()
    jams_per_month = jams_per_month.reset_index()
    jams_per_month['month_with_year'] = jams_per_month.apply(lambda row: dt.strptime('{}-{}-{}'.format(row['year'], row['month'], '15'), '%Y-%m-%d'), axis = 1)
    jams_per_month.set_index('month_with_year', inplace = True)
    plt.figure(figsize=(15, 7))
    plt.bar(jams_per_month.index, jams_per_month.uuid, width=10)
    plt.xticks(jams_per_month.index, rotation=45)
    plt.xlabel('Month')
    plt.ylabel('Number of Jams')
    plt.title('Number of Jams per month')
    plt.show()
    if save_fig:
        plt.savefig('./images/jams_by_month.png', bbox_inches='tight')
    plt.close()

def regional_tci_per_day(data, save_fig = False):
    '''
    Plot the daily Traffic Congestion Index (TCI) aggregated by region.

    Parameters:
    - data (DataFrame): A Dask or Pandas DataFrame containing 'date', 'region', and 'length' columns.
    - save_fig (bool): If True, saves the plot as a PNG file. Default is False.

    Returns:
    - None: Displays the plot and optionally saves it to a file.
    '''
    tci = utils.tci_by_period_geography(data, ['date'], ['region'], 'length')
    tci.reset_index(inplace = True)
    tci.sort_values('date', inplace = True)
    plt.figure(figsize=(15, 7))
    plt.plot(tci['date'], tci['tci'])
    plt.xticks(rotation=45)
    plt.xlabel('Date')
    plt.ylabel('TCI')
    plt.title('Regional Daily TCI')
    plt.show()
    if save_fig:
        plt.savefig('./images/daily_tci.png', bbox_inches='tight')
    plt.close()

def hourly_tci_by_month (ddf, geog, combination_year_month, dow, group_name, save_fig = False):
    '''
    Plot the hourly Traffic Congestion Index (TCI) for selected months.

    Parameters:
    - ddf (DataFrame): A Dask DataFrame containing traffic data.
    - geog (str): The geographic column to group by.
    - combination_year_month (list of tuples): List of (year, month) pairs to plot.
    - dow (list): Days of the week to include (e.g. [0, 1, 2, 3, 4] for weekdays).
    - group_name (str): Label used in the plot title (e.g. region name).
    - save_fig (bool): Unused currently. Reserved for future implementation.

    Returns:
    - None: Displays the interactive Plotly figure.
    '''
    fig = go.Figure()
    for year, month in combination_year_month:
        data = utils.monthly_hourly_tci(ddf, geog, ['date', 'hour'], year, month, 'length', dow = dow).reset_index()
        fig.add_trace(go.Scatter(x=data.hour, y=data.tci, mode='lines', name=f'{year}{month}', visible=True))
    fig.update_layout(title=f'Monthly Hourly TCI - {group_name}', 
                      xaxis_title='Hour', 
                      yaxis_title='TCI', 
                      legend_title='Dates', 
                      hovermode='x unified') 
    fig.show()
    # TODO: implement the save_fig option. Plotly has options for saving static and interactive figures. 



