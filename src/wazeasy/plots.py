from datetime import datetime as dt
import itertools
import pandas as pd
from wazeasy import utils
import seaborn as sns
import matplotlib.pyplot as plt
sns.set()

def jams_per_day(data, save_fig = False):
    jams_per_day = data.groupby('date')['uuid'].nunique().compute()
    plt.figure(figsize=(15, 7))
    plt.plot(jams_per_day.index, jams_per_day)
    plt.xticks(rotation=45)
    plt.xlabel('Date')
    plt.ylabel('Number of Jams')
    plt.title('Number of Jams per day')
    plt.show()
    if save_fig:
        plt.savefig('./images/jams_by_day.png', bbox_inches='tight')
    plt.close()

def jams_per_day_per_level(data, save_fig = False):
    jams_per_day_per_level = (data.groupby(['date', 'level'])['uuid'].nunique().compute()).reset_index()
    plt.figure(figsize=(15, 7))
    colors_by_level = {1: '#FFD700', 2: '#FFA500', 3: '#FF4500', 4: '#FF0000'}
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
    data['region'] = 'Baghdad'
    tci = utils.tci_by_period_geography(data, ['date'], ['region'], 'length')
    tci.reset_index(inplace = True)
    plt.figure(figsize=(15, 7))
    plt.plot(tci['date'], tci['tci'])
    plt.xticks(rotation=45)
    plt.xlabel('Date')
    plt.ylabel('TCI')
    plt.title('Daily TCI')
    plt.show()
    if save_fig:
        plt.savefig('./images/daily_tci.png', bbox_inches='tight')
    plt.close()

#TODO: generalize the groups. Introduce them in the arguments
def hourly_tci_by_month (data, date_start, date_end, combination_year_month, group, save_fig = False):
    hourly_tci = utils.tci_by_period_geography(data, ['date', 'hour'], ['region'], 'length')
    hourly_tci.reset_index(inplace=True)
    dates = (pd.date_range(start=date_start, end=date_end, freq='D')).date
    all_idx = list(itertools.product(dates, list(range(0, 24))))
    hourly_tci.set_index(['date', 'hour'], inplace=True)
    hourly_tci = hourly_tci.reindex(all_idx).copy()
    hourly_tci.reset_index(inplace=True)
    hourly_tci['year'] = hourly_tci['date'].apply(lambda x: x.year)
    hourly_tci['month'] = hourly_tci['date'].apply(lambda x: x.month)
    hourly_tci['region'] = 'Baghdad'
    hourly_tci.fillna(0, inplace=True)
    groups = {0: 'Mon-Tues-Wed',
              1: 'Mon-Tues-Wed',
              2: 'Mon-Tues-Wed',
              3: 'Sun-Thu',
              4: 'Fri-Sat',
              5: 'Fri-Sat',
              6: 'Sun-Thu'}
    hourly_tci['dow'] = hourly_tci['date'].apply(lambda x: x.weekday())
    hourly_tci['group'] = hourly_tci['dow'].map(groups)
    hourly_tci_by_group_month = hourly_tci.groupby(['year', 'month', 'hour', 'group'])['tci'].mean().reset_index()
    fig, axes = plt.subplots(nrows=6, ncols=2, figsize=(10, 25))

    col = 0
    row = 0
    for y, m in combination_year_month:
        month = hourly_tci_by_group_month[(hourly_tci_by_group_month['year'] == y) &
                                          (hourly_tci_by_group_month['month'] == m) &
                                          (hourly_tci_by_group_month['group'] == group)]
        ax = axes[row, col]  # Access each subplot
        ax.plot(month.hour, month.tci)
        ax.set_title('{} - {}'.format(y, m))
        col += 1
        if col > 1:
            col = 0
            row += 1
    plt.show()
    if save_fig:
        plt.savefig('./images/hourly_tci.png', bbox_inches='tight')
    plt.close()



