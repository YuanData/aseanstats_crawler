# -*- coding: utf-8 -*-
import datetime
import json
import logging
import os

import pandas as pd
import requests

import config as config
from utils.tools import try_except_log, time_log, args_time_log, logger
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.getLogger('urllib3').setLevel(logging.WARNING)

data_path = config.DATA_PATH
raw_data_path = config.RAW_DATA_PATH
reporter_all_path = config.REPORTER_ALL_PATH
partner_list_csv = os.path.join(data_path, 'partner_list.csv')
reporter_list_csv = os.path.join(data_path, 'reporter_list.csv')


def gen_partner_lst():
    res = requests.get('https://data.aseanstats.org/api/partner?class_code=HS2')
    # res = requests.get('https://data.aseanstats.org/api/commodity?class_code=HS6&q=85')  # 可取得content
    dic_lst = json.loads(res.text)
    df = pd.DataFrame(dic_lst)
    df = df[df['partner_code'] != '--']
    df.to_csv(partner_list_csv, index=False)
    return df


def gen_reporter_lst():
    res = requests.get('https://data.aseanstats.org/api/reporter')
    dic_lst = json.loads(res.text)
    df = pd.DataFrame(dic_lst)
    df.to_csv(reporter_list_csv, index=False)
    return df


def read_partner_lst():
    return pd.read_csv(partner_list_csv) if os.path.exists(partner_list_csv) else gen_partner_lst()


def read_reporter_lst():
    return pd.read_csv(reporter_list_csv) if os.path.exists(reporter_list_csv) else gen_reporter_lst()


@args_time_log
def download_by(tpl):
    reporter, partner, year = tpl
    r_dir = os.path.join(raw_data_path, reporter)
    os.makedirs(r_dir) if not os.path.exists(r_dir) else None
    r_y_dir = os.path.join(r_dir, '{r}_{y}'.format(r=reporter, y=year))
    os.makedirs(r_y_dir) if not os.path.exists(r_y_dir) else None

    data = {
        'Reporter': reporter,
        'Partner': partner,
        'Year': year,
        'Frequency': 'A',
        'Period': 'Annual',
        'Flow': 'X,M,T',
        'Commodity': 'HS6',
        'CommodityCode': '______',
        'Unit': 1,
    }
    res = requests.post('https://data.aseanstats.org/api/trade', data=data)
    if res.status_code == 200:
        dic_lst = json.loads(res.text)
        df = pd.DataFrame(dic_lst['Trade'])
        if dic_lst['Trade']:
            df.to_excel(os.path.join(r_y_dir, '{r}_{y}_{p}.xlsx'.format(r=reporter, y=year, p=partner)), index=False)


@try_except_log
@time_log
def download_all_partner(reporter, year_start, year_end):
    df_partner_lst = read_partner_lst()
    partner_code_lst = df_partner_lst['partner_code'].to_list()
    # partner_code_lst = ['AX', 'CN', ]

    tpl_lst = []
    for year in range(year_start, year_end + 1):
        for partner in partner_code_lst:
            tpl_lst.append((reporter, partner, year))
            # download_by(reporter=reporter, partner=partner, year=year)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures_dict = {executor.submit(download_by, tpl): tpl for tpl in tpl_lst}
        for future in as_completed(futures_dict):
            tpl = futures_dict[future]
            result = future.result()


@try_except_log
@time_log
def download_all_reporter(year_start, year_end):  # [2004, 2018]
    df_reporter_lst = read_reporter_lst()
    reporter_code_lst = df_reporter_lst['reporter_code'].to_list()
    # reporter_code_lst = ['VN', 'KH']
    for reporter in reporter_code_lst:
        download_all_partner(reporter=reporter, year_start=year_start, year_end=year_end)


@try_except_log
@time_log
def concat_reporter_files():
    for r_dir in os.listdir(raw_data_path):
        r_path = os.path.join(raw_data_path, r_dir)
        r_y_dir_lst = sorted([r_y for r_y in os.listdir(r_path)], reverse=True)
        y_lst = sorted([int(r_y.split('_')[1]) for r_y in r_y_dir_lst])
        df_concat = pd.DataFrame()
        year_flag = None
        row_count = 0
        for r_y_dir in r_y_dir_lst:
            logger.info(r_y_dir)
            r_y_path = os.path.join(r_path, r_y_dir)
            _ = y_lst.pop()
            year_start = int(r_y_dir.split('_')[1])
            if year_flag is None:
                year_flag = year_start
            df_r_y_all = for__r_y_p_xlsx__in__r_y_path(r_y_path)
            df_row = df_r_y_all.shape[0]
            row_count += df_row
            if row_count < 950000:
                df_concat = pd.concat([df_concat, df_r_y_all])
            else:
                df_to_excel_r_y_all(df_concat, r_dir, year_start + 1, year_flag)
                df_concat = df_r_y_all
                row_count = df_row
                year_flag = year_start
            if not y_lst:
                df_to_excel_r_y_all(df_concat, r_dir, year_start, year_flag)


def for__r_y_p_xlsx__in__r_y_path(r_y_path):
    df_r_y_all = pd.DataFrame()
    for r_y_p_xlsx in os.listdir(r_y_path):
        print(r_y_p_xlsx)
        df = pd.read_excel(os.path.join(r_y_path, r_y_p_xlsx))
        df_r_y_all = pd.concat([df_r_y_all, df])
    return df_r_y_all


def df_to_excel_r_y_all(df, reporter, y_start, y_end):
    df = df[[
        'Reporter',
        'Partner',
        'Year',
        'Period',
        'Flow',
        'Commodity Code',
        'Trade Value (US$)',
        'Commodity',
    ]]
    if y_start != y_end:
        r_y_all_xlsx = '{r}_{y_start}-{y_end}_all.xlsx'.format(r=reporter, y_start=y_start, y_end=y_end)
    else:
        r_y_all_xlsx = '{r}_{y_start}_all.xlsx'.format(r=reporter, y_start=y_start)

    df.to_excel(os.path.join(reporter_all_path, r_y_all_xlsx), index=False)


if __name__ == '__main__':
    download_all_reporter(year_start=2004, year_end=2018)
    concat_reporter_files()
    logger.info('*' * 80)
