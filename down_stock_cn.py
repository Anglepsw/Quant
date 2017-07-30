# -*- coding: utf-8 -*-
"""
Created on 2017-7-30

@author: Sowill
"""

import tushare as ts
import time
import os
import matplotlib.pyplot as plt
import pandas as pd
import re


# 下载股票数据
def down_stock_date(stock_code, start_date, xtype):
    if xtype == 'D1':
        # 这个命令最多取到前3年
        df = ts.get_hist_data(stock_code, start=start_date, ktype='D')
    elif xtype == 'D2':
        # 这个命令可以取到上市时，但没有上面返回的信息丰富
        df = ts.get_k_data(stock_code, start=start_date, ktype='D')
    elif xtype == 'D3':
        # 这个命令可以去到上市时，而且可以选择复权,
        df = ts.get_h_data(stock_code, start=start_date)
        date = list([])
        for i in df.index:
            date.append(str(i).split(' ')[0])
        df['date'] = date
        print('\n')
    elif xtype == '51':
        # 获取5分钟K线
        df = ts.get_hist_data(stock_code, ktype='5')
    elif xtype == '52':
        df = ts.get_k_data(stock_code, ktype='5')
    elif xtype == 'W1':
        # 获取周K线
        df = ts.get_hist_data(stock_code, ktype='W')
    elif xtype == 'W2':
        df = ts.get_k_data(stock_code, ktype='W')
    return df


# 正则化检索式
def update_index(date, target_date):
    dateregex = re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s*(\d{1,2})?:?(\d{1,2})?:?(\d{1,2})?')
    mo = dateregex.search(date)
    mo1 = dateregex.search(target_date)
    if mo1.group(4) is None:  # target_date:2017-07-12
        date1 = '%04d-%02d-%02d' % (int(mo.group(1)), int(mo.group(2)), int(mo.group(3)))
    elif mo1.group(6) is None:  # target_date:2017-07-12 13:55
        date1 = '%04d-%02d-%02d %02d:%02d' % (
            int(mo.group(1)), int(mo.group(2)), int(mo.group(3)), int(mo.group(4)), int(mo.group(5)))
    else:  # target_date:2017-07-12 13:55:00
        if mo.group(6) is None:
            date1 = '%04d-%02d-%02d %02d:%02d:00' % (
                int(mo.group(1)), int(mo.group(2)), int(mo.group(3)), int(mo.group(4)), int(mo.group(5)))
        else:
            date1 = '%04d-%02d-%02d %02d:%02d:%02d' % (
                int(mo.group(1)), int(mo.group(2)), int(mo.group(3)), int(mo.group(4)), int(mo.group(5)),
                int(mo.group(6)))
    return date1


def down_date_to_csv(stock_code, xtype):
    if xtype == '51':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_5min1.csv'
    elif xtype == '52':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_5min2.csv'
    elif xtype == '53':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_5min3.csv'
    elif xtype == 'D1':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_day1.csv'
    elif xtype == 'D2':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_day2.csv'
    elif xtype == 'D3':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_day3.csv'
    elif xtype == 'W1':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_week1.csv'
    elif xtype == 'W2':
        fss = r'D:\PythonScript\Quant\\' + stock_code + '\\' + stock_code + '_kdata_week2.csv'
    file_exit = os.path.exists(fss)
    if file_exit == False:
        print('源文件不存在，直接创建:' + fss + '\n')
        stock_all = ts.get_stock_basics()
        stock_time = stock_all.loc[stock_code, 'timeToMarket']
        stock_time = str(stock_time)[0:4] + '-' + str(stock_time)[4:6] + '-' + str(stock_time)[6:8]
        df = down_stock_date(stock_code, start_date=stock_time, xtype=xtype)
        if 'date' in df.columns:
            df.index = df.date
            df.drop('date', axis=1, inplace=True)
        df = df.sort_index(ascending=True)
        df.to_csv(fss, encoding='gbk')
    else:
        print('已经存在' + fss + '\n')
        # 读入已有的文件并格式化
        df0 = pd.read_csv(fss, encoding='gbk')
        if 'date' in df0.columns:
            df0.index = df0.date
            df0.drop('date', axis=1, inplace=True)
        # 根据已有数据的最后日期开始下载新数据
        temp = str(df0.index[-1])
        stock_time = update_index(temp, '2017-01-01')
        df = down_stock_date(stock_code, start_date=stock_time, xtype=xtype)
        # 格式化df
        if 'date' in df.columns:
            df.index = df.date
            df.drop('date', axis=1, inplace=True)
        df = df.sort_index(ascending=True)
        end_date = df.index[-1]
        # 取出df0最新日期
        temp = df0.index[-1]
        begin_date = update_index(temp, end_date)
        # 向文件中写入新增数据
        if begin_date != end_date:
            df2 = df[begin_date:end_date]
            df2.drop(begin_date, axis=0, inplace=True)
            df2.to_csv(fss, encoding='gbk', mode='a', header=False)
            print('  写入新数据！\n')
            df0.append(df2)
        plt.figure()
        df0['close'].plot()


# ---------------Main函数
# 获取所有基本面
stock_all = ts.get_stock_basics()
# 股票代码
stock_code = '002415'
# 获取海康威视上市时间
stock_name = stock_all.loc[stock_code, 'name']
stock_time = stock_all.loc[stock_code, 'timeToMarket']
stock_time = str(stock_time)[0:4] + '-' + str(stock_time)[4:6] + '-' + str(stock_time)[6:8]
print(stock_code + stock_name + ' 上市时间：' + stock_time)
down_date_to_csv(stock_code, 'D1')
down_date_to_csv(stock_code, 'D2')
down_date_to_csv(stock_code, 'D3')
down_date_to_csv(stock_code, 'W1')
down_date_to_csv(stock_code, 'W2')
down_date_to_csv(stock_code, '51')
down_date_to_csv(stock_code, '52')
