'''
将当天每只股票成交间隔的中位数输出到文件夹中
'''
from shennong.stk import stream
import shennong.utils.symbol as cn_symbol
from datetime import datetime
import pandas as pd
import numpy as np

def make_zero(s):
    if len(s) == 1:
        s = '0'+s
    return str(s)
    
def get_today():
    year = datetime.today().date().year
    month = datetime.today().date().month
    day = datetime.today().date().day
    return '{}-{}-{}'.format(year,make_zero(str(month)),make_zero(str(day)))

def get_abs_time(f_time):
    s_time = str(int(f_time))
    ms = int(s_time[-3:])
    S = int(s_time[-5:-3])
    M = int(s_time[-7:-5])
    H = int(s_time[:-7])
    abstime = (H*60*60+M*60+S+ms*0.001)*1000
    return abstime

SYMBOL = cn_symbol.load(region='cn',product='ashare')
today=get_today()

data=stream.load(start_datetime = today+' 09:30:00',
                    end_datetime = today+' 14:57:00',
                    key_group_name = 'raw_wangbo_transaction__complete',
                    symbol_list = SYMBOL,
                    region='cn', product='ashare', freq='tick',
                    load_root = '/mnt/sda/NAS/AllData/', 
                    verbose=True)
                    
df_active = pd.DataFrame(index=SYMBOL,columns=['median delta t'])
for stock in list(data.keys()):
    print('Loading {}'.format(stock))
    abs_series = data[stock]['raw_wangbo_ntime'].apply(get_abs_time)
    diff_series = abs_series-abs_series.shift(1)
    df_active.loc[stock,'median delta t'] = diff_series.median()
df_active.dropna().to_csv('/mnt/sda/NAS-203/ShareFolder2/zhizhou/active_stocks/{}.csv'.format(today))