'''
本脚本与`generate_train_data.py`基本一致，不同的是生成的训练数据是一天一天的，并且不会采样。
'''

import pandas as pd
import numpy as np
import time

from shennong.stk import stream,bar
import shennong.utils.symbol as cn_symbol
from shennong.utils import trading_days

start = '2021-08-01'
end = '2021-09-30'

SYMBOL = cn_symbol.load(region='cn',product='ashare')

dates = trading_days.load(start_datetime = start, 
                        end_datetime  = end,
                        region='cn', product='ashare')

def drop_duplicated(df):
    return df[~df.index.duplicated(keep='first')]

num_rows=0
for date in dates:
    t1=time.time()
    dict_dfs_X = stream.load(start_datetime = date+ " 09:31:30",
                    end_datetime = date+ " 14:57:00",
                    key_group_name = 'freq_3s_MA',
                    symbol_list = SYMBOL,
                    region='cn', product='ashare', freq='tick',
                    load_root = '/mnt/sda/NAS-203/ShareFolder2/zhizhou/', 
                    verbose=True)
    dict_dfs_Y = stream.load(start_datetime = date+ " 09:31:30",
                end_datetime = date+ " 14:57:00",
                key_group_name = 'label_wutianhao_ret__second_handle_limit',
                symbol_list = SYMBOL,
                region='cn', product='ashare', freq='tick',
                load_root = '/mnt/sda/NAS/AllData/', verbose=True)
    dict_dfs_XY={}
    len_=0
    for stock in SYMBOL:
        if stock in set(dict_dfs_X.keys()).intersection(dict_dfs_Y.keys()):
            entire=pd.concat([dict_dfs_X[stock],drop_duplicated(dict_dfs_Y[stock]).iloc[:,3]*1000],axis=1)
            dict_dfs_XY[stock] = entire.sample(frac=0.05)
            len_ += len(dict_dfs_XY[stock])

    np_XY = np.empty([len_,70])
    count_index=0
    for stock in dict_dfs_XY.keys():
        len_stock = len(dict_dfs_XY[stock])
        np_XY[count_index:count_index+len_stock,:]=dict_dfs_XY[stock].to_numpy()
        count_index+=len_stock

    mask1 = np.any(np.isnan(np_XY), axis=1)
    mask2 = np.all(np_XY==0,axis=1)
    data = np_XY[~(mask1|mask2)]
    num_rows+=len(data)
    np.save('./documents/bar/data_set_test'+date+'.npy',data)
    t2=time.time()
    print('Day {} completed. Time Needed for it: {}'.format(date,round(t2-t1,2)))