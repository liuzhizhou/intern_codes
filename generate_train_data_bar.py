'''
这个脚本的输入参数是开始日期和结束日期，可以将.h5文件读入，并采样，生成训练所需要的数据。

采样的比率；读数据的路径；输出的路径 需要自行在脚本的代码中修改。
'''

import pandas as pd
import numpy as np
import time

from shennong.stk import stream,bar
import shennong.utils.symbol as cn_symbol
from shennong.utils import trading_days

import sys


print('Start date: '+sys.argv[1])
print('End date: '+sys.argv[2])

start = sys.argv[1]
end = sys.argv[2]


SYMBOL = cn_symbol.load(region='cn',product='ashare')

dates = trading_days.load(start_datetime = start, 
                        end_datetime  = end,
                        region='cn', product='ashare')

def drop_duplicated(df):
    return df[~df.index.duplicated(keep='first')]

train_data_list=[]
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
            label=drop_duplicated(dict_dfs_Y[stock]).iloc[:,3]*1000
            
            # start normolizing the shape of label
            start1 = date+" 09:31:30"
            end1 = date+" 11:29:59"
            start2 = date+" 13:00:00"
            end2 = date+" 14:59:59"
            reindex_1s = pd.date_range(start = start1, end = end1, freq="s").append(pd.date_range(start = start2,end = end2, freq="s"))
            label_reindex=label.reindex(reindex_1s)
            label_reindex.fillna(method='bfill')
            reindex_3s = pd.date_range(start = start1, end = end1, freq="3S").append(pd.date_range(start = start2,end = end2, freq="3S"))
            label = label_reindex.reindex(reindex_3s)           
             # end normolizing the shape of label
                
            entire=pd.concat([dict_dfs_X[stock],label],axis=1)
            dict_dfs_XY[stock] = entire.sample(frac=0.05)
            len_ += len(dict_dfs_XY[stock])

    np_XY = np.empty([len_,70])
    count_index=0
    for stock in dict_dfs_XY.keys():
        len_stock = len(dict_dfs_XY[stock])
        np_XY[count_index:count_index+len_stock,:]=dict_dfs_XY[stock].to_numpy()
        count_index+=len_stock

    mask1 = np.any(np.isnan(np_XY), axis=1) # 存在nan的行
    mask2 = np.all(np_XY==0,axis=1) # 全 0 的行
    data = np_XY[~(mask1|mask2)]
    num_rows+=len(data)
    train_data_list.append(data)
    t2=time.time()
    print('Day {} completed. Time Needed for it: {}'.format(date,round(t2-t1,2)))
    
train_data = np.empty([num_rows,70])
rows=0
for data in train_data_list:
    len_data = len(data)
    train_data[rows:rows+len_data,:] = data
    rows+=len_data
    
np.save('./documents/bar/dataset'+start+'_'+end+'.npy',train_data)
    
    
    
    
    
    
    
    
    
    
    
    
    