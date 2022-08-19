'''
本脚本功能与'get_MA.py'类似，只不过读入的数据为xarray的结构。
'''

import shennong.utils.symbol as cn_symbol
from shennong.utils import trading_days
from shennong.stk import stream,bar 

import numpy as np
import pandas as pd
import time
import sys
from multiprocessing import Pool, Process
from itertools import product


SYMBOL = cn_symbol.load(region='cn',product='ashare')


# price_key 需要通过preclose转化称为百分比，或者千分比
price_keys = ['raw_ygb_symbol_rollbar_close_price__20',
           'raw_ygb_symbol_rollbar_high_price__20',
           'raw_ygb_symbol_rollbar_low_price__20']
# log不用动
log_keys = ['raw_ygb_symbol_rollbar_buy_turnover_log__20',
           'raw_ygb_symbol_rollbar_sell_turnover_log__20',
           'raw_ygb_symbol_rollbar_buy_count_log__20',
           'raw_ygb_symbol_rollbar_sell_count_log__20',
           'raw_ygb_symbol_rollbar_buy_turnover_mean_log__20',
           'raw_ygb_symbol_rollbar_sell_turnover_mean_log__20']
# weight_price可以不用动
weight_price_keys = ['raw_ygb_symbol_rollbar_weight_price__20',
           'raw_ygb_symbol_rollbar_weight_buy_price__20',
           'raw_ygb_symbol_rollbar_weight_sell_price__20']
# std,mean除以1000
std_mean_keys = ['raw_ygb_symbol_rollbar_buy_time_gap_std__20',
           'raw_ygb_symbol_rollbar_sell_time_gap_std__20',
           'raw_ygb_symbol_rollbar_buy_time_gap_mean__20',
           'raw_ygb_symbol_rollbar_sell_time_gap_mean__20']
# rollbar -1   这一类数据的特征是前一分钟为0，后面在1附近，在1附近的数据需要-1 * 1000
industry_rollbar_keys = ["raw_zhutong_industry_rollbar_weighted_high_price__20",
             "raw_zhutong_industry_rollbar_weighted_low_price__20",
             "raw_zhutong_industry_rollbar_weighted_close_price__20",
             "raw_zhutong_industry_rollbar_avg_high_price__20",
             "raw_zhutong_industry_rollbar_avg_low_price__20",
             "raw_zhutong_industry_rollbar_avg_close_price__20"]
# turnover: np.log(series/10000+1)
turnover_key = ["raw_zhutong_industry_rollbar_turnover__20"]

keys = ['raw_ygb_symbol_rollbar_close_price__20',
           'raw_ygb_symbol_rollbar_high_price__20',
           'raw_ygb_symbol_rollbar_low_price__20',
           #'raw_ygb_symbol_rollbar_turnover__20',
           'raw_ygb_symbol_rollbar_buy_turnover_log__20',
           'raw_ygb_symbol_rollbar_sell_turnover_log__20',
           'raw_ygb_symbol_rollbar_buy_count_log__20',
           'raw_ygb_symbol_rollbar_sell_count_log__20',
           'raw_ygb_symbol_rollbar_buy_turnover_mean_log__20',
           'raw_ygb_symbol_rollbar_sell_turnover_mean_log__20',
           'raw_ygb_symbol_rollbar_weight_price__20',
           'raw_ygb_symbol_rollbar_weight_buy_price__20',
           'raw_ygb_symbol_rollbar_weight_sell_price__20',
           'raw_ygb_symbol_rollbar_buy_time_gap_std__20',
           'raw_ygb_symbol_rollbar_sell_time_gap_std__20',
           'raw_ygb_symbol_rollbar_buy_time_gap_mean__20',
           'raw_ygb_symbol_rollbar_sell_time_gap_mean__20',
           "raw_zhutong_industry_rollbar_weighted_high_price__20",
             "raw_zhutong_industry_rollbar_weighted_low_price__20",
             "raw_zhutong_industry_rollbar_weighted_close_price__20",
             "raw_zhutong_industry_rollbar_avg_high_price__20",
             "raw_zhutong_industry_rollbar_avg_low_price__20",
             "raw_zhutong_industry_rollbar_avg_close_price__20",
             "raw_zhutong_industry_rollbar_turnover__20"]

def moving_average(df,window=100,min_periods=1): 
    return df.rolling(window=window,min_periods=min_periods).mean()

def get_preclose(date,stock):
    start_datetime=date+' 09:31:00'
    end_datetime = date+' 15:00:00'
    raw_data=stream.load(start_datetime = start_datetime,
                  end_datetime = end_datetime,
                  key_group_name = 'raw_wangbo_market__complete',
                  symbol_list = [stock],
                  region='cn', product='ashare', freq='tick',
                  load_root = '/mnt/sda/NAS/AllData/cn_ashare/tick/', verbose=False)
    return raw_data[stock]['raw_wangbo_npreclose'].max()

def convert_zero(x,preclose):
    if abs(x)<0.00001:
        return preclose
    else: return x

def get_MA(date,data_stock):
    data,stock=data_stock
    
#     print("Loading DAY: {} ing".format(date))
    # ================================ Section 1 Read Data
#     start_datetime=day+" 09:30:00"
#     end_datetime=day+" 15:00:00"
#     data = bar.load(start_datetime = start_datetime, 
#                end_datetime  = end_datetime, 
#                region='cn', product='ashare', freq='3second', 
#                symbol_list=SYMBOL,
#                load_root= "/mnt/sda/NAS/AllData/",
#                key_list = keys,
#                verbose=True, use_multiprocess=False).dropna(dim='SYMBOL',how='all')
    # ============================= Section 2 Read preclose
    
    
    print("Loading Stock: {} ing".format(stock))
    preclose=get_preclose(date,stock)

# ================================== Section 3 convert 0  and scale
    df = data.to_pandas()
    
    for key in keys:
        if key not in df.columns:
            return (stock,None)
    
    df[price_keys] = df[price_keys].applymap(lambda x:convert_zero(x,preclose))
    df[price_keys] = (df[price_keys]/preclose-1)*1000
    df[std_mean_keys] = df[std_mean_keys]/1000
    df[weight_price_keys] = df[weight_price_keys]-1
    df[industry_rollbar_keys]= (df[industry_rollbar_keys]-1)*1000
    df[turnover_key]=np.log(df[turnover_key]/10000+1)

    # ==================================== section 4 get MA
    data_MA300s = moving_average(df,window=100)
    data_MA300s.columns=[x+'MA300s' for x in keys]
    data_MA1800s = moving_average(df,window=600)
    data_MA1800s.columns=[x+'MA1800s' for x in keys]
    data_mean = moving_average(df,window=4800)
    data_mean.columns=[x+'mean' for x in keys]


    # ================================== Section 5 concat
    data_MA = pd.concat([data_MA300s,data_MA1800s,data_mean],axis=1).astype(np.float32)
    return stock,data_MA

def stream_save(dict_data):
    global date
    stream.save(entity=dict_data,
             key_group_name='freq_3s_MA',
             region='cn', product='ashare', freq='tick',
             save_root='/mnt/sda/NAS-203/ShareFolder2/zhizhou/',
             start_date=date, end_date=date, verbose=True)
    return   

if __name__ == '__main__':
    print('Start date: '+sys.argv[1])
    print('End date: '+sys.argv[2])

    start = sys.argv[1]
    end = sys.argv[2]

    SYMBOL = cn_symbol.load(region='cn',product='ashare')

    AllDates = trading_days.load( start_datetime = start, 
                        end_datetime  = end,
                        region='cn', product='ashare')
    
    pool=Pool(40)
#     list_results = pool.map(get_MA,AllDates)
    
#     for result in list_results:
#         p4=Process(target=stream_save,args=(result[0],result[1],))
#         p4.start()
    for date in AllDates:
        t1=time.time()
        start_datetime=date+" 09:31:30"
        end_datetime=date+" 15:00:00"
        data = bar.load(start_datetime = start_datetime, 
               end_datetime  = end_datetime, 
               region='cn', product='ashare', freq='3second', 
               symbol_list=SYMBOL,
               load_root= "/mnt/sda/NAS/AllData/",
               key_list = keys,
               verbose=True, use_multiprocess=False).dropna(dim='SYMBOL',how='all')
        stocks=list(data.SYMBOL.to_numpy())
        results_date=pool.starmap( get_MA,product([date],list(zip(data.loc[stocks],stocks))) )
        save_dict = {}
        for result in results_date:
            if result[1] is not None:
                save_dict[result[0]] = result[1]
        print('Start saving {} data!'.format(date))
        
        p4=Process(target=stream_save,args=(save_dict,))
        p4.start()
        
        t2= time.time()
        print("ALL stocks need {} seconds per day with pool=40".format(round(t2-t1,2)))
        




