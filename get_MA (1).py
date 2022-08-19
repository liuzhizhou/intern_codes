'''
该脚本的输入参数为一个日期时间段如 2022-07-04 2022-08-04，输出为一个.h5文件，保存在了路径："/mnt/sda/NAS-203/ShareFolder2/zhizhou/feature_ashare_semi_3_MA/cn_ashare/tick/MA/" 下，文件中是我们关心的因子经过MA（滑窗）处理后的数据。对于某天某股票的源数据，其“行”为时间序列（大多为3秒一次，不过不全是），“列”为因子。我们只对关心的因子做MA操作。在MA操作之前，该脚本还对不同类型的因子做了对应的处理，使得其值落在合理的区间内，方便后续的训练。以下是MA操作的流程：首先将源数据的 DataFrame Reindex 为以一秒为间隔，然后使用Rolling函数得到MA300s，MA1800s的数据；使用Expanding().mean()函数得到累积均值。最后将MA处理后的数据再 Reindex 为原先的时间序列。

注意：1. 保存时考虑到数据大小的问题，将数据一致转为了float32类型。
2. 对于不满足条件的股票数据（i）tick数量过少（ii）不存在关心的因子，在处理是会剔除
3. 如果 price 的价格为0，则用preclose的价格填充
'''
# 获取全市场所有股票代码
import shennong.utils.symbol as cn_symbol
import pandas as pd
import numpy as np
from shennong.stk import stream,bar
import time
from multiprocessing import Pool, Process
from itertools import product
import sys
# 获取交易日期
from shennong.utils import trading_days


def normalize_date(df,day):
    start1 = day+" 09:30:00"
    end1 = day+" 11:29:59"
    start2 = day+" 13:00:01"
    end2 = day+" 14:57:00"
    reindex = pd.date_range(start = start1, end = end1, freq="s").append(pd.date_range(start = start2,end = end2, freq="s"))
    return [df.reindex(reindex),df.index]


concern_factor = [
        'feature_ashare_interval_stat_buy_turnover__3',
        'feature_ashare_interval_stat_sell_turnover__3',
        'feature_ashare_interval_stat_buy_cancel_turnover__3',
        'feature_ashare_interval_stat_sell_cancel_turnover__3',
        'feature_ashare_interval_stat_buy_order_turnover__3',
        'feature_ashare_interval_stat_sell_order_turnover__3',
    
        'feature_ashare_interval_stat_buy_trade_count__3',
        'feature_ashare_interval_stat_sell_trade_count__3',
        'feature_ashare_interval_stat_buy_cancel_trade_count__3',
        'feature_ashare_interval_stat_sell_cancel_trade_count__3',
        'feature_ashare_interval_stat_buy_order_trade_count__3',
        'feature_ashare_interval_stat_sell_order_trade_count__3',
    
        'feature_ashare_interval_stat_buy_avg_price__3',
        'feature_ashare_interval_stat_sell_avg_price__3',
        'feature_ashare_interval_stat_cancel_buy_avg_price__3',
        'feature_ashare_interval_stat_cancel_sell_avg_price__3',
        'feature_ashare_interval_stat_order_buy_avg_price__3',
        'feature_ashare_interval_stat_order_sell_avg_price__3',
    
#         'feature_ashare_interval_stat_buy_maximum_order_turnover_ratio__3',
#         'feature_ashare_interval_stat_sell_maximum_order_turnover_ratio__3',
        'feature_ashare_interval_stat_buy_maximum_single_order_dealt_ratio__3',
        'feature_ashare_interval_stat_sell_maximum_single_order_dealt_ratio__3',
        'feature_ashare_interval_stat_buy_maximum_single_order_quote_ratio__3',
        'feature_ashare_interval_stat_sell_maximum_single_order_quote_ratio__3',
        ]

price_factor = ['feature_ashare_interval_stat_buy_avg_price__3',
        'feature_ashare_interval_stat_sell_avg_price__3',
        'feature_ashare_interval_stat_cancel_buy_avg_price__3',
        'feature_ashare_interval_stat_cancel_sell_avg_price__3',
        'feature_ashare_interval_stat_order_buy_avg_price__3',
        'feature_ashare_interval_stat_order_sell_avg_price__3']
 
def moving_average(df,window=100,min_periods=1): 
    return df.rolling(window=window,min_periods=min_periods).mean().rename(columns=lambda x:x+"_MA"+str(window))
 
def get_preclose(date,stock):
    start_datetime=date+' 09:30:00'
    end_datetime = date+' 15:00:00'
    raw_data=stream.load(start_datetime = start_datetime,
                  end_datetime = end_datetime,
                  key_group_name = 'raw_wangbo_market__complete',
                  symbol_list = [stock],
                  region='cn', product='ashare', freq='tick',
                  load_root = '/mnt/sda/NAS/AllData/cn_ashare/tick/', verbose=True)
    return raw_data[stock]['raw_wangbo_npreclose'].max()
    
    
def scale_data(series,preclose):
    if 'turnover' in series.name:
        if 'ratio' not in series.name:
            #series=series.rename(index=series.name+"_log_div10000",level=0)
            return np.log(series/10000+1)
        else: return series
    elif 'count' in series.name:
        #series.rename(series.name+"_log",level=0)
        return np.log(series+1)
    elif 'price' in series.name:
        #series.rename(series.name+"_ratio",level=0)
        return series/preclose-1
    else: return series
    
def stream_save(dict_data):
    global date
    stream.save(entity=dict_data,
             key_group_name='MA',
             region='cn', product='ashare', freq='tick',
             save_root='/mnt/sda/NAS-203/ShareFolder2/zhizhou/feature_ashare_semi_3_MA/',
             start_date=date, end_date=date, verbose=True)
    return   

def convert_zero(x,preclose):
    if abs(x)<0.00001:
        return preclose
    else: return x

def get_MA(day,stock):
    #day,stock=parm[0],parm[1]
    print("Loading {}ing".format(stock))
    start_datetime=day+" 09:30:00"
    end_datetime=day+" 15:00:00"
    data_day=stream.load(start_datetime = start_datetime,
                    end_datetime = end_datetime,
                    key_group_name = 'feature_ashare_semi_3',
                    symbol_list = [stock],
                    region='cn', product='ashare', freq='tick',
                    load_root = '/mnt/sda/NAS-203/ShareFolder2/zhutong/', verbose=True)
    
    if stock in data_day.keys():
        preclose = get_preclose(day,stock)
        df_raw = data_day[stock]
        
        if len(df_raw)<100:
            return (stock,None)
        for factor in concern_factor:  
            if factor not in df_raw.columns:
                return (stock,None)
            
        df_raw[price_factor] = df_raw[price_factor].applymap(lambda x:convert_zero(x,preclose))
            
        if df_raw.index.has_duplicates:
            df_drop_duplicate = df_raw[~df_raw.index.duplicated()]
            df,prev_index= normalize_date(df_drop_duplicate,day)
        else: df,prev_index = normalize_date(df_raw,day)
            
        df_MA300s = moving_average(df[concern_factor],window=300)
        df_MA1800s=moving_average(df[concern_factor],window=1800)
        df_mean = df[concern_factor].expanding(1).mean().rename(columns=lambda x:x+"_CUM_MEAN")
        df_MA = pd.concat([df_MA300s,df_MA1800s,df_mean],axis=1)
        df_MA_scaled = df_MA.apply(scale_data,axis=0,args=(preclose,)).astype(np.float32)
        return (stock,df_MA_scaled.reindex(prev_index).dropna(axis=0,how='all'))        
    else:
        return (stock,None)


if __name__ == '__main__':
    print('Start date: '+sys.argv[1])
    print('Start date: '+sys.argv[2])

    start = sys.argv[1]
    end = sys.argv[2]

    SYMBOL = cn_symbol.load(region='cn',product='ashare')

    AllDates = trading_days.load( start_datetime = start, 
                        end_datetime  = end,
                        region='cn', product='ashare')
    
    pool=Pool(40)
    for date in AllDates: 
        t1=time.time()
        results_date=pool.starmap(get_MA,product([date],SYMBOL))
        save_dict = {}
        for result in results_date:
            if result[1] is not None:
                save_dict[result[0]] = result[1]
        print('Start saving {} data!'.format(date))
        
        p4=Process(target=stream_save,args=(save_dict,))
        p4.start()
        
        t2= time.time()
        print("ALL stocks need {} seconds per day with pool=40".format(round(t2-t1,2)))



