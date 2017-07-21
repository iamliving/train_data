# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 16:24:01 2017

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 15:46:13 2017
@author: Administrator
"""
'''
cd /home
python2.7 train_data_627_1.py
'''
import pandas as pd
# import numpy as np
# import numexpr
# from datetime import datetime 
# from dateutil.parser import parse
# sudo apt-get install python3-pip
# pip3 install pymysql
import pymysql # Linux Windows 都可以用, 可以导入，不能导出，导出还得要 mysqldb
from sqlalchemy import create_engine # Column, String, 
import pandas.io.sql as sql
# import MySQLdb
import gc # garbage collector

'''
def read_table(cur, sql_order): # sql_order is a string
    try:
        cur.execute(sql_order) # 多少条记录
    except Exception: #, e:
        frame = pd.DataFrame()
        # print e
        # continue
    else:
        data  = cur.fetchall(  )
        frame = pd.DataFrame(list(data))
    return frame
'''

def read_table(cur, sql_order): # sql_order is a string
    try:
        cur.execute(sql_order) # 多少条记录
        data  = cur.fetchall(  )
        frame = pd.DataFrame(list(data))
    except Exception: #, e:
        frame = pd.DataFrame()
        # print e
        # continue 
    return frame    

def type_jdg(dataframe): # p_base
    dataframe.loc[(dataframe.NAT_AN.str.len()<13) & ((dataframe.NAT_AN.str[4]=="1")|(dataframe.NAT_AN.str[4]=="8")),'type']=3
    dataframe.loc[(dataframe.NAT_AN.str.len()<13) & ((dataframe.NAT_AN.str[4]=="2")|(dataframe.NAT_AN.str[4]=="9")),'type']=2
    dataframe.loc[(dataframe.NAT_AN.str.len()<13) &  (dataframe.NAT_AN.str[4]=="3"),'type']=1
    dataframe.loc[(dataframe.NAT_AN.str.len()>13) & ((dataframe.NAT_AN.str[6]=='1')|(dataframe.NAT_AN.str[6]=='8')),'type']=3
    dataframe.loc[(dataframe.NAT_AN.str.len()>13) & ((dataframe.NAT_AN.str[6]=='2')|(dataframe.NAT_AN.str[6]=='9')),'type']=2
    dataframe.loc[(dataframe.NAT_AN.str.len()>13) &  (dataframe.NAT_AN.str[6]=='3'),'type']=1 

def method_jdg(dataframe): # p_claim
    dataframe['is_method']=0
    dataframe.loc[dataframe.stc_1.str[-6:]=="方法",'is_method']=1
    dataframe.loc[(dataframe.is_method==1)&(dataframe.type==3),'feature']='方法'
    dataframe.loc[(dataframe.is_method==1)&(dataframe.type==2),'feature']='构造'
    dataframe.loc[(dataframe.is_method==1)&(dataframe.type==1),'feature']='图案或颜色'
    dataframe.loc[(dataframe.is_method==0)&(dataframe.type.isin((2,3))),'feature']='产品' 
    dataframe.loc[(dataframe.is_method==0)&(dataframe.type==1),'feature']='图案或颜色' 

# 改错误IPC,得先有了IPC
def nat_crt(dataframe): # 效果暂时不方便查验，最后再说
    dataframe.loc[dataframe.NAT_AN=='CN02135177.5','ipc_3'] = 'A61K' 
    dataframe.loc[dataframe.NAT_AN=='CN200880022821.8','ipc_3'] = 'A61K' 
    dataframe.loc[dataframe.NAT_AN=='CN86100417.5','ipc_3'] = 'H01R' 
    dataframe.loc[dataframe.NAT_AN=='CN201510166720.X','ipc_3'] = 'B29C' 
    dataframe.loc[dataframe.NAT_AN=='CN201510227087.0','ipc_3'] = 'B29C' 
    dataframe.loc[dataframe.NAT_AN=='CN201510458170.9','ipc_3'] = 'B29C' 
    dataframe.loc[dataframe.NAT_AN=='CN201520330638.1','ipc_3'] = 'B29C' 
    dataframe.loc[dataframe.NAT_AN=='CN86105982.4','ipc_3'] = 'C22B' 
    dataframe.loc[dataframe.NAT_AN=='CN93224645.1','ipc_3'] = 'C14B' 
    dataframe.loc[dataframe.NAT_AN=='CN94190581.0','ipc_3'] = 'C07D' 
    dataframe.loc[dataframe.NAT_AN=='CN02234132.3','ipc_3'] = 'C25C' 
    dataframe.loc[dataframe.NAT_AN=='CN86104516.5','ipc_3'] = 'E04F' 
    dataframe.loc[dataframe.NAT_AN=='CN85101733.9','ipc_3'] = 'F02L' 
    dataframe.loc[dataframe.NAT_AN=='CN90209950.7','ipc_3'] = 'F16K' 
    dataframe.loc[dataframe.NAT_AN=='CN89107389.2','ipc_3'] = 'G01P' 
    dataframe.loc[dataframe.NAT_AN=='CN200880010185.7','ipc_3'] = 'H02P' 
    dataframe.loc[dataframe.NAT_AN=='CN200880025080.9','ipc_3'] = 'H04B' 
    dataframe.loc[dataframe.NAT_AN=='CN89205453.0','ipc_3'] = 'H04M' 
    dataframe.loc[dataframe.NAT_AN=='CN89207796.4','ipc_3'] = 'H01T' 
    dataframe.loc[dataframe.NAT_AN=='CN94190018.5','ipc_3'] = 'H04Q' 

# 专利被引用次数，cited_count
def count_uniq(series):
    return len(series.unique())
# 权利类型，avr_method，慢，结果为 int
# def sum_count(series):
            # return series.sum()/series.count()

def unique_natan(p_base):
    return p_base.sort_values(['NAT_AN','STD_PUB_DATE'], ascending=False).drop_duplicates('NAT_AN',keep='first')    
    
# def show_duplicates(p_base):            
    # print p_base[p_base.NAT_AN.isin(p_base.NAT_AN[p_base.duplicated('NAT_AN')==True])].sort_values(['NAT_AN','STD_PUB_DATE']) # 含原始的和重复的
                 
year = range(1989,2014)+[2015]
# year = range(2015,2016)

for i in year:        
    # 后面分行业各部分更少，运算更省内存
    # 注意：输入输出的行数是否不变，每过一步，查一下
    # 有些计数相关的指标这里会有误，因为一与 pbase merge数就不对了，如何？先处理了，然后去重
    # 有些需要unique之前处理(groupby(nat_an).sum)，有些必须unique之后    
    print i, 'p_base is reading'
    con = pymysql.connect(host='192.168.0.10', user='lwn', passwd='123456', db='NEW_PATENT', port = 3306) # 连接
    cur = con.cursor()
    sql_order = 'select PATDOC_ID,NAT_PN,NAT_AN,STD_PUB_DATE,STD_APP_DATE,PRIORITY_DATE,LATEST_PRS_AFFAIR,APPOR_NAME_EN,APPOR_NAME_ZH from SLAVE_PATENT_BASE_INFO_'+str(i) + " where left(NAT_AN,2)='CN'"
    p_base = read_table(cur, sql_order)
    con.commit()
    cur.close()
    con.close()
    p_base.columns = ['PATDOC_ID','NAT_PN','NAT_AN','STD_PUB_DATE','STD_APP_DATE','PRIORITY_DATE','LATEST_PRS_AFFAIR','APPOR_NAME_EN','APPOR_NAME_ZH']
    ## p_base 专利类型判断，type
    type_jdg(p_base)           
    p_base_23 = p_base # .loc[p_base.type.isin((2,3))] # 只选 发明 和 实用新型 419249
    # p_base_23 = p_base
    ## 释放内存
    del p_base
    gc.collect()
    # p_base_1 = p_base.loc[p_base.type==1]     
    # 是否创造性，natan_pn & is_warrant，unique之前，其实就是选出FM未授权的，先分年，按ipc合并后再sum一次
    natan_pn=p_base_23.groupby('NAT_AN')['NAT_PN'].apply(count_uniq).reset_index()# unique 之前进行此步，结果 419096
    natan_pn.rename(columns={'NAT_PN':'count_nat_pn'}, inplace=True)
    natan_pn['is_warrant']=1
    natan_pn.loc[(natan_pn.count_nat_pn==1) & (natan_pn.NAT_AN.str.len()<13) & ((natan_pn.NAT_AN.str[4]=='1')|(natan_pn.NAT_AN.str[4]=='8')),'is_warrant']=0
    natan_pn.loc[(natan_pn.count_nat_pn==1) & (natan_pn.NAT_AN.str.len()>13) & ((natan_pn.NAT_AN.str[6]=='1')|(natan_pn.NAT_AN.str[6]=='8')),'is_warrant']=0
    natan_pn.loc[(natan_pn.NAT_AN.str.len()<13) & (natan_pn.NAT_AN.str[4]=='3'),'is_warrant']=0
    natan_pn.loc[(natan_pn.NAT_AN.str.len()>13) & (natan_pn.NAT_AN.str[6]=='3'),'is_warrant']=0
    # unique & merge     
    p_base_23=unique_natan(p_base_23).merge(natan_pn[['NAT_AN','is_warrant']], on='NAT_AN', how='left') # 419096    
    ## 释放内存
    del natan_pn
    gc.collect()
    #
    # p_base 增加 ipc，没有的就空着，看最后能有多少
    print i, 'p_ipc is reading'
    con = pymysql.connect(host='192.168.0.10', user='lwn', passwd='123456', db='NEW_PATENT', port = 3306) # 连接
    cur = con.cursor()
    sql_order = 'select PATDOC_ID,IPC_MAIN,IPC,IPCR_MAIN,IPCR from SLAVE_PATENT_IPC_INFO_'+str(i)
    p_ipc = read_table(cur, sql_order)
    con.commit()
    cur.close()
    con.close()
    p_ipc.columns = ['PATDOC_ID','IPC_MAIN','IPC','IPCR_MAIN','IPCR']
    p_base_23_ipc = pd.merge(p_base_23,p_ipc,on = 'PATDOC_ID', how = 'left') # 419096
    ## 释放内存
    del p_ipc
    gc.collect()
    # IPC 空缺的用 IPCR 补齐
    p_base_23_ipc.loc[(p_base_23_ipc.IPC_MAIN=="")|(p_base_23_ipc.IPC_MAIN==None)|(p_base_23_ipc.IPC_MAIN=='None')|(p_base_23_ipc.IPC_MAIN==0),'IPC_MAIN']=p_base_23_ipc.IPCR_MAIN # ipc_main 空白的用 ipcr_main
    p_base_23_ipc.loc[(p_base_23_ipc.IPC     =="")|(p_base_23_ipc.IPC     ==None)|(p_base_23_ipc.IPC     =='None')|(p_base_23_ipc.IPC     ==0),'IPC'     ]=p_base_23_ipc.IPCR      # ipc      空白的用 ipcr
    # 
    p_base_23_ipc['ipc_3']=p_base_23_ipc.IPC_MAIN.str[0:4] 
    nat_crt(p_base_23_ipc)
    # std_app_year    
    p_base_23_ipc['std_app_year']=[(str(x)[0:4]) for x in p_base_23_ipc['STD_APP_DATE']]
    p_base_23_ipc.loc[p_base_23_ipc.std_app_year=='None','std_app_year']=0
    p_base_23_ipc['std_app_year']=[int(x) for x in p_base_23_ipc['std_app_year']]
    # [x.strftime('%Y') for x in p_base_23_ipc['STD_APP_DATE'] 
    # min_priority_year
    p_base_23_ipc['min_priority_year']=[min(x)[0:4] for x in p_base_23_ipc.PRIORITY_DATE.str.split(';')]
    p_base_23_ipc.loc[p_base_23_ipc.min_priority_year=='','min_priority_year']=0
    p_base_23_ipc['min_priority_year']=[int(x) for x in p_base_23_ipc.min_priority_year]
    ## 专利分类号个数，count_ipc
    p_base_23_ipc['count_ipc']=p_base_23_ipc['IPC'].str.count(';')+1    
    ## 剩余期限，left_year
    p_base_23_ipc['left_year']=0
    # p_base_23_ipc.loc[p_base_23_ipc.type==3,'left_year']=20-(2017-p_base_23_ipc.std_app_year)
    # p_base_23_ipc.loc[p_base_23_ipc.type==2,'left_year']=10-(2017-p_base_23_ipc.std_app_year)
    # p_base_23_ipc.loc[(p_base_23_ipc.min_priority_year!=0)&(p_base_23_ipc.std_app_year>p_base_23_ipc.min_priority_year)&(p_base_23_ipc.type==3), 'left_year']=20-(2017-p_base_23_ipc.min_priority_year)
    # p_base_23_ipc.loc[(p_base_23_ipc.min_priority_year!=0)&(p_base_23_ipc.std_app_year>p_base_23_ipc.min_priority_year)&(p_base_23_ipc.type==2), 'left_year']=10-(2017-p_base_23_ipc.min_priority_year)
    p_base_23_ipc.loc[p_base_23_ipc.type==3,'left_year']=20-(2017-p_base_23_ipc.min_priority_year)
    p_base_23_ipc.loc[p_base_23_ipc.type.isin((2,1)),'left_year']=10-(2017-p_base_23_ipc.min_priority_year)
    p_base_23_ipc.loc[(p_base_23_ipc.min_priority_year==0)&(p_base_23_ipc.type==3), 'left_year']=20-(2017-p_base_23_ipc.std_app_year)
    p_base_23_ipc.loc[(p_base_23_ipc.min_priority_year==0)&(p_base_23_ipc.type.isin((2,1))), 'left_year']=10-(2017-p_base_23_ipc.std_app_year)
    p_base_23_ipc.loc[ p_base_23_ipc.left_year<0, 'left_year']=0    
    #
    # p_claim，不能concat，也可能出现之前年份的 patdoc_id，比如公开2次
    print i, 'p_claim is reading'
    con = pymysql.connect(host='192.168.0.10', user='lwn', passwd='123456', db='NEW_PATENT', port = 3306) # 连接
    cur = con.cursor()
    sql_order = 'select PATDOC_ID,CLAIM_SEQ,CLAIM from SLAVE_PATENT_CLAIM_INFO_'+str(i)
    p_claim = read_table(cur, sql_order)
    con.commit()
    cur.close()
    con.close()
    if len(p_claim) > 0:
        p_claim.columns = ['PATDOC_ID','CLAIM_SEQ','CLAIM']        
        p_claim_23 = pd.merge(p_base_23_ipc[['PATDOC_ID','NAT_AN','type','ipc_3']], p_claim, on = 'PATDOC_ID', how = 'inner')
        ## 释放内存
        del p_claim
        gc.collect()
        # p_claim_23['start']=p_claim_23.CLAIM_SEQ.astype(str).str.len()
        p_claim_23['CLAIM']=p_claim_23.CLAIM.str.replace('、','.')    
        ## 去掉权利要求序号    
        # for j in range(1,max(p_claim_23.start)+1):
            # p_claim_23.loc[p_claim_23.start==j,'CLAIM']=p_claim_23.CLAIM.str[(j+1):]
        p_claim_23['stc_1']=p_claim_23.CLAIM.str.split('，').str[0]        
        ## is_main
        p_claim_23['is_main']=1
        p_claim_23.loc[(p_claim_23.stc_1.str.find('权利要求')!=-1),'is_main']=0
        method_jdg(p_claim_23) # method & feature        
        ## pre_len & rear_len
        p_claim_23['pre_len'] = 0
        p_claim_23['rear_len'] = 0
        p_claim_23.loc[p_claim_23.is_main==1,'pre_len'] =p_claim_23.stc_1.str.len() - p_claim_23.CLAIM_SEQ.astype(str).str.len() - 1
        p_claim_23.loc[p_claim_23.is_main==1,'rear_len']=p_claim_23.CLAIM.str.len() - p_claim_23.stc_1.str.len()       
        ## 权利要求完整性，is_integrity，外生        
        is_integrity=p_claim_23.groupby('NAT_AN')['feature'].apply(count_uniq).reset_index()             
        is_integrity['is_integrity']=0
        is_integrity.loc[is_integrity.feature>1,'is_integrity']=1
        p_base_23_ipc=p_base_23_ipc.merge(is_integrity[['NAT_AN','is_integrity']], on='NAT_AN', how='left')   
        ## 权利类型(专利主权利中方法类比例)，is_method，与 p_claim_23.is_method 含义不同        
        # p_claim_23.groupby('NAT_AN')['is_method'].apply(sum_count) # 慢，结果为int
        p_base_23_ipc=p_base_23_ipc.merge((1-p_claim_23.groupby('NAT_AN')['is_method'].sum()/p_claim_23.groupby('NAT_AN')['is_method'].count()).reset_index(), on='NAT_AN', how='left')  
        # 导出
        print i, 'p_claim is writing'
        engine = create_engine('mysql://root:123456@192.168.0.10/Model_718') 
        sql.to_sql(p_claim_23[['NAT_AN','CLAIM_SEQ','ipc_3','is_main','pre_len','rear_len']], 'p_claim_'+str(i), engine, index=False, if_exists='replace', chunksize=1000)   
        ## 释放
        del p_claim_23
        gc.collect()
    else:
        p_base_23_ipc['is_integrity']=0
        p_base_23_ipc['is_method']=0
    #
    # PRS，不能concat,可能跨年，要在某ipc中 PRS信息，在整个有效期内可能有,与 p_base 合并导出，每个ipc groupby(antan)
    print i, 'p_prs is reading'
    up_year=min(2014,i+20)
    p_prs_23=pd.DataFrame()
    for j in range(i,up_year+1):
        print j
        con = pymysql.connect(host='192.168.0.10', user='lwn', passwd='123456', db='NEW_PATENT', port = 3306) # 连接
        cur = con.cursor()
        sql_order = 'select NATNUM,PRS_INFO,PRS_PUB_DATE,PRS_STATUS,AFFAIR from SLAVE_PATENT_PRS_INFO_'+str(j)
        p_prs = read_table(cur, sql_order)
        con.commit()
        cur.close()
        con.close()
        if len(p_prs) > 0:
            p_prs.columns = ['NAT_AN','PRS_INFO','PRS_PUB_DATE','PRS_STATUS','AFFAIR']        
            p_prs_23_part = pd.merge(p_base_23_ipc[['PATDOC_ID','NAT_AN']], p_prs, on='NAT_AN', how = 'inner')
            ## 释放内存
            del p_prs
            gc.collect()
            p_prs_23=pd.concat([p_prs_23, p_prs_23_part]) 
            ## 释放内存
            del p_prs_23_part
            gc.collect()
    if i==2015:        
        p_base_23_ipc['lisence'] =0 # 公告之后才可能发生，不必在 unique 之前
        p_base_23_ipc['transfer'] =0 # 公告之后才可能发生，不必在 unique 之前       
        p_base_23_ipc.loc[p_base_23_ipc.LATEST_PRS_AFFAIR=='许可备案','lisence'] =1 # 公告之后才可能发生，不必在 unique 之前
        p_base_23_ipc.loc[p_base_23_ipc.LATEST_PRS_AFFAIR=='权利的转移','transfer'] =1 # 公告之后才可能发生，不必在 unique 之前       
        p_base_23_ipc['is_failed']=0
        p_base_23_ipc['is_litigation']=0
        p_base_23_ipc['is_review']=0
        p_base_23_ipc['maintain_year']=2017-p_base_23_ipc['std_app_year']
        p_base_23_ipc.loc[p_base_23_ipc.min_priority_year!=0,'maintain_year']=2017-p_base_23_ipc['min_priority_year']    
        p_base_23_ipc.loc[(p_base_23_ipc.type==3)&(p_base_23_ipc.maintain_year>20),'maintain_year']=20
        p_base_23_ipc.loc[(p_base_23_ipc.type.isin((2,1)))&(p_base_23_ipc.maintain_year>10),'maintain_year']=10
    else:
        ## lisence & transfer，外生
        p_prs_23['lisence'] = 0
        p_prs_23['transfer'] = 0
        p_prs_23.loc[p_prs_23.AFFAIR=='许可备案','lisence']=1 
        p_prs_23.loc[p_prs_23.AFFAIR=='权利的转移','transfer']=1    
        p_base_23_ipc=pd.merge(p_base_23_ipc,p_prs_23.groupby('NAT_AN')[['lisence','transfer']].sum().reset_index(), on = 'NAT_AN', how = 'left') 
        p_base_23_ipc.loc[p_base_23_ipc.lisence>1,'lisence'] =1
        p_base_23_ipc.loc[p_base_23_ipc.transfer>1,'transfer']=1
        p_base_23_ipc.loc[p_base_23_ipc.LATEST_PRS_AFFAIR=='许可备案','lisence'] =1 # 公告之后才可能发生，不必在 unique 之前
        p_base_23_ipc.loc[p_base_23_ipc.LATEST_PRS_AFFAIR=='权利的转移','transfer'] =1 # 公告之后才可能发生，不必在 unique 之前        
        # 是否失效，is_failed
        p_prs_23['is_failed'] = 0
        p_prs_23.loc[p_prs_23.PRS_STATUS.str.find('失效')!=-1,'is_failed'] = 1 # 失效
        p_base_23_ipc=pd.merge(p_base_23_ipc,p_prs_23.groupby('NAT_AN')['is_failed'].sum().reset_index(), on = 'NAT_AN', how = 'left') 
        p_base_23_ipc.loc[p_base_23_ipc.is_failed>1, 'is_failed']=1
        ## 是否经历诉讼，is_litigation，
        p_prs_23['is_litigation']=0
        p_prs_23.loc[(p_prs_23.PRS_INFO.str.find('诉讼')!=-1)&(p_prs_23.is_failed!=1), 'is_litigation']=1
        # p_prs_23.loc[(p_prs_23.PRS_INFO.str.find('诉讼')!=-1), 'is_litigation']=1                     
        p_base_23_ipc=p_base_23_ipc.merge(p_prs_23.groupby('NAT_AN')['is_litigation'].sum().reset_index(), on='NAT_AN', how='left')  
        p_base_23_ipc.loc[p_base_23_ipc.is_litigation>1, 'is_litigation']=1
        ## 是否经历复审，is_review
        p_prs_23['is_review']=0
        p_prs_23.loc[(p_prs_23.PRS_INFO.str.find('复审')!=-1)&(p_prs_23.is_failed!=1), 'is_review']=1        
        p_base_23_ipc=pd.merge(p_base_23_ipc,p_prs_23.groupby('NAT_AN')['is_review'].sum().reset_index(), on = 'NAT_AN', how = 'left') 
        p_base_23_ipc.loc[p_base_23_ipc.is_review>1, 'is_review']=1
        # prs_pub_year
        p_prs_23['prs_pub_year']=[int(str(x)[0:4]) for x in p_prs_23['PRS_PUB_DATE']]
        p_base_23_ipc=pd.merge(p_base_23_ipc,p_prs_23.loc[p_prs_23.is_failed==1,['NAT_AN','prs_pub_year']].groupby('NAT_AN').max().reset_index(), on = 'NAT_AN', how = 'left') 
        ## maitain_year
        p_base_23_ipc['maintain_year']=2017-p_base_23_ipc['std_app_year']
        p_base_23_ipc.loc[p_base_23_ipc.min_priority_year!=0,'maintain_year']=2017-p_base_23_ipc['min_priority_year']    
        p_base_23_ipc.loc[ p_base_23_ipc.is_failed==1,'maintain_year']=p_base_23_ipc['prs_pub_year']-p_base_23_ipc['std_app_year']
        p_base_23_ipc.loc[(p_base_23_ipc.is_failed==1)&(p_base_23_ipc.min_priority_year!=0),'maintain_year']=p_base_23_ipc['prs_pub_year']-p_base_23_ipc['min_priority_year']
        p_base_23_ipc.loc[(p_base_23_ipc.type==3)&(p_base_23_ipc.maintain_year>20),'maintain_year']=20
        p_base_23_ipc.loc[(p_base_23_ipc.type.isin((2,1)))&(p_base_23_ipc.maintain_year>10),'maintain_year']=10        
        ## prs 不必导出，释放内存
        del p_prs_23
        gc.collect()
    #            
    # ABS，不能concat，引用可能跨行业跨年（引用是之前的专利，被之后的专利引用，全部合并，参与循环，但不导出，合并             
    print i, 'p_abs is reading'
    con = pymysql.connect(host='192.168.0.10', user='lwn', passwd='123456', db='NEW_PATENT', port = 3306) # 连接
    cur = con.cursor()
    sql_order = 'select DOC_NAT_AN, CITED_NAT_AN from SLAVE_PATENT_ABS_PATCIT_'+str(i)
    p_abs = read_table(cur, sql_order)
    con.commit()
    cur.close()
    con.close()    
    if len(p_abs) > 0:
        p_abs.columns = ['NAT_AN','CITED_NAT_AN']
        p_abs=pd.merge(p_base_23_ipc[['PATDOC_ID','NAT_AN']], p_abs, on='NAT_AN', how='inner')
        # 专利引用内容, count_cite
        count_cite=p_abs.groupby('NAT_AN')['CITED_NAT_AN'].apply(count_uniq).reset_index()
        count_cite.columns=['NAT_AN','cite_count']
        p_base_23_ipc = pd.merge(p_base_23_ipc, count_cite, on='NAT_AN', how='left')
        ## 释放内存
        del count_cite
        gc.collect()
    else:
        p_base_23_ipc['cite_count']=0
    # 从当年起全部 abs
    up_year_1=min(2015,i+20)
    # p_abs_23=pd.DataFrame()
    for j in range(i+1,up_year_1+1):
        print j
        con = pymysql.connect(host='192.168.0.10', user='lwn', passwd='123456', db='NEW_PATENT', port = 3306) # 连接
        cur = con.cursor()
        sql_order = 'select CITED_NAT_AN from SLAVE_PATENT_ABS_PATCIT_'+str(j)
        p_abs_part = read_table(cur, sql_order)
        con.commit()
        cur.close()
        con.close()
        if len(p_abs_part) > 0:
            p_abs_part.columns = ['CITED_NAT_AN']        
            p_abs_23_part = pd.merge(p_base_23_ipc[['PATDOC_ID','NAT_AN']], p_abs_part, left_on='NAT_AN', right_on='CITED_NAT_AN', how = 'inner')
            del p_abs_part
            gc.collect()
            p_abs=pd.concat([p_abs, p_abs_23_part])
            del p_abs_23_part
            gc.collect()
    # 专利被引用次数，cited_count，所有的都受人为因素影响，抵消
    cited_count=p_abs.groupby('CITED_NAT_AN')['NAT_AN'].apply(count_uniq).reset_index()
    cited_count.columns=['NAT_AN','cited_count']
    p_base_23_ipc = pd.merge(p_base_23_ipc, cited_count, on='NAT_AN', how='left')
    del cited_count
    gc.collect()
    ## 释放内存
    del p_abs
    gc.collect()      
    # 缺失值处理，最后，产生 ipc_3=0
    p_base_23_ipc.loc[(p_base_23_ipc.APPOR_NAME_ZH=="")|(p_base_23_ipc.APPOR_NAME_ZH ==None),'APPOR_NAME_ZH']=p_base_23_ipc.APPOR_NAME_EN
    p_base_23_ipc=p_base_23_ipc.fillna(0)
    p_base_23_ipc.loc[p_base_23_ipc.ipc_3==0,'ipc_3']='' 
    # 导出
    engine = create_engine('mysql://root:123456@192.168.0.10/Model_718') 
    sql.to_sql(p_base_23_ipc[['NAT_AN','STD_PUB_DATE','APPOR_NAME_ZH','type','is_warrant','ipc_3','std_app_year','count_ipc','is_integrity','is_method','lisence','transfer','is_failed','is_litigation','is_review','left_year',
    'maintain_year','cite_count','cited_count']], 'p_base_'+str(i), engine, index=False, if_exists='replace', chunksize=1000)
    ## 释放内存
    del p_base_23_ipc
    gc.collect()