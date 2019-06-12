# -*- coding: utf-8 -*-
"""
Created on Mon May 27 17:11:46 2019

@author: tjiang
"""

import pandas as pd
import os,datetime,re,calendar,sys
import requests
import zipfile


dict_inst={'豆一':'a',
           '豆二':'b',
           '胶合板':'bb',
           '玉米':'c',
           '玉米淀粉':'cs',
           '乙二醇':'eg',
           '纤维板':'fb',
           '铁矿石':'i',
           '焦炭':'j',
           '鸡蛋':'jd',
           '焦煤':'jm',
           '聚乙烯':'l',
           '豆粕':'m',
           '棕榈油':'p',
           '聚丙烯':'pp',
           '聚氯乙烯':'v',
           '豆油':'y'}

def dceDataRequest(date):
    
    
    ###日度的所有合约品种的成交量和持仓数据
    url_daily_1=r'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'
    r_daily_1 = requests.get(url_daily_1)
    
    try:
        if '小计' not in r_daily_1.content.decode(): #‘小计’字段不在文件中，即当日还未生成日行情文件，单个文件内部内容可用utf8解码
            return
    except Exception as e:  #由于没有测试非交易日，所以可能在非交易日response没有可decode的内容，可能抛出异常
        runningLog=open(cwd+os.path.sep+'runningLog_%s.txt'%date,'w')
        runningLog.write(date+'   '+str(e))
        runningLog.close()
        
        return
    
    txtPath_day=cwd+os.path.sep+'data'+os.path.sep+'txtfile'+os.path.sep+'daily'
    try:
        os.makedirs(txtPath_day)
    except EnvironmentError:
        pass
    with open (txtPath_day+os.path.sep+"%s_Daily.txt"%date,"wb") as code_day_1:
        code_day_1.write(r_daily_1.content)
        print('%s daily quote data get!'%date)
        
        
    ###每日的所有合约的排名数据
    url_daily=r'http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html'
    r_daily = requests.get(url_daily)
    try:
        if '结算文件还没有生成，请稍后再下载' in r_daily.content.decode():
            return
    except UnicodeDecodeError: #若出现解码错误，则说明已经有相应的数据文件，由于是有多个文件，文件名可能会导致utf8无法解码，这样直接捕捉后忽略即可
        pass
    zipPath=cwd+os.path.sep+'data'+os.path.sep+'zipfile'
    try:
        os.makedirs(zipPath)
    except EnvironmentError:
        pass
    with open (zipPath+os.path.sep+"%s_DCE_DPL.zip"%date,"wb") as code_day:
        code_day.write(r_daily.content)
        print('%s rank data get!'%date)

    ###月度的所有合约品种的成交量和持仓数据
    url_monthly=r'http://www.dce.com.cn/publicweb/quotesdata/exportMonthQuotesChData.html'
    r_monthly = requests.get(url_monthly)
    txtPath_month=cwd+os.path.sep+'data'+os.path.sep+'txtfile'+os.path.sep+'monthly'
    try:
        os.makedirs(txtPath_month)
    except EnvironmentError:
        pass
    with open (txtPath_month+os.path.sep+"%s_Monthly.txt"%date,"wb") as code_month:
        code_month.write(r_monthly.content)
        print('%s month quote data get!'%date)
    
    

def parseDataDaily(date):
    
    ###解析日度所有合约的期货公司排名数据
    zipFile=cwd+os.path.sep+'data'+os.path.sep+'zipFile'+os.path.sep+'%s_DCE_DPL.zip'%date
    txtFile=cwd+os.path.sep+'data'+os.path.sep+'txtFile'+os.path.sep+'daily'+os.path.sep+'%s_Daily.txt'%date
    F = zipfile.ZipFile(zipFile,'r')
    month=date[:6]
    txtPath=cwd+os.path.sep+'data'+os.path.sep+'txtfile'+os.path.sep+date
    try:
        os.makedirs(txtPath)
    except EnvironmentError:
        pass
    for file in F.namelist():
        F.extract(file,txtPath)
    
    l_allInst=[]
    for file in F.namelist():
        #print(file)
        filePath=txtPath+os.path.sep+file
        instrument=file.split('_')[1]
        
        l_df=[]
        with open(filePath,encoding='utf8') as rankFile:
            i=0
            flag='未知字段'
            for line in rankFile:
                if '成交量' in line:
                    flag='成交量'
                    rankData={'公司名称':[],
                              flag:[]
                              }
                elif '持买单量' in line:
                    flag='持买单量'
                    rankData={'公司名称':[],
                              flag:[]
                              }
                elif '持卖单量' in line:
                    flag='持卖单量'
                    rankData={'公司名称':[],
                              flag:[]
                              }
                
                if re.match(r'\d',line): #start with numbers
                    line=line.strip()
                    #print(line)
                    l_data=re.split('[\t ]+',line)
                    #print(l_data)
                    rankData['公司名称'].append(l_data[1])
                    rankData[flag].append(l_data[2].replace(',',''))
                elif '总计' in line:
                    i+=1
                    df=pd.DataFrame(rankData).set_index('公司名称')
                    l_df.append(df.T)
                    if i==3:
                        break
        df_allFlag=pd.concat(l_df,sort=False).T
        df_allFlag['Instrument']=[instrument]*len(df_allFlag)
        df_allFlag['Date']=[date]*len(df_allFlag)
        df_allFlag=df_allFlag.reset_index().rename(columns={'index':'公司名称'})
        l_allInst.append(df_allFlag)
    
    df=pd.concat(l_allInst,sort=False)
    resPath=cwd+os.path.sep+'result'+os.path.sep+month+os.path.sep+date
    try:
        os.makedirs(resPath)
    except EnvironmentError:
        pass
    df.to_csv(resPath+os.path.sep+'%s_DCE_rankData.csv'%date,index=False)
    
    ###解析日度所有合约品种的成交量和持仓数据
    statDict={'Instrument':[],
              '成交量':[],
              '持仓量':[]}
    with open(txtFile,encoding='utf8') as quoteFile:
        for line in quoteFile:
            if '小计' in line:
                line=line.strip()
                info=re.split('[\t ]+',line)
                instrument=dict_inst[info[0][:-2]]
                tradingVolume=info[1].replace(',','')
                position=info[2].replace(',','')
                statDict['Instrument'].append(instrument)
                statDict['成交量'].append(float(tradingVolume))
                statDict['持仓量'].append(float(position))
    
    df1=pd.DataFrame(statDict)
    df1['Date']=[date]*len(df1)
    df1.to_csv(resPath+os.path.sep+'%s_DCE_quoteData.csv'%date,index=False)
    
    
    
    return df,df1

def parseDataMonthly(month):
    
    path=cwd+os.path.sep+'data'+os.path.sep+'txtfile'+os.path.sep+'monthly'
    files=os.listdir(path)
    monthDays=[int(x.split('_')[0]) for x in files if x.split('_')[0][:6]==month]
    day=max(monthDays)

    monthFile=path+os.path.sep+"%s_monthly.txt"%str(day)
    
    statDict={'Instrument':[],
              '成交量':[],
              '持仓量':[]}
    
    with open(monthFile,encoding='utf8') as statFile:
        for line in statFile:
            if '小计' in line:
                line=line.strip()
                info=re.split('[ \t]+',line)
                instrument=dict_inst[info[0][:-2]]
                tradingVolume=info[1].replace(',','')
                position=info[2].replace(',','')
                statDict['Instrument'].append(instrument)
                statDict['成交量'].append(tradingVolume)
                statDict['持仓量'].append(position)
    
    df=pd.DataFrame(statDict)
    df.to_csv(cwd+os.path.sep+'result'+os.path.sep+month+os.path.sep+'%s_LastDayStatData.csv'%month)
    
    return df    

               
def stat(dates):
    
    #zipFiles=[cwd+os.path.sep+'data'+os.path.sep+'zipFile'+os.path.sep+'%s_DCE_DPL.zip'%x for x in dates]
    #txtFiles=[cwd+os.path.sep+'data'+os.path.sep+'txtFile'+os.path.sep+'daily'+os.path.sep+'%s_Daily.txt'%x for x in dates]
    #zipdir=os.listdir(zipPath)
    #txtdir=os.listdir(txtPath)
    
    statDict={'日期':[],
              '期货公司成交量':[],
              '市场成交量':[],
              '期货公司/市场':[]}
    for date in dates:
        #date=z.split(os.path.sep)[-1].split('_')[0]
        #date1=t.split('_')[0]
        print(date)
        res=parseDataDaily(date)
        comp=res[0]['成交量'].fillna(0).map(float).sum()
        mkt=res[1]['成交量'].fillna(0).map(float).sum()
        ratio=comp/mkt
        statDict['日期'].append(date)
        statDict['期货公司成交量'].append(comp)
        statDict['市场成交量'].append(mkt)
        statDict['期货公司/市场'].append(ratio)
        
    df=pd.DataFrame(statDict)
    df['日期']=pd.to_datetime(df['日期'])
    
    df.index=df['日期']
    
    return df
#dates=['20180122','20180222','20180329','20180423','20180529','20180614','20180725','20180803','20180903','20181016','20181127','20181210']
#df=stat(dates)

def getMonthStat(month):
    
    ###获取月度期货公司的成交量和持仓量
    #month='201903'
    days=pd.date_range(month+'01',periods=31,freq='d')
    dates=[x.strftime('%Y%m%d') for x in days]
    l_rank=[]
    l_quote=[]
    for date in dates:
        try:
            res=parseDataDaily(date)
            
        except EnvironmentError:
            continue
        else:
            print(date)
            df_rank=res[0]
            df_quote=res[1]
            #df_mkt=res[1]
            l_rank.append(df_rank)
            l_quote.append(df_quote)
            
    df_comp=pd.concat(l_rank).fillna(0)
    df_comp['InstrumentType']=df_comp['Instrument'].map(lambda x:x[:-4])
    #print(df_comp.head())
    df_comp[['成交量','持买单量','持卖单量']]=df_comp[['成交量','持买单量','持卖单量']].applymap(float)
    df=df_comp.groupby(by=['公司名称','InstrumentType']).sum()
    df=df.reset_index()
    df['持仓量']=df['持买单量']+df['持卖单量']
    
    ###获取月度市场成交量和持仓量
    df1=pd.concat(l_quote).fillna(0)[['Instrument','成交量','持仓量']].groupby(by=['Instrument']).sum()
    #df1.set_index('Instrument',inplace=True)
    #df1=df1[df1['成交量']>0]
    
    ###给每个期货公司的每个合约品种添加成交量占市场比例和持仓量占市场比例数据
    df['成交量比例']=df.apply(lambda x:x['成交量']/float(df1.loc[x['InstrumentType'],'成交量']),axis=1)
    df['持仓量比例']=df.apply(lambda x:x['持仓量']/float(df1.loc[x['InstrumentType'],'持仓量']),axis=1)
    
    #df.to_csv(cwd+os.path.sep+'result'+os.path.sep+month+os.path.sep+'%s_StatData.csv'%month)
    return df,df1



if __name__=='__main__':
    global cwd
    cwd=os.getcwd()
    
    try:
        month=sys.argv[1]
    except:
        
        date=datetime.datetime.now().strftime('%Y%m%d')
        lastDay=calendar.monthrange(int(date[:4]),int(date[4:6]))[1]
        zipfilepath=cwd+os.path.sep+'data'+os.path.sep+'zipfile'
        zipfiledir=os.listdir(zipfilepath)
        txtfilepath=cwd+os.path.sep+'data'+os.path.sep+'txtfile'+os.path.sep+'daily'
        txtfiledir=os.listdir(txtfilepath)
        if "%s_DCE_DPL.zip"%date in zipfiledir and "%s_Daily.txt"%date in txtfiledir: #由于月行情数据每天时刻都可以获取到，故只要前两个文件已经获取成功，则月行情数据也必定成功，故无需额外判断
            #print('done!')
            if int(date[-2:])==lastDay:
                month=date[:6]
                df=getMonthStat(month)
                parseDataMonthly(month)
                df.to_csv(cwd+os.path.sep+'result'+os.path.sep+month+os.path.sep+'%s_StatData.csv'%month)
        else:
            dceDataRequest(date)
    else:
        res=getMonthStat(month)
        parseDataMonthly(month)
        df_comp=res[0]
        df_mkt=res[1]
        df_comp.to_csv(cwd+os.path.sep+'result'+os.path.sep+month+os.path.sep+'%s_StatData.csv'%month)
        df_mkt.to_csv(cwd+os.path.sep+'result'+os.path.sep+month+os.path.sep+'mktData_%s.csv'%month)
        



    

        
   