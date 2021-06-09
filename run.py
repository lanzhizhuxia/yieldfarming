import requests
import json
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import DECIMAL, TEXT, Date, DateTime
from pyquery import PyQuery as pq
import random



def save_poolList_mysql( poolList):

    def ff(x):
        timeArray = time.strptime(x["timestamp"], "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

    poolList['updateTime'] = poolList['updateTime'].apply(lambda x: x[:-1])

    def fff(y):
        rewards=[]
        for r in iter(y):
            rewards.append(r['symbol'])
        str = ','.join(rewards)
        return str

    poolList['rewards'] = poolList['rewards'].apply(lambda x: fff(x))

    poolList2 = poolList.drop(['id', 'monthlyROI', 'totalStakeRatio', 'weeklyROI'], axis=1)
    poolList2.rename(columns={'name':'Pool'})


    return poolList2



def main():

    headerlist = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36 OPR/43.0.2442.991",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36 OPR/42.0.2393.94",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36 OPR/47.0.2631.39",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"]
    user_agent = random.choice(headerlist)
    headers = {'User-Agent': user_agent}

    now = datetime.datetime.now()
    engine = create_engine('mysql+pymysql://root:123456@3.115.88.92:3306/mysql?charset=utf8')

    print('==========UPDATE poolList START===========')

    url = "https://coinmarketcap.com/yield-farming/"
    response = requests.get(url, headers=headers)
    response.encoding = 'utf8'
    doc = pq(response.text)

    merchantDocGenerator = doc('#__NEXT_DATA__').items()
    merchantDocList = list(merchantDocGenerator)

    for curIdx, merchantItem in enumerate(merchantDocList):
        merchantName = merchantItem.text()
        result = json.loads(merchantName)
        yieldFarmingList = result['props']['initialProps']['pageProps']['yieldFarmingList']

    df = pd.json_normalize(yieldFarmingList)
    df['creatDate'] = now

    result = pd.DataFrame(columns=['projectid', 'projectname', 'projectplatform', 'dailyROI', 'yearlyROI', 'name'
        , 'pair', 'link', 'rewards', 'totalStake', 'impermanentLoss', 'updateTime', 'creatDate'])
    # print (result.head())
    for row in df.itertuples():
        poolList = pd.DataFrame(getattr(row, 'poolList'))
        poolList['projectid'] = getattr(row, 'id')
        poolList['projectname'] = getattr(row, 'name')
        poolList['projectplatform'] = getattr(row, 'platform')
        poolList['creatDate'] = now

        poolList2 = save_poolList_mysql(poolList)
        result = result.append(poolList2)

    dtypedict = {'projectid': TEXT,'projectname': TEXT,'projectplatform': TEXT,'dailyROI': DECIMAL(18, 8),'yearlyROI': DECIMAL(18, 8),'name': TEXT, 'pair': TEXT, 'link': TEXT, 'rewards': TEXT,'totalStake': DECIMAL(18, 8),'impermanentLoss': TEXT, 'updateTime': DateTime,'creatDate': DateTime}
    result.to_sql(name='poolList', con=engine, chunksize=1000, if_exists='append', index=None,dtype=dtypedict)

    print('==========UPDATE poolList END===========')
    print('==========UPDATE marketCap START===========')
    url = "https://companiesmarketcap.com/assets-by-market-cap/"
    response = requests.get(url, headers=headers)
    response.encoding = 'utf8'
    doc = pq(response.text)

    # company_name
    company_name = doc('.company-name')
    company_name_list = []
    for i in company_name:
        # print(i.text)
        company_name_list.append({'company_name': i.text})
    dfcn = pd.DataFrame(company_name_list)

    # rank,MarketCap,Price,Today,country
    items = doc('tr')
    # print (type(items))
    # y = items.find('td')
    ll = []
    for i in items:

        rank = i[0].text
        # company-name
        if rank == 'Rank':
            continue

        categoryClass = i.get('class')
        if categoryClass == 'precious-metals-outliner':
            category = 'precious metals'
        elif categoryClass == 'crypto-outliner':
            category = 'cryptocurrencies'
        elif categoryClass == 'etf-outliner':
            category = 'ETFs'
        else:
            category = 'public companies'

        j = {
            'rank': rank,
            'MarketCap': i[2].get("data-sort"),
            'Price': i[3].get('data-sort'),
            'Today': i[4].values()[0],
            'country': i[6].getchildren()[0].text,
            'category': category,
        }
        # print(j)
        ll.append(j)

    df = pd.DataFrame(ll)
    df['company_name'] = dfcn['company_name']
    df['creatDate'] = now
    # print(df)
    # df.to_csv('marketCap.csv')
    dtypedict = {'rank': TEXT, 'company_name': TEXT, 'country': TEXT, 'category': TEXT, 'Price': DECIMAL(18, 8), 'Today': DECIMAL(18, 8),
                 'MarketCap': TEXT, 'creatDate': DateTime}
    df.to_sql(name='marketCap', con=engine, chunksize=1000, if_exists='append', index=None, dtype=dtypedict)
    #df.to_csv('ma.csv')

    print('==========UPDATE marketCap END===========')

    print('==========UPDATE validator START===========')
    url = "https://beaconscan.com/stat/validator"
    response = requests.get(url, headers=headers)
    response.encoding = 'utf8'
    doc = pq(response.text)

    plotData = doc("script[type='text/javascript']")
    plotDataStr = plotData.text()
    strlist = plotDataStr.split('eval(')[1].split(');')
    plotDataList = eval(strlist[0])
    plotDataDf = pd.DataFrame(plotDataList)

    plotDataDf.rename(columns={0: 'timestamp', 1: 'totalValidators', 2: 'c', 3: 'd'}, inplace=True)
    plotDataDf = plotDataDf.drop(['c', 'd'], axis=1)
    plotDataDf['creatDate'] = now

    dtypedict = {'timestamp': TEXT, 'totalValidators': TEXT,'creatDate': DateTime}
    plotDataDf.to_sql(name='validator', con=engine, chunksize=1000, if_exists='append', index=None, dtype=dtypedict)

    print('==========UPDATE validator END===========')
    print('==========UPDATE validatortotaldailyincome START===========')

    url = "https://beaconscan.com/stat/validatortotaldailyincome"
    response = requests.get(url, headers=headers)
    response.encoding = 'utf8'
    doc = pq(response.text)

    plotData = doc("script[type='text/javascript']")
    plotDataStr = plotData.text()
    strlist = plotDataStr.split('eval(')[1].split(');')
    plotDataList = eval(strlist[0])
    plotDataDf = pd.DataFrame(plotDataList)

    plotDataDf.rename(columns={0: 'timestamp', 1: 'totalDailyIncome', 2: 'avgDailyIncome'}, inplace=True)
    plotDataDf['creatDate'] = now

    dtypedict = {'timestamp': TEXT, 'totalDailyIncome': TEXT, 'avgDailyIncome': TEXT, 'creatDate': DateTime}
    plotDataDf.to_sql(name='income', con=engine, chunksize=1000, if_exists='append', index=None, dtype=dtypedict)
    print('==========UPDATE validatortotaldailyincome END===========')

if __name__ == '__main__':
    main()