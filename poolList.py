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

    url = "https://coinmarketcap.com/yield-farming/"
    response = requests.get(url, headers=headers)
    response.encoding = 'utf8'
    doc = pq(response.text)

    now = datetime.datetime.now()
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

    #result.to_csv('poolList.csv')
    engine = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/mysql')
    dtypedict = {'projectid': TEXT,'projectname': TEXT,'projectplatform': TEXT,'dailyROI': DECIMAL(18, 8),'yearlyROI': DECIMAL(18, 8),'name': TEXT, 'pair': TEXT, 'link': TEXT, 'rewards': TEXT,'totalStake': DECIMAL(18, 8),'impermanentLoss': TEXT, 'updateTime': DateTime,'creatDate': DateTime}
    result.to_sql(name='poolList', con=engine, chunksize=1000, if_exists='append', index=None,dtype=dtypedict)

if __name__ == '__main__':
    main()