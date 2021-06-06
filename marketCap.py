import requests
import json
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import DECIMAL, TEXT, Date, DateTime
from pyquery import PyQuery as pq
import random




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

    url = "https://companiesmarketcap.com/assets-by-market-cap/"
    response = requests.get(url, headers=headers)
    response.encoding = 'utf8'
    doc = pq(response.text)

    now = datetime.datetime.now()

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

        j = {
            'rank': rank,
            'MarketCap': i[2].get("data-sort"),
            'Price': i[3].get('data-sort'),
            'Today': i[4].values()[0],
            'country': i[6].getchildren()[0].text,
        }
        #print(j)
        ll.append(j)

    df=pd.DataFrame(ll)
    df['company_name']=dfcn['company_name']
    df['creatDate'] = now
    #print(df)
    #df.to_csv('marketCap.csv')
    engine = create_engine('mysql+pymysql://root:123456@3.115.88.92:3306/mysql?charset=utf8')
    dtypedict = {'rank': TEXT,'company_name': TEXT,'country': TEXT,'Price': DECIMAL(18, 8),'Today': DECIMAL(18, 8),'MarketCap': TEXT,'creatDate': DateTime}
    df.to_sql(name='marketCap', con=engine, chunksize=1000, if_exists='append', index=None,dtype=dtypedict)


if __name__ == '__main__':
    main()