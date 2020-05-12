from iexfinance.stocks import Stock
from iexfinance.stocks import get_historical_data
import sqlite3
from random import randint
from datetime import datetime
import numpy as np

class StockDownloader():
    """docstring for StockDownloader"""

    def __init__(self):
        super(StockDownloader, self).__init__()
        self.conn = sqlite3.connect('stocks.db')

    def getStockFrom(self, stockName):
        a = Stock(stockName, token="PUBLIC_KEY")
        quote = a.get_quote()
        a.get
        print(quote)
        ts = int(str(quote['closeTime'])[0:10])
        price = quote['close']
        return price, ts

    def getHistoricalStocsFrom(self, stockName):
        start = datetime(2020, 1, 1)
        end = datetime(2020, 5, 1)
        data = get_historical_data(stockName, start, end, token="PUBLIC_KEY", close_only=True)
        return data

    def writeCompanyDataToDb(self, stockName):
        stock = Stock(stockName, token="PUBLIC_KEY")
        try:
            company = stock.get_company()
            name = company['companyName']
            symbol = company['symbol']
            industry = company['industry']
            sector = company['sector']
            issueType = company['issueType']
            exchange = company['exchange']
            country = company['country']
            employees = company['employees']

            print(symbol, industry, sector, issueType, exchange, country, employees)
            self.conn.execute(
                '''CREATE TABLE IF NOT EXISTS Corporates (SYMBOL TEXT NOT NULL, INDUSTRY TEXT NOT NULL, SECTOR TEXT NOT NULL, ISSUETYPE TEXT, EXCHANGE TEXT, COUNTRY TEXT, EMPLOYEES TEXT);''')

            data = (symbol, industry, sector, issueType, exchange, country, employees)
            cur = self.conn.cursor()
            sql = '''INSERT INTO Corporates (SYMBOL, INDUSTRY, SECTOR, ISSUETYPE, EXCHANGE, COUNTRY, EMPLOYEES) VALUES (?,?,?,?,?,?,?);'''
            cur.execute(sql, data)

            self.conn.commit()
        except:
            pass

    def writeHistoricalStocksToDatabase(self, stockName, data):
        try:
            self.conn.execute(
                '''CREATE TABLE IF NOT EXISTS {0} (PRICE REAL NOT NULL, DATEOFPRICE REAL NOT NULL, VOLUME REAL NOT NULL);'''.format(stockName))
            print(len(data.keys()))
            for d in data.keys():


                    date = self.getDate(d)
                    closePrice = data[d]['close']
                    volume = data[d]['volume']
                    self.conn.execute(
                        '''INSERT INTO {0} (PRICE,DATEOFPRICE, VOLUME) VALUES ({1},{2},{3});'''.format(stockName,closePrice, date, volume))

            self.conn.commit()
        except:
            pass

    def getDate(self, datestr):
        format_str = '%Y-%m-%d'  # The format
        try:
            datetime_obj = datetime.strptime(datestr, format_str)
            return int(datetime_obj.timestamp())
        except:
            return 1

def main():
    stocks = ['BA','UTX','LMT','GD','NOC','RTN','DAL','NKE','JPM-C','BAC','EOD','C','SAN','HSBC','MUFG','ITUB','GS','SMFG','MS','BUD','KO','FMX','CRH','LYB','ORCL','SAP','DELL','HPQ','CAJ','HPE','CAT','DE','BAM','AXP','SNE','HON','INT','DIS','ADM','BG','TSN','KR','WMT','TGT','M','UNH','ANTM','CNC','HUM','CI','HCA','FMS','CVS','PG','UL','GE','ABB','JCI','IBM','CAN','PUK','MFC','BRKB','AIG','ALL','CB','PGR','BABA','UPS','FDX','ABT','MDT','MT','PKX','ACH','NUE','RIO','COP','VALE','MMM','TM','F','GM','HMC','TTM','MGA','NOK','SLB','SIN','RDS-A','BP','XOM','TOT','CVX','PSX','VLO','MPC','PBR','E','ABBV','ET','ENB','PAGP','TSM','HD','LOW','BBY','TJX','DG','T','VZ','CHL','CCZ','CHA','TEF','AMX','ORAN','BTI','PM','ARW','SYY','MCK','ABC','CAH']
    sd = StockDownloader()
    for stock in stocks:
        sd.writeCompanyDataToDb(stock)
        data = sd.getHistoricalStocsFrom(stock)
        sd.writeHistoricalStocksToDatabase(stock, data)



if __name__ == "__main__":
    main()
