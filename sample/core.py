# -*- coding: utf-8 -*-
import csv
import datetime
import urllib
from urllib import request
import requests
import pandas as pd
import time
from pandas_datareader import data
import fix_yahoo_finance as yf
import numpy as np
from bs4 import BeautifulSoup
import re
from yahoo_finance import Share
import ystockquote

class Quote(object):
    DATE_FMT = '%Y-%m-%d'
    TIME_FMT = '%H:%M:%S'

    def __init__(self):
        self.symbol = ''
        self.date, self.time, self.open_, self.high, self.low, self.close, self.volume = ([] for _ in range(7))

    def append(self, dt, open_, high, low, close, volume):
        try:
            self.date.append(dt.date())
            self.time.append(dt.time())
            self.open_.append(float(open_))
            self.high.append(float(high))
            self.low.append(float(low))
            self.close.append(float(close))
            self.volume.append(int(volume))
        except (ValueError, AttributeError):
            self.date.append(datetime.datetime.strptime("10-Oct-10", "%d-%b-%y"))
            self.time.append(datetime.datetime.now().time())
            self.open_.append(100.0)
            self.high.append(100.0)
            self.low.append(100.0)
            self.close.append(100.0)
            self.volume.append(10000)

    def to_csv(self):
        lengths = [len(self.close), len(self.date), len(self.time), len(self.open_), len(self.high),
                   len(self.low), len(self.volume)]
        return ''.join(["{0},{1},{2},{3:.2f},{4:.2f},{5:.2f},{6:.2f},{7}\n".format(self.symbol,
                                                                                   self.date[bar].strftime('%Y-%m-%d'),
                                                                                   self.time[bar].strftime('%H:%M:%S'),
                                                                                   self.open_[bar], self.high[bar],
                                                                                   self.low[bar], self.close[bar],
                                                                                   self.volume[bar])
                        for bar in range(min(lengths))])

    def write_csv(self, filename):
        with open(filename, 'w') as f:
            f.write("symbol, date, time, open, high, low, close, volumn, \n")
            f.write(self.to_csv())

    def read_csv(self, filename):
        self.symbol = ''
        self.date, self.time, self.open_, self.high, self.low, self.close, self.volume = ([] for _ in range(7))
        for line in open(filename, 'r'):
            symbol, ds, ts, open_, high, low, close, volume = line.rstrip().split(',')
            self.symbol = symbol
            dt = datetime.datetime.strptime(ds + ' ' + ts, self.DATE_FMT + ' ' + self.TIME_FMT)
            self.append(dt, open_, high, low, close, volume)
        return True

    def __repr__(self):
        return self.to_csv()


class GoogleQuote(Quote):
    ''' Daily quotes from Google. Date format='yyyy-mm-dd' '''
    def __init__(self,symbol,start_date,end_date=datetime.date.today().isoformat(), file = open("failed.txt", "a+")):
        super(GoogleQuote,self).__init__()
        # file to write to if process fails
        self.file = file
        try:
            self.symbol = symbol.upper()
        except AttributeError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0, 100.0, 10000)
        start = datetime.date(int(start_date[0:4]),int(start_date[5:7]),int(start_date[8:10]))
        end = datetime.date(int(end_date[0:4]),int(end_date[5:7]),int(end_date[8:10]))
        url_string = "http://www.google.com/finance/historical?q={0}".format(self.symbol)
        url_string += "&startdate={0}&enddate={1}&output=csv".format(
                          start.strftime('%b %d, %Y'),end.strftime('%b %d, %Y'))
        try:
            print(url_string)
            csvFile = requests.get(url_string).content.decode("utf-8").split("\n")
            print(csvFile)
        except urllib.HTTPError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
        for bar in range(1, len(csvFile) - 1):
            try:
                ds,open_,high,low,close,volume = csvFile[bar].split(',')
            # if we get response, but the response isnt a csv, that means we get the webpage that checks whether
            # we are a robot, so stop execution for 10 minutes
            except ValueError:
                print("using yahoo")
                self = YahooQuote(symbol,start_date,end_date=datetime.date.today().isoformat())
                break
            try:
                open_,high,low,close = [float(x) for x in [open_,high,low,close]]
            except ValueError:
                self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
            dt = datetime.datetime.strptime(ds,'%d-%b-%y')
            self.append(dt,open_,high,low,close,volume)
    def errorHandle(self, dt,open_,high,low,close,volume):
        self.append(dt, open_, high, low, close, volume)
        self.file.write(self.symbol + "\n")

class NewGoogleQuote(Quote):
    ''' Daily quotes from Google. Date format='yyyy-mm-dd' '''
    def __init__(self,symbol,start_date,end_date=datetime.date.today().isoformat(), file = open("failed.txt", "a+")):
        super(NewGoogleQuote,self).__init__()
        print("using new google quotes")
        # file to write to if process fails
        self.file = file
        try:
            self.symbol = symbol.upper()
        except AttributeError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0, 100.0, 10000)
        start = datetime.date(int(start_date[0:4]),int(start_date[5:7]),int(start_date[8:10]))
        end = datetime.date(int(end_date[0:4]),int(end_date[5:7]),int(end_date[8:10]))

        data_url = "https://www.google.com/finance/historical?q={}".format(self.symbol)

        page = requests.get(data_url)
        soup = BeautifulSoup(page.content, "html.parser")
        downloadLink = soup.find("meta", {"name": "Description"})
        downloadLink = downloadLink["content"]
        try:
            left = downloadLink.index("(")
            right = downloadLink.index(")")
            ticker_real = downloadLink[left + 1: right]
        except ValueError:
            self = YahooQuote(symbol,start_date,end_date=datetime.date.today().isoformat())
            return

        url_string = "http://www.google.com/finance/historical?q={0}".format(ticker_real)
        url_string += "&startdate={0}&enddate={1}&output=csv".format(
                          start.strftime('%b %d, %Y'),end.strftime('%b %d, %Y'))

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        try:
            csvFile = requests.get(url_string, headers = headers).content.decode("utf-8").split("\n")
            print(url_string)
            print(csvFile)
        except urllib.error.URLError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
        for bar in range(1, len(csvFile) - 1):
            try:
                ds,open_,high,low,close,volume = csvFile[bar].split(',')
            # if we get response, but the response isnt a csv, that means we get the webpage that checks whether
            # we are a robot, so stop execution for 10 minutes
            except ValueError:
                print("using yahoo")
                self = YahooQuote(symbol,start_date,end_date=datetime.date.today().isoformat())
                break
            try:
                open_,high,low,close = [float(x) for x in [open_,high,low,close]]
            except ValueError:
                self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
            dt = datetime.datetime.strptime(ds,'%d-%b-%y')
            self.append(dt,open_,high,low,close,volume)
    def errorHandle(self, dt,open_,high,low,close,volume):
        self.append(dt, open_, high, low, close, volume)
        self.file.write(self.symbol + "\n")


class YahooQuote(Quote):
    ''' Daily quotes from Yahoo. Date format='yyyy-mm-dd' '''
    def __init__(self,symbol,start_date,end_date=datetime.date.today().isoformat(), file = open("failed.txt", "a+")):
        self.file = file
        super(YahooQuote,self).__init__()
        try:
            self.symbol = symbol.upper()
        except AttributeError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0, 100.0, 10000)
        startDate = datetime.date(int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]))
        endDate = datetime.date(int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]))
        try:
            yf.pdr_override()
            csvFile = data.get_data_yahoo(self.symbol, start_date, end_date)
        except urllib.HTTPError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
        try:
            ds,open_,high = csvFile["Open"].index.values, csvFile["Open"].values, csvFile["High"].values
            low = csvFile["Low"].values
            close, volume = csvFile["Close"].values, csvFile["Volume"].values
        # if we get response, but the response isnt a csv, that means we get the webpage that checks whether
        # we are a robot, so stop execution for 10 minutes
        except (ValueError, KeyError):
            print(csvFile)
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0, 100.0, 10000)
            return
        try:
            for dataArray in [open_, high,low,close]:
                for i in range (0, len(dataArray)):
                    dataArray[i] = float(dataArray[i])
        except (ValueError, UnboundLocalError):
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
        dt = []
        for i in range(0, len(ds)):
            # convert numpy.datetime64 to datetime.datetime
            years = int(ds[i].astype('datetime64[Y]').astype(int) + 1970)
            months = int(ds[i].astype('datetime64[M]').astype(int) % 12 + 1)
            days = int((ds[i] - ds[i].astype('datetime64[M]'))/86400000000000 + 1)
            newDate = datetime.datetime(years, months, days, 0, 0, 0)
            dt.append(newDate)
            #dt[i] = datetime.datetime.strptime(str(ds[i]),'%d-%b-%y')
        for i in range(0, len(dt)):
            #print(dt[i],open_[i],high[i],low[i],close[i],volume[i])
            self.append(dt[i],open_[i],high[i],low[i],close[i],volume[i])
    def errorHandle(self, dt,open_,high,low,close,volume):
        self.append(dt, open_, high, low, close, volume)
        self.file.write(self.symbol + "\n")


class NewYahooQuote(Quote):
    ''' Daily quotes from Google. Date format='yyyy-mm-dd' '''
    def __init__(self,symbol,start_date,end_date=datetime.date.today().isoformat(), file = open("failed.txt", "a+")):
        super(NewYahooQuote,self).__init__()
        # file to write to if process fails
        self.file = file
        try:
            self.symbol = symbol.upper()
        except AttributeError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0, 100.0, 10000)
        start = datetime.datetime(int(start_date[0:4]),int(start_date[5:7]),int(start_date[8:10]))
        end = datetime.datetime(int(end_date[0:4]),int(end_date[5:7]),int(end_date[8:10]))

        crumbles, cookies = self.get_crumble_and_cookie(self.symbol)

        url_string = "https://query1.finance.yahoo.com/v7/finance/download/{0}?".format(self.symbol)
        url_string += "period1={0}".format(int(start.timestamp()))
        url_string += "&period2={0}".format(int(end.timestamp()))
        url_string += "&interval=1d&events=history&crumb={0}".format(crumbles)
        #url_string += "&cookie:{0}".format(cookies)

        s = requests.Session()

        try:
            print(url_string)
            csvFile = s.get(url_string).content.decode("utf-8").split("\n")
            print(csvFile)
        except urllib.HTTPError:
            self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
        for bar in range(1, len(csvFile) - 1):
            try:
                ds,open_,high,low,close,volume = csvFile[bar].split(',')
            # if we get response, but the response isnt a csv, that means we get the webpage that checks whether
            # we are a robot, so stop execution for 10 minutes
            except ValueError:
                print("using yahoo")
                #self = YahooQuote(symbol,start_date,end_date=datetime.date.today().isoformat())
                break
            try:
                open_,high,low,close = [float(x) for x in [open_,high,low,close]]
            except ValueError:
                self.errorHandle(datetime.datetime(2017,1,1,0,0,0), 100.0, 100.0, 100.0,100.0, 10000)
            dt = datetime.datetime.strptime(ds,'%d-%b-%y')
            self.append(dt,open_,high,low,close,volume)
    def errorHandle(self, dt,open_,high,low,close,volume):
        self.append(dt, open_, high, low, close, volume)
        self.file.write(self.symbol + "\n")

    def get_crumble_and_cookie(self, symbol):
        crumble_link = 'https://finance.yahoo.com/quote/{0}/history?p={0}'
        cookie_regex = r'Set-Cookie: (.*?); '
        crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
        link = crumble_link.format(symbol)
        response = request.urlopen(link)
        match = re.search(cookie_regex, str(response.info()))
        cookie_str = match.group(1)
        text = response.read()
        text = str(text)
        match = re.search(crumble_regex, text)
        crumble_str = match.group(1)
        print(crumble_str, cookie_str)
        return crumble_str, cookie_str

if __name__ == "__main__":
    success_file = open("successfully_read.txt", "a+")
    failed_file = open("failed.txt", "a+")
    success_file_read = open("successfully_read.txt", "r")
    failed_file_read = open("failed.txt", "r")
    successful_stock = success_file_read.read()
    failed_stock = failed_file_read.read()
    successful_set = set(successful_stock.split("\n"))
    failed_set = set(failed_stock.split("\n"))
    data_source = pd.read_csv("ticker_all.csv")
    tickers = data_source["oftic"]
    sdates = data_source["sdates"]

    # sampleQuote = NewYahooQuote("AAPL", "2016-08-01")
    # sampleQuote.write_csv("randomTest.csv")
    #
    # sampleQuote.get_crumble_and_cookie(sampleQuote.symbol)


    # share = Share('AAPL')
    # print(share.get_historical('2014-04-25', '2014-04-29'))

    # print(ystockquote.get_historical_prices('GOOGL', '2013-01-03', '2013-01-08'))

    for i in range(0, len(tickers)):
        if tickers[i] != "" and sdates[i] != "" and (not successful_set.__contains__(tickers[i])) \
                and (not failed_set.__contains__(tickers[i])) :
            print(tickers[i])
            ticker = tickers[i]
            if "/" in str(ticker):
                ticker = ticker[0: ticker.index("/")]
            startDate = datetime.date.isoformat(datetime.datetime.strptime(sdates[i],'%d-%b-%y'))
            sampleQuote = NewGoogleQuote(ticker, startDate, file = failed_file)
            if len(sampleQuote.close) > 1:
                sampleQuote.write_csv("prices/{0}.csv".format(sampleQuote.symbol.replace("/", "")))
                success_file.write(str(tickers[i]) + "\n")
                print("success")
            elif not failed_set.__contains__(tickers[i]):
                failed_file.write(str(tickers[i]) + "\n")
                print("failed")


