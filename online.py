import json
import urllib
import datetime as dt
import pandas as pd
import sqlalchemy
from numpy import where
import numpy as np
from algo import Algo
from coins import Coins
import requests
import pandas_ta as pta
from scipy.signal import argrelextrema


class Online:

    @staticmethod
    def main_online():

        try:
            now = dt.datetime.now
            if now().minute % 15 == 0 and now().second > 10:
                Coins.initial_update(True)
        except Exception:
            print("initial update")

        try:
            coinList = Online.get_deviation()
            coinList = coinList.to_numpy()
            dateNow = dt.datetime.today().replace(microsecond=0, second=0)
            startDate = dateNow - dt.timedelta(days=2)
            coinsPercent = Coins.get_data_algo()
            coinsPercent = coinsPercent.to_numpy()
        except Exception:
            print("get deviation")

        url = 'https://api.binance.com/api/v3/ticker/price'
        dfCurrentPrice = pd.DataFrame(json.loads(requests.get(url).text))
        dfCurrentPrice = dfCurrentPrice.to_numpy()

        # цикл по монетам
        for i in range(len(coinList)):

            try:
                indexCP = where(dfCurrentPrice == coinList[i][0])
                indexCP = indexCP[0][0]
                currentPrice = float(dfCurrentPrice[indexCP, 1])

                movementDf = Coins.data_request(coinList[i][0], "Binance15mActive", startDate, dateNow)
                movementNum = movementDf.to_numpy()
                movementNum20 = movementNum[-81:-1]
                movementNum20Sort = movementNum20[movementNum20[:, 3].argsort()]
                maxDate = movementNum20Sort[-1, 0]
                max20 = movementNum20Sort[-1, 3]
                percent = where(coinsPercent == coinList[i][0])
                percent = percent[0][0]
                percent = coinsPercent[percent][1]
                entryPrice = max20 * (1 - percent)

                entryCoinsDF = Coins.get_entry_points()
                entryCoins = entryCoinsDF.to_numpy()

                for j in range(len(entryCoins)):
                    if coinList[i][0] == entryCoins[j][0]:
                        period = dateNow - entryCoins[j][3]
                        if currentPrice > entryCoins[j][2] or period > dt.timedelta(days=1):
                            db = Coins.connect_db()
                            conn = db.cursor()
                            ex = "DELETE FROM [ICH].[dbo].[EntryList] WHERE coin = '{}' and exitPrice = '{}'".format(
                                coinList[i][0], entryCoins[j][2])
                            conn.execute(ex)
                            conn.commit()
                            db.close()
                            if currentPrice > entryCoins[j][2]:
                                Online.bot_chat_message("Congratulations - " + str(coinList[i][0]))

                if max20 * (1 - percent) >= currentPrice >= 0.0000005:

                    movementDfRSI = movementNum[-24:-1]
                    rsi = pta.rsi(movementDfRSI['close'], length=22)
                    rsi = rsi.to_numpy()

                    if rsi[-2] < 35 and rsi[-1] > rsi[-2]:

                        index = where(movementNum == maxDate)
                        index = index[0][0]
                        index2 = index - 96
                        movementNum24 = movementNum[index2:index]
                        growth = round(max20 / min(movementNum24[:, 4]), 3)
                        a = max20 + 1

                        if growth <= 1.1:

                            repeat = False

                            for j in range(len(entryCoins)):
                                if coinList[i][0] == entryCoins[j][0]:
                                    if entryCoins[j][1] == entryPrice:
                                        repeat = True

                            if not repeat:
                                eventDf = pd.DataFrame(columns=['coin', 'entryPrice', 'exitPrice', 'datetime'])
                                eventDf.loc[0] = [coinList[i][0], entryPrice, entryPrice * 1.02, movementNum[-1][0]]

                                Coins.data_recording(eventDf, 'EntryList')

                                Online.bot_chat_events(coinList[i][0], movementNum[-1][0], entryPrice, entryPrice * 1.02, a,
                                                       growth, percent)
            except Exception:
                print("failed " + coinList[i][0])

    @staticmethod
    def advisor_online():

        coinList = Algo.get_active_coins('Binance15mUSD')
        coinList = coinList.to_numpy()
        dateNow = dt.datetime.today().replace(microsecond=0, second=0)
        startDate = dateNow - dt.timedelta(days=2)

        url = 'https://api.binance.com/api/v3/ticker/price'
        dfCurrentPrice = pd.DataFrame(json.loads(requests.get(url).text))
        dfCurrentPrice = dfCurrentPrice.to_numpy()

        # цикл по монетам
        for i in range(len(coinList)):

            try:

                indexCP = where(dfCurrentPrice == coinList[i][0])
                indexCP = indexCP[0][0]
                currentPrice = float(dfCurrentPrice[indexCP, 1])

                movementDf = Coins.data_request(coinList[i][0], "Binance1mUSD", startDate, dateNow)
                movementNum = movementDf.to_numpy()
                x = movementNum[:, 3]
                localMax = argrelextrema(x, np.greater)
                r = movementNum[localMax]

            except Exception:
                print("failed " + coinList[i][0])

    @staticmethod
    def get_deviation():
        params = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                                         "Server=****;"
                                         "Database=ICH;"
                                         "uid=****;"
                                         "pwd=****")

        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        engine.connect()
        ex = "SElECT coin, [percent] from dbo.ActivePercent where entryCount > 5"
        df = pd.read_sql_query(ex, engine)
        return df

    @staticmethod
    def bot_chat_events(coin, dateEntry, entry, gP, max20, growth, percent):
        api_token = '1554164988:AAGBrbw88vc2a4aB6NR1J93IxP0dpMZXduc'

        requests.get('https://api.telegram.org/bot{}/sendMessage'.format(api_token), params=dict(
            chat_id='977247818',
            text='Монета: ' + str(coin) + '\nДата: ' + str(dateEntry) + '\nЦена: ' + str('%.9f' % entry) +
                 '\nПрибыль: ' + str('%.9f' % gP) + '\nМакс20: ' + str(max20) + ' \nРост: ' + str(growth) +
                 '\nОтклонение: ' + str(percent)
        ))

        requests.get('https://api.telegram.org/bot{}/sendMessage'.format(api_token), params=dict(
            chat_id='312137983',
            text='Монета: ' + str(coin) + '\nДата: ' + str(dateEntry) + '\nЦена: ' + str('%.9f' % entry) +
                 '\nПрибыль: ' + str('%.9f' % gP) + '\nМакс20: ' + str(max20) + ' \nРост: ' + str(growth) +
                 '\nОтклонение: ' + str(percent)
        ))

    @staticmethod
    def bot_chat_message(text):
        api_token = '1554164988:AAGBrbw88vc2a4aB6NR1J93IxP0dpMZXduc'

        requests.get('https://api.telegram.org/bot{}/sendMessage'.format(api_token), params=dict(
            chat_id='977247818',
            text=text
        ))

        requests.get('https://api.telegram.org/bot{}/sendMessage'.format(api_token), params=dict(
            chat_id='312137983',
            text=text
        ))
