import requests
import json
import sys
import pandas as pd
from datetime import timedelta, time
import datetime as dt
import sqlalchemy
import pyodbc
from six.moves import urllib
import time


class Coins:

    # ежедневное обновление данных
    @staticmethod
    def initial_update(active):

        begin_time = time.time()
        dateNow = dt.datetime.today()

        if active:
            Coins.clear_table("Binance15mActive")
            startDate = dateNow - timedelta(days=2)
            Coins.binance_data_request('15m', 'Binance15mActive', startDate, True)
        else:
            startDate = Coins.max_date_request('Binance15mUSD') + timedelta(minutes=15)
            Coins.binance_data_request('15m', 'Binance15mUSD', startDate, False)

        end_time = time.time()
        print('Время загрузки данных: ' + str(round((end_time - begin_time), 2)))

    # запрос списка монет из бинанса и запись в БД
    @staticmethod
    def coins_list_request():

        Coins.clear_table("Coins")

        url = "https://api.binance.com/api/v1/exchangeInfo"

        df = json.loads(requests.get(url).text)
        df = pd.DataFrame(df['symbols'])
        df = df.loc[df['quoteAsset'] == 'BTC']
        df = df.iloc[:, 0:1]
        df.columns = ['coin']

        delistedCoins = ('AEBTC', 'ARNBTC', 'BCCBTC', 'BCHABCBTC', 'BCHSVBTC', 'BCNBTC', 'BCPTBTC', 'BTCBBTC', 'BTTBTC',
                         'CHATBTC', 'CLOAKBTC', 'CMTBTC', 'COCOSBTC', 'DAIBTC', 'DENTBTC', 'DGDBTC', 'FUELBTC', 'HCBTC',
                         'HOTBTC', 'HSRBTC', 'ICNBTC', 'INSBTC', 'KEYBTC', 'LUNBTC', 'MBLBTC', 'MCOBTC', 'MFTBTC',
                         'MODBTC', 'NCASHBTC', 'NPXSBTC', 'PAXBTC', 'PHXBTC', 'POEBTC', 'RENBTCBTC', 'RPXBTC',
                         'SALTBTC', 'SUBBTC', 'SUSDBTC', 'TNBBTC', 'TNTBTC', 'TRIGBTC', 'TUSDBTC', 'VENBTC', 'VIBEBTC',
                         'WBTCBTC', 'WINBTC', 'WINGSBTC') #, 'XZCBTC', 'BOTBTC') снять

        for coin in delistedCoins:
            df = df.loc[df['coin'] != coin]

        tokenSwap = ('EDOBTC', 'ENGBTC', 'ERDBTC', 'GNTBTC', 'LENDBTC', 'STORMBTC', 'STRATBTC', 'XZCBTC', 'BOTBTC')
        db = Coins.connect_db()
        conn = db.cursor()

        for coin in tokenSwap:
            if coin == 'ENGBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'SCRTBTC' WHERE coin = '{}'".format(coin)

            if coin == 'EDOBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'PNTBTC' WHERE coin = '{}'".format(coin)

            if coin == 'ERDBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'EGLDBTC' WHERE coin = '{}'".format(coin)

            if coin == 'GNTBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'GLMBTC' WHERE coin = '{}'".format(coin)

            if coin == 'LENDBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'AAVEBTC' WHERE coin = '{}'".format(coin)

            if coin == 'STORMBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'STMXBTC' WHERE coin = '{}'".format(coin)

            if coin == 'STRATBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'STRAXBTC' WHERE coin = '{}'".format(coin)

            if coin == 'XZCBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'FIROBTC' WHERE coin = '{}'".format(coin)

            if coin == 'BOTBTC':
                ex = "UPDATE [dbo].[Binance15m] SET coin = 'AUCTIONBTC' WHERE coin = '{}'".format(coin)

            conn.execute(ex)
            conn.commit()
            # df = df.loc[df['coin'] != coin] снять

        db.close()
        Coins.data_recording(df, "Coins")

    # запрос списка монет из бинанса и запись в БД
    @staticmethod
    def coinsUSD_list_request():

        Coins.clear_table("CoinsUSD")

        url = "https://api.binance.com/api/v1/exchangeInfo"

        df = json.loads(requests.get(url).text)
        df = pd.DataFrame(df['symbols'])
        df = df.loc[df['quoteAsset'] == 'USDT']
        df = df.iloc[:, 0:1]
        df.columns = ['coin']

        Coins.data_recording(df, "CoinsUSD")

    # запрос данных у бинанса и запись в БД
    @staticmethod
    def binance_data_request(tf, table, startDate, active):

        if active:
            coins = Coins.get_coins_algo()
        else:
            #Coins.coins_list_request()
            coins = Coins.get_coin_list(active)

        url = "https://api.binance.com/api/v3/klines"

        fullDf = pd.DataFrame()

        # цикл по монетам
        for row in coins:

            dateStart = startDate

            row = row[0]

            dateEnd = dt.datetime.now()
            if not active:
                dateEnd = dateEnd.replace(second=0, microsecond=0, minute=0, hour=0) - timedelta(minutes=15)
                # dateEnd = dt.datetime(2021, 6, 19) - timedelta(minutes=15)

            # цикл по 1000 строк
            while dateStart < dateEnd:

                difference = dateEnd - dateStart
                difference = difference.total_seconds()

                if float(difference) < 900:
                    print("Торги загружены " + str(dateStart) + " " + row)
                    break

                symbol = row
                interval = tf
                startTime = str(int(dateStart.timestamp() * 1000))
                endTime = str(int(dateEnd.timestamp() * 1000))
                limit = '1000'
                req_params = {'symbol': row, 'interval': interval, 'limit': limit, 'startTime': startTime,
                              'endTime': endTime}
                try:
                    df = pd.DataFrame(json.loads(requests.get(url, params=req_params).text))
                except Exception:
                    print('The request timed out')
                    df = pd.DataFrame()

                try:
                    if df.empty is not True:
                        df = df.iloc[:, 0:8]
                        del df[6]
                        try:
                            3
                        except Exception:
                            continue

                        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                        df['coin'] = row


                        columnsTitles = ['datetime', 'coin', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                        df = df.reindex(columns=columnsTitles)

                        df["datetime"] = [dt.datetime.fromtimestamp(x / 1000.0) for x in df["datetime"]]
                        df["datetime"] = df["datetime"].astype("str")
                        df["open"] = df["open"].astype("float")
                        df["high"] = df["high"].astype("float")
                        df["low"] = df["low"].astype("float")
                        df["close"] = df["close"].astype("float")
                        df["volumefrom"] = df["volumefrom"].astype("float")
                        df["volumeto"] = df["volumeto"].astype("float")

                        try:
                            dateCheck = df.iloc[:, 0].max()
                            dateCheck = dt.datetime.strptime(dateCheck, "%Y-%m-%d %H:%M:%S")

                            if dateStart == dateCheck:
                                print("Торги завершились " + str(dateCheck) + " " + row)
                                break

                            dateStart = dateCheck + timedelta(minutes=15)
                        except Exception:
                            dateCheck = dateCheck.split('.')[:-1]
                            dateCheck = dt.datetime.strptime(dateCheck[0], "%Y-%m-%d %H:%M:%S")

                            if dateStart == dateCheck:
                                print("Торги завершились " + dateCheck + " " + row)
                                break

                            dateStart = dateCheck + timedelta(minutes=15)

                        fullDf = pd.concat([fullDf, df], axis=0)
                        sys.stdout.write("\r" + row + ' ' + str(dt.datetime.now().replace(microsecond=0)) + ' loaded')

                    else:
                        print(row)
                        break
                except Exception:
                    break
        print('\nRecording to DB')
        Coins.data_recording(fullDf, table)

    # запись данных в выбранную таблицу в БД
    @staticmethod
    def data_recording(df, table):
        params = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                                         "Server=****;"
                                         "Database=ICH;"
                                         "uid=****;"
                                         "pwd=****")

        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        engine.connect()

        df.to_sql(name=table, con=engine, index=False, if_exists='append')

    # удаление данных из выбранной таблицы в БД
    @staticmethod
    def clear_table(table):
        db = Coins.connect_db()
        conn = db.cursor()
        ex = "TRUNCATE TABLE dbo." + table
        conn.execute(ex)
        conn.commit()
        db.close()

    # получение списка монет
    @staticmethod
    def get_coin_list(active):
        db = Coins.connect_db()
        conn = db.cursor()
        if active:
            ex = "Select coin from dbo.Activepercent"
        else:
            ex = "Select distinct(coin) from dbo.Binance15mUSD"
        conn.execute(ex)
        df = pd.Series(conn.fetchall())
        db.close()
        return df

    # подключение к базе данных
    @staticmethod
    def connect_db():
        db = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=****;"
                            "DATABASE=ICH;"
                            "UID=****;"
                            "PWD=****")

        return db

    # создание таблиц в базе данных
    @staticmethod
    def create_tables(tables, dictionary):
        db = Coins.connect_db()
        conn = db.cursor()

        if type(tables) is str:
            ex = "CREATE TABLE " + tables + "("
            for i in dictionary:
                ex = ex + i + " " + dictionary[i] + ","
            ex = ex[:-1]
            ex = ex + ")"
            conn.execute(ex)
            conn.commit()
        elif type(tables) is tuple:
            for i in tables:
                ex = "CREATE TABLE " + i + "("
                for j in dictionary:
                    ex = ex + j + " " + dictionary[j] + ","
                ex = ex[:-1]
                ex = ex + ")"
                conn.execute(ex)
                conn.commit()
        else:
            print("Некорректный тип данных")
        db.close()

    # удаление столбцов в таблицах базы данных
    @staticmethod
    def delete_column(table, column):
        db = Coins.connect_db()
        conn = db.cursor()

        ex = "ALTER TABLE {} DROP COLUMN {}".format(table, column)
        conn.execute(ex)
        conn.commit()
        db.close()

    # удаление таблицы из базы данных
    @staticmethod
    def delete_tables():

        db = Coins.connect_db()
        conn = db.cursor()
        ex = "SELECT [table] FROM Timeframes"
        conn.execute(ex)
        timeframes = conn.fetchall()
        timeframes.append(('Coins',))
        timeframes.append(('Timeframes',))
        i = 0

        while i < len(timeframes):
            ex = "DROP TABLE {}".format(timeframes[i][0])
            conn.execute(ex)
            i += 1
        conn.commit()
        db.close()

    # создание таблицы с таймфрэймами
    @staticmethod
    def create_timeframes():
        tables = "Timeframes"
        dictTF = {"timeframe": "varchar(20) null", "[table]": "varchar(20) null"}
        timeframes = [["1m", "Binance1m"], ["5m", "Binance5m"], ["15m", "Binance15m"], ["30m", "Binance30m"],
                      ["1h", "Binance1h"], ["4h", "Binance4h"], ["6h", "Binance6h"], ["1d", "Binance1d"],
                      ["1w", "Binance1w"]]

        tables = (
        "Binance1m", "Binance5m", "Binance15m", "Binance30m", "Binance1h", "Binance4h", "Binance6h", "Binance1d",
        "Binance1w")
        dictionary = {"datetime": "smalldatetime null", "coin": "varchar(20) null", "[open]": "float null",
                      "high": "float null", "low": "float null",
                      "[close]": "float null"}
        Coins.create_tables(tables, dictionary)
        df = pd.DataFrame(timeframes, columns=['timeframe', "table"])
        Coins.data_recording(df, tables)

    # получение даты последней загрузки
    @staticmethod
    def max_date_request(table):
        db = Coins.connect_db()
        conn = db.cursor()
        ex = "SELECT MAX(datetime) FROM [ICH].[dbo].[{}]".format(table)
        conn.execute(ex)
        a = conn.fetchall()
        db.close()
        return a[0][0]

    # получение активных монет Algo
    @staticmethod
    def get_coins_algo():

        db = Coins.connect_db()
        conn = db.cursor()
        ex = "SELECT[coin] FROM[ICH].[dbo].[ActivePercent] WHERE entryCount > 5"
        conn.execute(ex)
        df = pd.Series(conn.fetchall())
        db.close()
        return df

    # получение списка отклонений Algo
    @staticmethod
    def get_data_algo():
        params = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                                         "Server=****;"
                                         "Database=ICH;"
                                         "uid=****;"
                                         "pwd=****")

        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        engine.connect()
        ex = "SELECT[coin], [percent] FROM[ICH].[dbo].[ActivePercent]"
        df = pd.read_sql_query(ex, engine)
        return df

    # получение списка точек входа
    @staticmethod
    def get_entry_points():
        params = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                                         "Server=****;"
                                         "Database=ICH;"
                                         "uid=****;"
                                         "pwd=****")

        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        engine.connect()
        ex = "SELECT * FROM[ICH].[dbo].[EntryList]"
        df = pd.read_sql_query(ex, engine)
        return df

    # получение данных для анализа
    @staticmethod
    def data_request(coin, table, startDate, endDate):
        params = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                                         "Server=****;"
                                         "Database=ICH;"
                                         "uid=****;"
                                         "pwd=****")

        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        engine.connect()
        ex = "SElECT * from {} WHERE [coin] = '{}' and [datetime] > '{}' and [datetime] < '{}' order by " \
             "datetime".format(table, coin, startDate, endDate)
        df = pd.read_sql_query(ex, engine)
        return df

    @staticmethod
    def data_check(table, startDate, endDate):

        coinList = Coins.get_coin_list(False)

        for coin in coinList:

            data = Coins.data_request(coin[0], table, startDate, endDate)
            data = data['datetime']
            # было 12 системных обновлений торги приостонавливались
            systemUpdate = ['2020-02-09 04:45:00', '2020-02-19 14:30:00', '2020-03-04 12:15:00', '2020-04-25 04:45:00',
                            '2020-06-28 04:45:00', '2020-11-30 08:45:00', '2020-12-21 17:00:00', '2020-12-25 04:45:00',
                            '2021-02-11 06:30:00', '2021-03-06 04:45:00', '2021-04-20 04:45:00', '2021-04-25 07:00:00']
            for i in range(len(data) - 1):
                if data[i] + timedelta(minutes=15) != data[i + 1]:

                    for startTime in systemUpdate:
                        if startTime == str(data[i]):
                            update = True
                    if not update:
                        print(coin[0] + " " + str(data[i]) + " " + str(data[i + 1]))

                update = False


