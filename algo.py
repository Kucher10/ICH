import datetime as dt
from _ast import mod
import numpy as np
from coins import Coins
import pandas as pd
import pandas_ta as pta

class Algo:

    @staticmethod
    def main_algo_analytics(startDate, endDate):

        Coins.clear_table("ActivePercentAnalytics")

        coinList = Algo.get_active_coins('Binance15m')

        for rowCoin in coinList:
            entryCount = 0
            deviation = 0.05

            movementDf = Coins.data_request(rowCoin[0], "Binance15mProb", startDate, endDate)

            if movementDf.empty:
                continue

            movementNum = movementDf.to_numpy()
            period = max(movementNum[:, 0]) - min(movementNum[:, 0])

            if period < dt.timedelta(days=120):
                continue

            while deviation < 100:

                max24 = 0
                flag = False
                for i in range(len(movementNum)):
                    if movementNum[i][7] * (1 - deviation) >= movementNum[i][6] >= 0.0000005 \
                            and movementNum[i][8] <= 1.1:

                        if movementNum[i][9] == 1 and max24 != movementNum[i][7]:
                            entryCount += 1
                            max24 = movementNum[i][7]
                        elif movementNum[i][9] == 0:
                            flag = True
                            break

                if not flag:
                    fullDf = pd.DataFrame(columns=['coin', 'percent', 'entryCount'])
                    fullDf.loc[0] = [rowCoin[0], deviation, entryCount]
                    Coins.data_recording(fullDf, 'ActivePercentAnalytics')
                    print(rowCoin[0] + " Количество входов: " + str(entryCount) + ' Отклонение: ' + str(deviation))
                    break

                entryCount = 0
                deviation += 0.005

    @staticmethod
    def algo_psy_point(startDate, endDate):

        minWeightP = 1.01
        maxWeightP = 1.04

        minWeightL = 0.95
        maxWeightL = 0.99

        coinList = Algo.get_active_coins('Binance15mUSD')

        psy_points = [0.001, 0.0015, 0.002, 0.0025, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009,
                      0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
                      0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
                      1.0, 1.5, 2.0, 2.5, 3.0, 4 ,5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80 ,90, 100,
                      150, 200, 250, 300, 400, 500, 600, 700, 800, 900, 1000]


        while minWeightP < maxWeightP:

            while minWeightL < maxWeightL:

                for rowCoin in coinList:

                    fullDf = pd.DataFrame(columns=['coin', 'datetime','low', 'high', 'result', 'point', 'point_origin', 'weight', 'weight2'])

                    movementDf = Coins.data_request(rowCoin[0], "Binance15mUSD", startDate, endDate)

                    if movementDf.empty:
                        continue

                    movementNum = movementDf.to_numpy()

                    count = 0

                    for i in range(192, len(movementNum) - 96):

                        low = movementNum[i][4]
                        high = movementNum[i][3]

                        avg = (high + low) / 2
                        psy_points_v = psy_points

                        point = 0


                        for j in range(7):

                            psy_point_len = len(psy_points_v)

                            index = psy_point_len // 2

                            if low < psy_points_v[index] < high:
                                point = psy_points_v[index] * 1.001
                                point_origin = str(psy_points_v[index])
                                break
                            elif psy_points_v[index] > avg:
                                psy_points_v = psy_points_v[0:index]
                            else:
                                psy_points_v = psy_points_v[index:]

                        movementNum48 = movementNum[i - 192:i - 1]
                        min48 = min(movementNum48[:,4])

                        if point > 0:

                            if min48 < point * 1.011:

                                continue

                            else:

                                for z in range(i + 1, len(movementNum) - 96):

                                    if point * minWeightP < movementNum[z][3]:

                                        fullDf.loc[count] = [rowCoin[0], movementNum[i][0], movementNum[i][4],
                                                              movementNum[i][3],
                                                              'P', point, point_origin, minWeightP, minWeightL]
                                        count += 1
                                        break

                                    if point * minWeightL > movementNum[z][4]:
                                        fullDf.loc[count] = [rowCoin[0], movementNum[i][0], movementNum[i][4],
                                                              movementNum[i][3],
                                                              'L', point, point_origin, minWeightP, minWeightL]
                                        count += 1
                                        break

                    Coins.data_recording(fullDf, 'PsyPoint')

                minWeightL += 0.01
            minWeightL = 0.95
            minWeightP += 0.01

    @staticmethod
    def EMA(startDate,endDate):

        coinList = Algo.get_active_coins('Binance15mUSD')

        for rowCoin in coinList:

            fullDf = pd.DataFrame(
                columns=['coin', 'price', 'datetime', 'result', 'close', 'exitdatetime'])

            movementDf = Coins.data_request(rowCoin[0], "Binance15mUSD", startDate, endDate)

            movementNum = movementDf.to_numpy()

            a = pta.rsi(movementDf['close'], length=14)
            plt.plot(a)

            count = 0



            for i in range(13, len(a)):
                if a[i] < 30:
                    price = movementNum[i][5]
                    enter_date = movementNum[i][0]
                    # вход в сделку
                    for j in range(i, len(movementNum)):

                        if movementNum[j][3] > price * 1.02:
                            fullDf.loc[count] = [rowCoin[0], price, movementNum[i][0], 'P', movementNum[j][3],
                                                 movementNum[j][0]]
                            count += 1
                            break
                        if movementNum[j][3] > price * 0.95:
                            fullDf.loc[count] = [rowCoin[0], price, movementNum[i][0], 'L', movementNum[j][4],
                                                 movementNum[j][0]]
                            count += 1
                            break

            Coins.data_recording(fullDf, 'RSI')
            plt.show()
            break

    @staticmethod
    def main_algo(startDate, endDate):

        Coins.clear_table("ActivePercentAnalytics")

        coinList = Algo.get_active_coins('Binance15mUSD')

        for rowCoin in coinList:
            entryCount = 0
            # count = 0
            deviation = 0.05

            movementDf = Coins.data_request(rowCoin[0], "Binance15mProbUSD", startDate, endDate)

            if movementDf.empty:
                continue

            movementNum = movementDf.to_numpy()
            period = max(movementNum[:, 0]) - min(movementNum[:, 0])

            if period < dt.timedelta(days=480):
                continue

            while deviation < 100:

                c_max24 = 0
                flag = False
                for i in range(len(movementNum)):

                    avg = movementNum[i][6]
                    growthBefore = movementNum[i][8]
                    probability = movementNum[i][9]
                    max24 = movementNum[i][7]


                    if max24 * (1 - deviation) >= avg >= 0.0000005 \
                            and growthBefore <= 1.1:

                        if probability == 1 and c_max24 != max24:

                            if i > 24:

                                movementDfRSI = movementDf[i-24:i]
                                rsi = pta.rsi(movementDfRSI['close'], length=22)
                                rsi = rsi.to_numpy()
                                if rsi[-2] < 35 and rsi[-1] > rsi[-2]:

                                    entryCount += 1
                                    c_max24 = max24

                        elif probability == 0:
                            if i > 24:

                                movementDfRSI = movementDf[i - 24:i]
                                rsi = pta.rsi(movementDfRSI['close'], length=22)
                                rsi = rsi.to_numpy()
                                if rsi[-2] < 35 and rsi[-1] > rsi[-2]:
                                    flag = True
                                    break
                                    # count += 1

                if not flag:
                    fullDf = pd.DataFrame(columns=['coin', 'percent', 'entryCount'])
                    fullDf.loc[0] = [rowCoin[0], deviation, entryCount]
                    Coins.data_recording(fullDf, 'ActivePercentAnalytics')
                    print(rowCoin[0] + " Количество входов: " + str(entryCount) + ' Отклонение: ' + str(deviation))
                    break

                # if entryCount / count > 0.85:
                #     print(str(rowCoin[0]) + " - отклонение " + str(deviation) + "%, количество входов " + str(count) +
                #           ", процент прибыльных сделок " + str((entryCount / count) * 100) + "%")

                entryCount = 0
                # count = 0
                deviation += 0.005

    @staticmethod
    def algo_check(startDate, endDate):
        db = Coins.connect_db()
        conn = db.cursor()
        ex = "Select coin, [percent] from dbo.ActivePercentAnalytics"
        conn.execute(ex)

        df = pd.Series(conn.fetchall())
        activeCoin = df.to_numpy()

        entryCount = 0
        lossEntryCount = 0

        totalPercentProfit = 0
        totalPercentLoss = 0

        table = []

        for coin in activeCoin:

            # ex = "Select * from dbo.Binance15mProb where coin = '{}' and datetime >= '{}' and datetime <= '{}' " \
            #       " order by datetime".format(coin[0], startDate, endDate)
            # conn.execute(ex)
            # df = pd.Series(conn.fetchall())
            df = Coins.data_request(coin[0], 'dbo.Binance15mProbUSD', startDate, endDate)
            period = df.to_numpy()

            deviation = coin[1]
            max24 = 0

            percentLoss = 0
            percentProfit = 0
            date = dt.datetime.now()
            for i in range(len(period) - 96):
                if period[i][10] * (1 - deviation) >= period[i][4] >= 0.0000005:

                    entryPrice = period[i][10] * (1 - deviation)
                    if entryPrice > period[i][6]:
                        entryPrice = period[i][6]
                    actionDate = period[i][0].replace(second=0, microsecond=0, minute=0, hour=0)
                    dayAfterNum = period[i: i + 96]
                    max24after = max(dayAfterNum[:, 3])
                    # проверить на повторение max24 != period[i][7]
                    if entryPrice * 1.02 < max24after and period[i][8] <= 1.1:
                        if i > 23:
                            movementDfRSI = df[i - 24:i]
                            rsi = pta.rsi(movementDfRSI['close'], length=22)
                            rsi = rsi.to_numpy()
                            if rsi[-2] < 35 and rsi[-1] > rsi[-2]:
                                entryCount += 1
                                date = actionDate
                                max24 = period[i][10]
                                percentProfit += 1.8
                                totalPercentProfit += 1.8
                                table.append([coin[0], period[i][0], str(entryPrice + 1), str(period[i][10] + 1),
                                              str(max24after + 1), "Прибыль"])

                    elif entryPrice * 1.02 > max24after and period[i][8] <= 1.1:
                        if i > 23:
                            movementDfRSI = df[i - 24:i]
                            rsi = pta.rsi(movementDfRSI['close'], length=22)
                            rsi = rsi.to_numpy()
                            if rsi[-2] < 35 and rsi[-1] > rsi[-2]:
                                entryCount += 1
                                lossEntryCount += 1
                                date = actionDate
                                max24 = period[i][10]
                                exitPrice = period[i + 96][6]
                                loss = 1 - exitPrice / entryPrice
                                percentLoss += loss
                                totalPercentLoss += loss
                                table.append([coin[0], period[i][0], str(entryPrice + 1), str(period[i][10] + 1),
                                              str(max24after + 1), "Убыток"])

        table = np.array(table)
        table = table[table[:, 1].argsort()]
        for deal in range(len(table)):
            print(table[deal])
        print("Количество входов: " + str(entryCount) + " Процент прибыли: " + str(totalPercentProfit) +
              " Количество убыточных входов: " + str(lossEntryCount) + " Процент убытка: " +
              str(totalPercentLoss * 100))

    @staticmethod
    def algo_check_stop(startDate, endDate):
        db = Coins.connect_db()
        conn = db.cursor()
        ex = "Select coin, [percent] from dbo.ActivePercentAnalytics where entryCount > 20"
        conn.execute(ex)

        df = pd.Series(conn.fetchall())
        activeCoin = df.to_numpy()

        entryCount = 0
        lossEntryCount = 0

        totalPercentProfit = 0
        totalPercentLoss = 0

        table = []

        for coin in activeCoin:

            # ex = "Select * from dbo.Binance15mProb where coin = '{}' and datetime >= '{}' and datetime <= '{}' " \
            #       " order by datetime".format(coin[0], startDate, endDate)
            # conn.execute(ex)
            # df = pd.Series(conn.fetchall())
            df = Coins.data_request(coin[0], 'dbo.Binance15mProbUSD', startDate, endDate)
            period = df.to_numpy()

            deviation = coin[1]
            max24 = 0

            percentLoss = 0
            percentProfit = 0
            date = dt.datetime.now()
            for i in range(len(period) - 96):
                if period[i][10] * (1 - deviation) >= period[i][4] >= 0.0000005:

                    entryPrice = period[i][10] * (1 - deviation)
                    if entryPrice > period[i][6]:
                        entryPrice = period[i][6]
                    actionDate = period[i][0].replace(second=0, microsecond=0, minute=0, hour=0)
                    # dayAfterNum = period[i: i + 96]
                    # max24after = max(dayAfterNum[:, 3])
                    # проверить на повторение max24 != period[i][7]
                    if period[i][8] <= 1.1:

                        if i > 23:
                            movementDfRSI = df[i - 24:i]
                            rsi = pta.rsi(movementDfRSI['close'], length=22)
                            rsi = rsi.to_numpy()
                            if rsi[-2] < 35 and rsi[-1] > rsi[-2]:

                                profit = entryPrice * 1.02
                                stop = entryPrice * 0.8

                                for j in range(i + 1, len(period)):

                                    if stop >= period[j][4]:
                                        entryCount += 1
                                        lossEntryCount += 1
                                        date = actionDate
                                        max24 = period[i][10]
                                        exitPrice = period[i + 96][6]
                                        loss = 5
                                        percentLoss += loss
                                        totalPercentLoss += loss
                                        table.append([coin[0], period[i][0], str(entryPrice + 1), str(period[i][10] + 1),
                                                      str(stop + 1), "Убыток"])
                                        break

                                    if profit <= period[j][3]:
                                        entryCount += 1
                                        date = actionDate
                                        max24 = period[i][10]
                                        percentProfit += 1.8
                                        totalPercentProfit += 1.8
                                        table.append([coin[0], period[i][0], str(entryPrice + 1), str(period[i][10] + 1),
                                                      str(profit + 1), "Прибыль"])
                                        break






        table = np.array(table)
        table = table[table[:, 5].argsort()]
        print(table)
        print("Количество входов: " + str(entryCount) + " Процент прибыли: " + str(totalPercentProfit) +
              " Количество убыточных входов: " + str(lossEntryCount) + " Процент убытка: " +
              str(totalPercentLoss))

    @staticmethod
    def probability(startDate, endDate):

        coinList = Algo.get_active_coins('Binance15mUSD')

        percentProfit = 1.02

        for rowCoin in coinList:

            fullDf = pd.DataFrame(columns=['datetime', 'coin', 'open', 'high', 'low', 'close', 'avg', 'max24',
                                           'growthBefore', 'probability', 'max20'])

            movementDf = Coins.data_request(rowCoin[0], "Binance15mUSD", startDate, endDate)

            movementNum = movementDf.to_numpy()

            # начинается с 193 строки массива, чтобы не брать начало торгов с огромными свечками и получить макс за
            # предыдущие 24 часа и заканчивается за 96 строк чтобы получить макс за последующие 24 часа
            for i in range(192, len(movementNum) - 96):

                # условие для монет которые поменяли тикер
                if movementNum[i][0] - dt.timedelta(days=2) != movementNum[i - 192][0]:
                    continue

                movementNumMax24 = movementNum[i - 96: i]
                max24before = max(movementNumMax24[:, 3])
                # index = 96
                #
                # for j in range(len(movementNumMax24) - 1):
                #     if max24before < movementNumMax24[j + 1][3]:
                #         max24before = movementNumMax24[j + 1][3]
                #         index = len(movementNumMax24) - (j + 1)
                #
                # growthNum = movementNum[(i + 1 - 96 - index): (i + 1 - index)]
                # growthBefore = round(growthNum[-1][3] / min(growthNum[:, 4]), 3)

                movementNumMax24 = movementNum[i: i + 96]
                max24after = max(movementNumMax24[:, 3])

                movementNumMax20 = movementNum[i - 80: i]
                max20before = movementNumMax20[0][3]
                index2 = 80

                for j in range(len(movementNumMax20) - 1):
                    if max20before < movementNumMax20[j + 1][3]:
                        max20before = movementNumMax20[j + 1][3]
                        index2 = len(movementNumMax20) - (j + 1)

                growth20Num = movementNum[(i + 1 - 96 - index2): (i + 1 - index2)]
                growth20Before = round(growth20Num[-1][3] / min(growth20Num[:, 4]), 3)

                avgStr = (movementNum[i][2] + movementNum[i][2] + movementNum[i][2] + movementNum[i][2]) / 4

                if avgStr * percentProfit <= max24after:
                    probability = 1
                else:
                    probability = 0

                fullDf.loc[i - 96] = [movementNum[i][0], movementNum[i][1], movementNum[i][2], movementNum[i][3],
                                      movementNum[i][4], movementNum[i][5], avgStr, max24before, growth20Before,
                                      probability, max20before]

            Coins.data_recording(fullDf, 'Binance15mProbUSD')
            print(str(rowCoin) + ' Расчеты выполнены ' + str(dt.datetime.now()))

    # получение монет по которым идут торги
    @staticmethod
    def get_active_coins(table):
        db = Coins.connect_db()
        conn = db.cursor()
        ex = "Select distinct[coin] from dbo.{}".format(table)
        conn.execute(ex)
        df = pd.Series(conn.fetchall())
        db.close()
        return df

    # получение максимального значение за предыдущие 24 часа
    @staticmethod
    def max24_request(coin, dateStart, dateEnd):
        db = Coins.connect_db()
        conn = db.cursor()
        ex = "SElECT max(high) from dbo.Binance15m WHERE [coin] = '{}' and datetime >= '{}' and datetime <= '{}'" \
            .format(coin, dateStart, dateEnd)
        conn.execute(ex)
        a = conn.fetchall()
        db.close()
        return a[0][0]

