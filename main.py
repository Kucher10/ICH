from algo import Algo
from coins import Coins
import datetime as dt
import time
from online import Online
import os

if __name__ == '__main__':
    # Coins.clear_table('RSI')
    # Algo.probability(dt.datetime(2019, 1, 1), dt.datetime(2021, 6, 1))
    # main_algo(dt.datetime(2019, 7, 1), dt.datetime(2021, 10, 1))
    # Algo.algo_check(dt.datetime(2020, 10, 1), dt.datetime(2020, 11, 1))
    # Coins.data_check('Binance15mProb', dt.datetime(2020, 1, 1), dt.datetime(2021, 5, 7))
    # Algo.main_algo(dt.datetime(2020, 4, 29), dt.datetime(2021, 5, 1))
    # Algo.algo_check_stop(dt.datetime(2021, 5, 1), dt.datetime(2021, 6, 1))
    # Coins.clear_table("Binance1mUSD")
    # Coins.binance_data_request('5m', 'Binance1mUSD', dt.datetime(2021, 7, 1), False)
    # Online.advisor_online()
    # Coins.clear_table('PsyPoint')
    # Algo.algo_psy_point(dt.datetime(2021, 1, 1), dt.datetime(2021, 6, 15))
    # Coins.clear_table('RSI')
    # Algo.EMA(dt.datetime(2021, 6, 1), dt.datetime(2021, 6, 2))

    message = True

    while True:

        now = dt.datetime.now

        if now().hour % 3 == 0 and message:
            Online.bot_chat_message('In the process')
            message = False

        if now().hour % 3 == 1 and not message:
            message = True

        if now().minute % 15 == 0 and now().second < 10:
            time.sleep(10 - now().second)

        print('Beginning of work ' + str(dt.datetime.now().replace(microsecond=0)))
        begin_time = time.time()

        try:
            Online.main_online()
        except Exception:
            Online.bot_chat_message("Онлайн " + str(dt.datetime.now()))

        now = dt.datetime.now
        if now().hour == 0 and now().minute < 5:

            try:
                Coins.initial_update(False)
                Online.bot_chat_message(
                    "Analytics data loaded " + str(dt.datetime.now().replace(microsecond=0)))
            except Exception:
                Online.bot_chat_message(
                    "Analytics data failed " + str(dt.datetime.now().replace(microsecond=0)))

            startDate = Coins.max_date_request('Binance15mProbUSD') - dt.timedelta(days=2)
            endDate = dt.datetime.now().replace(microsecond=0, second=0, minute=0) - dt.timedelta(minutes=15)

            try:
                Algo.probability(startDate, endDate)
                Online.bot_chat_message("Probability loaded " + str(dt.datetime.now().replace(microsecond=0)))
            except Exception:
                Online.bot_chat_message("Probability failed " + str(dt.datetime.now().replace(microsecond=0)))

            startDate = endDate - dt.timedelta(days=365)
            try:
                Algo.main_algo(startDate, endDate)
                Online.bot_chat_message("Percent loaded " + str(dt.datetime.now().replace(microsecond=0)))
            except Exception:
                Online.bot_chat_message("Percent failed " + str(dt.datetime.now().replace(microsecond=0)))

        end_time = time.time()
        work = end_time - begin_time

        end = time.time()
        os.system("cls")
        print('Algorithm running time: ' + str(round((end - begin_time), 2)))


