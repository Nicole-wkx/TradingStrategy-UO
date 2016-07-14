#!/usr/bin/python
from evt_framework import *
from std_feeds import *
from valgo.evt_generator.utilities.trade_status_evt_generator import TradeStatusEvtGenerator

from datetime import datetime, time, timedelta
import numpy as np
from talib import ULTOSC

# -------------- UO Parameters ------------------
lowlevel = 30
highlevel = 70
midlevel = 50
avg1 = 5
avg2 = 2*avg1
avg3 = 2*avg2
# -------------------- End ----------------------

# -------------- Market Parameters ------------------
# trading hours: two periods, 9:15 - 12:00, 13:00 - 16:00
tradingHours = [ [time(9,15), time(12)], [time(13), time(16)] ]
day_start_time = tradingHours[0][0]
day_end_time = tradingHours[-1][1]
# -------------------- End ----------------------

# -------------- Order Parameters ----------------
ordersize = 1


class SampleEvtGenerator(EvtGenerator):
        def __init__(self, evt_mgr):
                print "SampleEvtGenerator __init__:"
                EvtGenerator.__init__(self, evt_mgr)

        def on_marketdatafeed(self, md):
                # from md.timestamp to datetime structure as current_time
                current_time = datetime(int(md.timestamp[:4]), int(md.timestamp[4:6]),  int(md.timestamp[6:8]), int(md.timestamp[9:11]),  int(md.timestamp[11:13]),  int(md.timestamp[13:15]))

                # is current_time in trading hours?
                flag = 0
                for period in tradingHours:
                    start_time = period[0]
                    end_time = period[1]
                    # current_time is in the period, set flag to 1
                    if current_time.time() >= start_time and current_time.time() <= end_time:
                        flag = 1
                        break
                if flag == 0:
                    return

                if md.productCode not in self._ohlcv:
                    if current_time.time() != end_time:
                        # combine date with start_time
                        this_interval = datetime.combine(current_time.date(), start_time)
                        next_interval = this_interval + timedelta(minutes = self.OHLCV_length)
                        # find time interval of current_time
                        # if current_time < earlist time interval, break
                        # else create an entry in self._ohlcv for the productCode
                        while True:
                            if current_time >= this_interval and current_time < next_interval:
                                self._ohlcv[md.productCode] = [
                                    this_interval,
                                    md.lastPrice,
                                    md.lastPrice,
                                    md.lastPrice,
                                    md.lastPrice
                                ]
                                break
                            elif current_time >= next_interval:
                                this_interval = next_interval
                                next_interval = this_interval + timedelta(minutes = self.OHLCV_length)
                            else:
                                break
                else:
                    # get timestamp of current ohlc
                    this_interval = self._ohlcv[md.productCode][0]
                    # if in the same time interval, update ohlcv
                    if (current_time - this_interval).seconds < 60 * self.OHLCV_length:
                        self._ohlcv[md.productCode] = [
                            self._ohlcv[md.productCode][0], \
                            self._ohlcv[md.productCode][1], \
                            max(self._ohlcv[md.productCode][2], md.lastPrice), \
                            min(self._ohlcv[md.productCode][3], md.lastPrice), \
                            md.lastPrice
                        ]
                    # otherwise, reserve current ohlcv to _high, _low, _close dictionaries and update ohlcv to the currently growing one
                    else:
                        print str(md.productCode) + ',' + self._ohlcv[md.productCode][0].strftime('%Y%m%d_%H%M%S')+ ',' + ','.join(map(str, self._ohlcv[md.productCode][1:]))

                        if md.productCode not in self._high:
                            self._high[md.productCode], self._low[md.productCode], self._close[md.productCode] = [],[],[]
                        else:
                            self._high[md.productCode].append(self._ohlcv[md.productCode][2])
                            self._low[md.productCode].append(self._ohlcv[md.productCode][3])
                            self._close[md.productCode].append(self._ohlcv[md.productCode][4])
                        while len(self._high[md.productCode]) > avg3+1:
                            del self._high[md.productCode][0]
                            del self._low[md.productCode][0]
                            del self._close[md.productCode][0]
                        if len(self._high[md.productCode]) == avg3+1:
                            self.calculateUO(md)
                        if current_time.time() == day_end_time:
                            del self._ohlcv[md.productCode]
                            del self._high[md.productCode]
                            del self._low[md.productCode]
                            del self._close[md.productCode]

                            # -------------------- calculate pnl at the end of the day --------------------
                            self.calculatePNL(md.productCode, current_time.date(), float(md.lastPrice))
                            # ts = self._ts.get_current_trade_status()
                            # print ts.get_all_product_positions()
                            # print ','.join(map(str, ['pnl'] + ts.get_product_codes() + [current_time.strftime('%Y%m%d'), ts['total_pnl']]))
                            # -----------------------------------------------------------------------------
                        else:
                            if current_time.time() == end_time:
                                del self._ohlcv[md.productCode]
                            # find the new this interval
                            else:
                                while True:
                                    this_interval += timedelta(minutes = self.OHLCV_length)
                                    if (current_time - this_interval).seconds < 60 * self.OHLCV_length:
                                        # reset the ohlcv of productCode
                                        self._ohlcv[md.productCode] = [
                                            this_interval,
                                            md.lastPrice,
                                            md.lastPrice,
                                            md.lastPrice,
                                            md.lastPrice
                                        ]
                                        break


                #print "SampleEvtGenerator..on_marketdatafeed called:" + str(md.timestamp) + " "  + str(md.lastPrice) + " " + str(md.lastVolume)
                '''
                self._latest_md_price[md.productCode] = md.lastPrice
                self._latest_md_vol[md.productCode] = md.lastVolume



                if self._latest_md_vol[md.productCode] > 10:
                        self.m_evt_mgr.insertEvt(Evt(1, "final_signalfeed", \
                                                        SignalFeed("{},signalfeed,{},{},{},{},{},{},{},{},{},{}".format(md.timestamp, \

                                        "SimulationMarket", \

                                        md.productCode, \
                                        
                                        "oid_" +  md.timestamp, \

                                        md.lastPrice, \

                                        int(md.lastVolume), \

                                        "open", \

                                        1, \

                                        "insert", \

                                        "limit_order", \

                                        "today", \

                                        ""))))
                '''
        def calculateUO(self, md):
            product = md.productCode
            uo = ULTOSC(np.array(self._high[product]), np.array(self._low[product]), np.array(self._close[product]), avg1, avg2, avg3)[-1]
            
            if product not in self.buy_flag:
                self.buy_flag[product] = 0
            if product not in self.sell_flag:
                self.sell_flag[product] = 0
            if product not in self.position:
                self.position[product] = 0
            # print 'Product:', product,'  UO:', uo, '  buy flag:', self.buy_flag[product], '  sell flag:', self.sell_flag[product]
            # buy signal
            # step 1: the low of the Divergence should be below the lowlevel
            if self.buy_flag[product] == 0 and uo < lowlevel:
                self.buy_flag[product] = 1
                self.uohigh_b[product] = uo
                self.uolow_b[product] = uo
                self.price_b[product] = self._low[product][-1]
            elif self.buy_flag[product] == 1 and uo < lowlevel:
                if uo < self.uolow_b[product]:
                    self.uolow_b[product] = uo
                self.uohigh_b[product] = uo
                self.price_b[product] = self._low[product][-1]
            # step 2: bullish Divergence forms meaning price forms a lower low while UO makes a higher lower
            elif self.buy_flag[product] == 1 and uo >= lowlevel:
                self.buy_flag[product] = 2
            elif self.buy_flag[product] == 2:
                if uo < lowlevel:
                    del self.buy_flag[product]
                else:
                    if self._low[product][-1] < self.price_b[product]:
                        self.buy_flag[product] = 3
                    if uo > self.uohigh_b[product]:
                        self.uohigh_b[product] = uo
            # step 3: UO breaks above the high of the Divergence
            else:
                if self.buy_flag[product] == 3 and uo > self.uohigh_b[product]:
                    '''
                    if product not in self.buy_signal:
                        self.buy_signal[product] = []
                    self.buy_signal[product].append([md.timestamp, 'b'])
                    '''

                    # print 'Buy:', product, '  ', md.timestamp, '  price:', md.lastPrice, '  volume:', md.lastVolume
                    if self.position[product] >= 0:
                        size = ordersize
                    else:
                        size = abs(self.position[product]) + ordersize

                    print 'buy' + ',' + str(product) + ',' + str(md.timestamp) + ',' + str(size) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
                    self.m_evt_mgr.insertEvt(Evt(1, "final_signalfeed", \
                                                    SignalFeed("{},signalfeed,{},{},{},{},{},{},{},{},{},{}".format(md.timestamp, \

                                    "SimulationMarket", \

                                    md.productCode, \
                                        
                                    "oid_" +  md.timestamp, \

                                    md.lastPrice, \

                                    # int(md.lastVolume), \
                                    size, \

                                    "open", \

                                    1, \

                                    "insert", \

                                    "limit_order", \

                                    "today", \

                                    ""))))
                    # reset flag and parameters
                    del self.buy_flag[product]


            # sell signal
            # step 1: the high of the Divergence should be above the highlevel
            if self.sell_flag[product] == 0 and uo > highlevel:
                self.sell_flag[product] = 1
                self.uohigh_s[product] = uo
                self.uolow_s[product] = uo
                self.price_s[product] = self._high[product][-1]

            elif self.sell_flag == 1 and uo > highlevel:
                if uo > self.uohigh_s[product]:
                    self.uohigh_s[product] = uo
                self.uolow_s[product] = uo
                self.price_s[product] = self._high[product][-1]
            elif self.sell_flag[product] == 1 and uo <= highlevel:
                self.sell_flag[product] = 2
            
            # step 2: bearish Divergence forms meaning price forms a higher high while UO makes a lower high
            elif self.sell_flag[product] == 2:
                if uo > highlevel:
                    del self.sell_flag[product]
                else:
                    if self._high[product][-1] > self.price_s[product]:
                        self.sell_flag[product] = 3
                    if uo < self.uolow_s[product]:
                        self.uolow_s[product] = uo
            # step 3: UO breaks below the low of the Divergence
            else:
                if self.sell_flag[product] == 3 and uo < self.uolow_s[product]:
                    '''
                    if product not in self.sell_signal:
                        self.sell_signal[product] = []
                    self.sell_signal[product].append([md.timestamp, 's'])
                    '''

                    # print 'Sell:', product, '  ', md.timestamp, '  price:', md.lastPrice, '  volume:', md.lastVolume
                    if self.position[product] <= 0:
                        size = ordersize
                    else:
                        size = abs(self.position[product]) + ordersize
                    print 'sell' + ',' + str(product) + ',' + str(md.timestamp) + ',' + str(size) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
                    self.m_evt_mgr.insertEvt(Evt(1, "final_signalfeed", \
                                                    SignalFeed("{},signalfeed,{},{},{},{},{},{},{},{},{},{}".format(md.timestamp, \

                                    "SimulationMarket", \

                                    md.productCode, \
                                        
                                    "oid_" +  md.timestamp, \

                                    md.lastPrice, \

                                    # int(md.lastVolume), \
                                    size, \

                                    "open", \

                                    2, \

                                    "insert", \

                                    "limit_order", \

                                    "today", \

                                    ""))))
                    # reset flag and parameters
                    del self.sell_flag[product]

        

        def calculateM(self, md):
            pass


        def calculateCV(self, md):
            pass


        def on_tradefeed(self, tf):
                if tf.productCode not in self.day_tradelog:
                    self.day_tradelog[tf.productCode] = [[0,0],[0,0]]
                self.day_tradelog[tf.productCode][tf.buySell-1][0] += int(tf.volumeFilled)
                self.day_tradelog[tf.productCode][tf.buySell-1][1] += int(tf.volumeFilled) * float(tf.price)
                # self.day_tradelog[tf.productCode].append(tf)
                # if tf.productCode not in self.position:
                #     self.position[tf.productCode] = 0
                if tf.buySell == 1:
                    self.position[tf.productCode] += tf.volumeFilled
                if tf.buySell == 2:
                    self.position[tf.productCode] -= tf.volumeFilled

                # ['__doc__', '__init__', '__module__', 'buySell', 'deleted', 'errorDescription', 'market', 'orderID', 'price', 'productCode', 'source', 'status', 'timestamp', 'tradeID', 'volume', 'volumeFilled']
                # print "tf:" + str(tf.price)
                # print 'trade feed,' + str(tf.productCode) + ',' + str(tf.buySell) + ',' + str(tf.volumeFilled) + ',' + str(tf.price)
                print ','.join(map(str, ['tradefeed', tf.productCode, tf.timestamp, tf.buySell, tf.volumeFilled, tf.price]))


        def calculatePNL(self, product, dt, lastPrice):
            # if product not in self.all_pnl:
            #     self.all_pnl[product] = []
            if product not in self.day_tradelog:
                self.day_tradelog[product]= [[0,0.0], [0,0.0]]
            [buy_size, buy_cashflow] = self.day_tradelog[product][0]
            [sell_size, sell_cashflow] = self.day_tradelog[product][1]
            if product in self.day_position:
                [position_old, positionprice_avg] = self.day_position[product]
                if position_old > 0:
                    buy_size += position_old
                    buy_cashflow += position_old * positionprice_avg
                    # buyprice_avg = (buy_cashflow + position_old * positionprice_avg) / buy_size
                if position_old < 0:
                    sell_size += abs(position_old)
                    sell_cashflow += abs(position_old) * positionprice_avg
                    # sellprice_avg = (sell_cashflow + abs(position_old) * positionprice_avg) / sell_size
            if buy_size == 0:
                buyprice_avg = 0.0
            else:
                buyprice_avg = buy_cashflow/buy_size
            if sell_size == 0:
                sellprice_avg = 0.0
            else:
                sellprice_avg = sell_cashflow/sell_size
            realized = min(buy_size, sell_size) * (sellprice_avg-buyprice_avg)
            position = buy_size - sell_size
                
            if position == 0:
                unrealized = 0.0
                self.day_position[product] = [0, 0.0]
            else:
                if position > 0:
                    unrealized = position * (lastPrice - buyprice_avg)
                if position < 0:
                    unrealized = position * (lastPrice - sellprice_avg)
                self.day_position[product] = [position, (sell_cashflow-buy_cashflow) / abs(position)]
            print ','.join(map(str, ['pnl', product, dt.strftime('%Y%m%d'), position, realized, unrealized]))
            del self.day_tradelog[product]

            # self.all_pnl.append([dt, self.day_position[product], realized, unrealized])

        
        def start(self):
                print "SampleEvtGenerator.start()"
                # time interval options: 1min, 5min, 15min
                self.OHLCV_length = 1
                self._ohlcv = {}
                self._high, self._low, self._close = {},{},{}
                self.position = {}

                # -------------------------------- Ultimate Oscillator -------------------------------
                # ------------- buy parameters -----------------
                self.buy_flag = {}
                self.uohigh_b, self.uolow_b, self.price_b = {}, {}, {}

                # ------------- sell parameters -----------------
                self.sell_flag = {}
                self.uohigh_s, self.uolow_s, self.price_s = {}, {}, {}
                # ------------------------------------------------------------------------------------


                # ------------------------------------- Momentum ------------------------------------ 

                # ------------------------------------------------------------------------------------





                # ------------- pnl variables -----------------
                self.day_tradelog = {}
                # self.all_pnl = {}
                self.day_position = {}

                self._ts = TradeStatusEvtGenerator(self.m_evt_mgr)

                
                #self._latest_md_price = {}
                #self._latest_md_vol = {}