#!/usr/bin/python
from evt_framework import *
from std_feeds import *
from valgo.evt_generator.utilities.trade_status_evt_generator import TradeStatusEvtGenerator

from datetime import datetime, time, timedelta
import numpy as np
from talib import ULTOSC, MOM, EMA

# -------------- UO Parameters ------------------
lowlevel = 30
highlevel = 70
midlevel = 50
avg1 = 5
avg2 = 2*avg1
avg3 = 2*avg2
# -------------------- End ----------------------

cv_period = avg3

# -------------- Market Parameters ------------------
# trading hours: two periods, 9:15 - 12:00, 13:00 - 16:00
tradingHours = [ [time(9,15), time(12)], [time(13), time(16)] ]
day_start_time = tradingHours[0][0]
day_end_time = tradingHours[-1][1]
# -------------------- End ----------------------

# -------------- Order Parameters ----------------
ordersize = 1
# -------------------- End ----------------------


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
                        while len(self._high[md.productCode]) > 2*avg3:
                            del self._high[md.productCode][0]
                            del self._low[md.productCode][0]
                            del self._close[md.productCode][0]
                        if len(self._high[md.productCode]) >= avg3+1:
                            self.calculateUO(md)
                        if current_time.time() == day_end_time:
                            del self._ohlcv[md.productCode]
                            del self._high[md.productCode]
                            del self._low[md.productCode]
                            del self._close[md.productCode]
                            del self.buy_flag[md.productCode]
                            del self.sell_flag[md.productCode]

                            # -------------------- calculate pnl at the end of the day --------------------
                            # self.calculatePNL(md.productCode, current_time.date(), float(md.lastPrice))
                            '''
                            ['_MutableMapping__marker', '__abstractmethods__', '__class__', '__contains__', 
                            '__delattr__', '__delitem__', '__dict__', '__doc__', '__eq__', '__format__', '__getattribute__', 
                            '__getitem__', '__hash__', '__init__', '__iter__', '__keytransform__', '__len__', '__metaclass__', 
                            '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', 
                            '__setitem__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_abc_cache', 
                            '_abc_negative_cache', '_abc_negative_cache_version', '_abc_registry', '_enable_update_risk', 
                            '_product_ts', '_refresh_realized_risk_status', '_refresh_total_risk_status', 
                            '_refresh_unrealized_risk_status', '_status', 'clear', 'clear_pnl', 'enable_auto_update_risk_status', 
                            'get', 'get_all_product_positions', 'get_all_product_trade_status', 'get_product_codes', 
                            'get_product_positions', 'get_product_trade_status', 'items', 'iteritems', 'iterkeys', 
                            'itervalues', 'keys', 'pop', 'popitem', 'setdefault', 'update', 'update_product_trade_status', 
                            'update_timestamp', 'values']
                            '''
                            ts = self._ts.get_current_trade_status()
                            positioninfo = ts.get_product_positions(md.productCode)
                            tradeinfo = ts.get_product_trade_status(md.productCode)
                            print positioninfo
                            print ','.join(map(str, ['pnl', md.productCode, current_time.strftime('%Y%m%d'), \
                                positioninfo['1'], positioninfo['-1'], positioninfo['1'] + positioninfo['-1'], \
                                tradeinfo['realized_pnl'], tradeinfo['unrealized_pnl'], tradeinfo['transaction_cost']]))
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
            if product not in self.order_uo:
                self.order_uo[product] = 0
            # print 'Product:', product,'  UO:', uo, '  buy flag:', self.buy_flag[product], '  sell flag:', self.sell_flag[product]
            # buy signal
            # step 1: the low of the Divergence should be below the lowlevel
            if self.buy_flag[product] == 0 and uo < lowlevel:
                self.buy_flag[product] = 1
                self.uohigh_b[product] = uo
                self.uolow_b[product] = uo
                self.price_b[product] = self._low[product][-1]
            elif self.buy_flag[product] == 1 and uo < lowlevel:
                self.uohigh_b[product] = max(self.uohigh_b[product], uo)
                self.uolow_b[product] = min(self.uolow_b[product], uo)
                self.price_b[product] = min(self.price_b[product], self._low[product][-1])
            elif self.buy_flag[product] == 1 and uo >= lowlevel:
                self.buy_flag[product] = 2
            # step 2: bullish Divergence forms meaning price forms a lower low while UO makes a higher lower
            elif self.buy_flag[product] == 2:
                if uo < lowlevel or uo > highlevel:
                    del self.buy_flag[product]
                else:
                    if self._low[product][-1] < self.price_b[product]:
                        self.buy_flag[product] = 3
                    self.uohigh_b[product] = max(self.uohigh_b[product], uo)
            # step 3: UO breaks above the high of the Divergence
            elif self.buy_flag[product] == 3:
                if uo < lowlevel or uo > highlevel:
                    del self.buy_flag[product]
                else:
                    if uo > self.uohigh_b[product]:
                        m = self.calculateM(md)
                        if m <= 0:
                            self.order_uo[product] = 1
                        else:
                            self.order_uo[product] = 0
                        del self.buy_flag[product]
            else:
                pass


            # sell signal
            # step 1: the high of the Divergence should be above the highlevel
            if self.sell_flag[product] == 0 and uo > highlevel:
                self.sell_flag[product] = 1
                self.uohigh_s[product] = uo
                self.uolow_s[product] = uo
                self.price_s[product] = self._high[product][-1]

            elif self.sell_flag == 1 and uo > highlevel:
                self.uohigh_s[product] = max(self.uohigh_s[product], uo)
                self.uolow_s[product] = min(self.uolow_s[product], uo)
                self.price_s[product] = max(self.price_s[product], self._high[product][-1])
            elif self.sell_flag[product] == 1 and uo <= highlevel:
                self.sell_flag[product] = 2
            
            # step 2: bearish Divergence forms meaning price forms a higher high while UO makes a lower high
            elif self.sell_flag[product] == 2:
                if uo > highlevel or uo < lowlevel:
                    del self.sell_flag[product]
                else:
                    if self._high[product][-1] > self.price_s[product]:
                        self.sell_flag[product] = 3
                    self.uolow_s[product] = min(self.uolow_s[product], uo)
            # step 3: UO breaks below the low of the Divergence
            elif self.sell_flag[product] == 3:
                if uo > highlevel or uo < lowlevel:
                    del self.sell_flag[product]
                else:
                    if uo < self.uolow_s[product]:
                        m = self.calculateM(md)
                        if m >= 0:
                            self.order_uo[product] = 2
                        else:
                            self.order_uo[product] = 0
                        # reset flag and parameters
                        del self.sell_flag[product]
            else:
                pass
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

            if self.buy_flag[product] == 0 and uo < lowlevel:
                self.buy_flag[product] = 1
            if (self.buy_flag[product] == 1 and uo > lowlevel) or self.buy_flag[product]==2:
                cv = self.calculateCV(md)
                if cv == 999999:
                    self.buy_flag[product] = 2
                else:
                    if cv > 0:
                        print 'buy' + ',' + str(md.productCode) + ',' + str(md.timestamp) + ',' + str(ordersize) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
                        self.m_evt_mgr.insertEvt(Evt(1, "final_signalfeed", \
                                                        SignalFeed("{},signalfeed,{},{},{},{},{},{},{},{},{},{}".format(md.timestamp, \

                                        "SimulationMarket", \

                                        md.productCode, \
                                                            
                                        "oid_" +  md.timestamp, \

                                        md.lastPrice, \

                                        # int(md.lastVolume), \
                                        ordersize, \

                                        "open", \

                                        1, \

                                        "insert", \

                                        "limit_order", \

                                        "today", \

                                        ""))))
                    self.buy_flag[product] = 0


            if self.sell_flag[product] == 0 and uo > highlevel:
                self.sell_flag[product] = 1
            if (self.sell_flag[product] == 1 and uo < highlevel) or self.sell_flag[product]==2:
                cv = self.calculateCV(md)
                if cv == 999999:
                    self.sell_flag[product] = 2
                else:
                    if cv < 0:
                        print 'sell' + ',' + str(md.productCode) + ',' + str(md.timestamp) + ',' + str(ordersize) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
                        self.m_evt_mgr.insertEvt(Evt(1, "final_signalfeed", \
                                                        SignalFeed("{},signalfeed,{},{},{},{},{},{},{},{},{},{}".format(md.timestamp, \

                                        "SimulationMarket", \

                                        md.productCode, \
                                                            
                                        "oid_" +  md.timestamp, \

                                        md.lastPrice, \

                                        # int(md.lastVolume), \
                                        ordersize, \

                                        "open", \

                                        2, \

                                        "insert", \

                                        "limit_order", \

                                        "today", \

                                        ""))))
                    self.sell_flag[product] = 0



        

        def calculateM(self, md):
            return MOM(np.array(self._close[md.productCode]),avg3)[-1]


        def calculateCV(self, md):
            differ = np.array(self._high[md.productCode]) - np.array(self._low[md.productCode])
            ma = EMA(differ, timeperiod=cv_period)
            if len(ma) == cv_period * 2:
                cv = (ma[-1] - ma[-1-cv_period]) / ma[-1-cv_period] *100
            else:
                cv = 999999
            return cv
            print 'Chaikin Volatility: ' + str(cv)

        '''
        def decideOrder(self, md):
            if self.order_uo[md.productCode] != 0:
                m = self.calculateM(md)
                if self.order_uo[md.productCode] == 1:
                    if m > 0:
                        self.order_uo[md.productCode] = 0
                        # place buy orders
                        if self.position[md.productCode] >= 0:
                            size = ordersize
                        else:
                            size = abs(self.position[md.productCode]) + ordersize
                        print 'buy' + ',' + str(md.productCode) + ',' + str(md.timestamp) + ',' + str(size) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
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
                if self.order_uo[md.productCode] == 2:
                    if m < 0:
                        self.order_uo[md.productCode] = 0
                        # place sell orders
                        if self.position[md.productCode] <= 0:
                            size = ordersize
                        else:
                            size = abs(self.position[md.productCode]) + ordersize
                        print 'sell' + ',' + str(md.productCode) + ',' + str(md.timestamp) + ',' + str(size) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
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
        '''


        def on_tradefeed(self, tf):
                '''
                if tf.productCode not in self.tradelog:
                    self.tradelog[tf.productCode] = [[0,0],[0,0]]
                self.tradelog[tf.productCode][tf.buySell-1][0] += int(tf.volumeFilled)
                self.tradelog[tf.productCode][tf.buySell-1][1] += int(tf.volumeFilled) * float(tf.price)
                '''
                # if tf.buySell == 1:
                #     self.position[tf.productCode] += tf.volumeFilled
                # if tf.buySell == 2:
                #     self.position[tf.productCode] -= tf.volumeFilled

                # ['__doc__', '__init__', '__module__', 'buySell', 'deleted', 'errorDescription', 'market', 'orderID', 'price', 'productCode', 'source', 'status', 'timestamp', 'tradeID', 'volume', 'volumeFilled']
                # print "tf:" + str(tf.price)
                # print 'trade feed,' + str(tf.productCode) + ',' + str(tf.buySell) + ',' + str(tf.volumeFilled) + ',' + str(tf.price)
                print ','.join(map(str, ['tradefeed', tf.productCode, tf.timestamp, tf.buySell, tf.volumeFilled, tf.price]))

        
        def start(self):
                print "SampleEvtGenerator.start()"
                # time interval options: 1min, 5min, 15min
                self.OHLCV_length = 1
                self._ohlcv = {}
                self._high, self._low, self._close = {},{},{}
                self.position = {}

                # -------------------------------- Ultimate Oscillator -------------------------------
                self.order_uo = {}
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
                self._ts = TradeStatusEvtGenerator(self.m_evt_mgr)
                # self.tradelog = {}
                # self.day_position = {}