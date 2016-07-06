#!/usr/bin/python
from evt_framework import *
from std_feeds import *
from datetime import datetime, time, timedelta
import numpy as np
from talib import ULTOSC

# -------------- UO Parameters ------------------
lowlevel = 30
highlevel = 70
midlevel = 50
avg1 = 7
avg2 = 2*avg1
avg3 = 2*avg2

start_time = time(9,15)
end_time = time(16)
# -------------------- End ----------------------


class SampleEvtGenerator(EvtGenerator):
        def __init__(self, evt_mgr):
                print "SampleEvtGenerator __init__:"
                EvtGenerator.__init__(self, evt_mgr)

        def on_marketdatafeed(self, md):
                # from md.timestamp to datetime structure as current_time
                current_time = datetime(int(md.timestamp[:4]), int(md.timestamp[4:6]),  int(md.timestamp[6:8]), int(md.timestamp[9:11]),  int(md.timestamp[11:13]),  int(md.timestamp[13:15]))
                if current_time.time() > end_time or current_time.time() < start_time:
                    return
                if md.productCode not in self._ohlcv:
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
                        if current_time.time() == end_time:
                            del self._ohlcv[md.productCode]
                        else:
                            # find the new this interval
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
            # print 'Product:', product,'  UO:', uo, '  buy flag:', self.buy_flag[product], '  sell flag:', self.sell_flag[product]
            # buy signal
            # step 1: the low of the Divergence should be below the lowlevel
            if self.buy_flag[product] == 0 and uo < lowlevel:
                self.buy_flag[product] = 1
                self.uohigh_b[product] = uo
                self.uolow_b[product] = uo
                self.price_b[product] = self._low[product][-1]
            # step 2: bullish Divergence forms meaning price forms a lower low while UO makes a higher low
            if self.buy_flag[product] == 1:
                if uo > self.uohigh_b[product]:
                    self.uohigh_b[product] = uo
                if self._low[product][-1] < self.price_b[product] and uo > self.uolow_b[product] and uo > lowlevel:
                    self.buy_flag[product] = 2
            # step 3: UO breaks above the high of the Divergence
            if self.buy_flag[product] == 2:
                if uo > self.uohigh_b[product]:
                    '''
                    if product not in self.buy_signal:
                        self.buy_signal[product] = []
                    self.buy_signal[product].append([md.timestamp, 'b'])
                    '''

                    # print 'Buy:', product, '  ', md.timestamp, '  price:', md.lastPrice, '  volume:', md.lastVolume
                    print 'buy' + ',' + str(product) + ',' + str(md.timestamp) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
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
                    # reset flag and parameters
                    del self.buy_flag[product]


            # sell signal
            # step 1: the high of the Divergence should be above the highlevel
            if self.sell_flag[product] == 0 and uo > highlevel:
                self.sell_flag[product] = 1
                self.uohigh_s[product] = uo
                self.uolow_s[product] = uo
                self.price_s[product] = self._high[product][-1]
            # step 2: bearish Divergence forms meaning price forms a higher high while UO makes a lower high
            if self.sell_flag[product] == 1:
                if uo < self.uolow_s[product]:
                    self.uolow_s[product] = uo
                if self._high[product][-1] > self.price_s[product] and uo < self.uohigh_s[product] and uo < highlevel:
                    self.sell_flag[product] = 2
            # step 3: UO breaks below the low of the Divergence
            if self.sell_flag[product] == 2:
                if uo < self.uolow_s[product]:
                    '''
                    if product not in self.sell_signal:
                        self.sell_signal[product] = []
                    self.sell_signal[product].append([md.timestamp, 's'])
                    '''

                    # print 'Sell:', product, '  ', md.timestamp, '  price:', md.lastPrice, '  volume:', md.lastVolume
                    print 'sell' + ',' + str(product) + ',' + str(md.timestamp) + ',' + str(md.lastPrice) + ',' + str(md.lastVolume)
                    self.m_evt_mgr.insertEvt(Evt(1, "final_signalfeed", \
                                                    SignalFeed("{},signalfeed,{},{},{},{},{},{},{},{},{},{}".format(md.timestamp, \

                                    "SimulationMarket", \

                                    md.productCode, \
                                        
                                    "oid_" +  md.timestamp, \

                                    md.lastPrice, \

                                    int(md.lastVolume), \

                                    "open", \

                                    2, \

                                    "insert", \

                                    "limit_order", \

                                    "today", \

                                    ""))))
                    # reset flag and parameters
                    del self.sell_flag[product]


        def on_tradefeed(self, tf):

                # ['__doc__', '__init__', '__module__', 'buySell', 'deleted', 'errorDescription', 'market', 'orderID', 'price', 'productCode', 'source', 'status', 'timestamp', 'tradeID', 'volume', 'volumeFilled']
                print "tf:" + str(tf.price)
        
        def start(self):
                print "SampleEvtGenerator.start()"
                # time interval options: 1min, 5min, 15min
                self.OHLCV_length = 1
                self._ohlcv = {}
                self._high, self._low, self._close = {},{},{}

                # ------------- buy parameters -----------------
                self.buy_flag = {}
                self.uohigh_b, self.uolow_b, self.price_b = {}, {}, {}

                # ------------- sell parameters -----------------
                self.sell_flag = {}
                self.uohigh_s, self.uolow_s, self.price_s = {}, {}, {}
                
                
                #self._latest_md_price = {}
                #self._latest_md_vol = {}