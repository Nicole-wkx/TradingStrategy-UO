#!/usr/bin/python
from evt_framework import *
from std_feeds import *
from datetime import datetime
import numpy as np
from talib import ULTOSC

# -------------- UO Parameters ------------------
lowlevel = 30
highlevel = 70
midlevel = 50
avg1 = 12
avg2 = 2*avg1
avg3 = 2*avg2

signalPath = './trading_signals'
# -------------------- End ----------------------


class SampleEvtGenerator(EvtGenerator):
        def __init__(self, evt_mgr):
                print "SampleEvtGenerator __init__:"
                EvtGenerator.__init__(self, evt_mgr)

        def on_marketdatafeed(self, md):
                current_time = datetime(int(md.timestamp[:4]), int(md.timestamp[4:6]),  int(md.timestamp[6:8]), int(md.timestamp[9:11]),  int(md.timestamp[11:13]),  int(md.timestamp[13:15]))
                if (current_time - self.timestamp).seconds > 60 * self.OHLCV_length:
                    print self._ohlcv
                    if md.productCode not in self._high:
                        self._high[md.productCode], self._low[md.productCode], self._close[md.productCode] = [],[],[]
                    if md.productCode in self._ohlcv:
                        self._high[md.productCode].append(self._ohlcv[md.productCode][2])
                        self._low[md.productCode].append(self._ohlcv[md.productCode][3])
                        self._close[md.productCode].append(self._ohlcv[md.productCode][4])

                        # keep the length of high, low, close list to be avg1
                        if len(self._high[md.productCode]) > avg3+1:
                            del self._high[md.productCode][0]
                            del self._low[md.productCode][0]
                            del self._close[md.productCode][0]
                        
                        # self.count += 1
                        # if self.count >= avg1:
                        #     self.calculateUO(md)
                        # self.append_list()
                        if len(self._high[md.productCode]) == avg3+1:
                            self.calculateUO(md)
                    self.timestamp = datetime(current_time.year, current_time.month, current_time.day, current_time.hour, current_time.minute)
                    self._ohlcv.clear()
                        

                if md.productCode not in self._ohlcv:
                    self._ohlcv[md.productCode] = [
                        self.timestamp.strftime('%Y%m%d_%H%M%S'),
                        md.lastPrice,
                        md.lastPrice,
                        md.lastPrice,
                        md.lastPrice
                    ]
                else:
                    self._ohlcv[md.productCode] = [
                        self._ohlcv[md.productCode][0], \
                        self._ohlcv[md.productCode][1], \
                        max(self._ohlcv[md.productCode][2], md.lastPrice), \
                        min(self._ohlcv[md.productCode][3], md.lastPrice), \
                        md.lastPrice
                    ]

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
            print 'UO:', uo, ' buy flag:', self.buy_flag, ' sell flag:', self.sell_flag
            # buy signal
            # step 1: the low of the Divergence should be below the lowlevel
            if self.buy_flag == 0 and uo < lowlevel:
                self.buy_flag = 1
                self.uohigh_b = uo
                self.uolow_b = uo
                self.price_b = self._low[product][-1]
            # step 2: bullish Divergence forms meaning price forms a lower low while UO makes a higher low
            if self.buy_flag == 1:
                if uo > self.uohigh_b:
                    self.uohigh_b = uo
                if self._low[product][-1] < self.price_b and uo > self.uolow_b and uo > lowlevel:
                    self.buy_flag = 2
            # step 3: UO breaks above the high of the Divergence
            if self.buy_flag == 2:
                if uo > self.uohigh_b:
                    if product not in self.buy_signal:
                        self.buy_signal[product] = []
                    self.buy_signal[product].append([self.timestamp, 'b'])
                    print 'Buy:', md.timestamp, ' price:', md.lastPrice, ' volume:', md.lastVolume
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
                    self.buy_flag = 0
                    self.uohigh_b, self.uolow_b, self.price_b = 0.0, 0.0, 0.0


            # sell signal
            # step 1: the high of the Divergence should be above the highlevel
            if self.sell_flag == 0 and uo > highlevel:
                self.sell_flag = 1
                self.uohigh_s = uo
                self.uolow_s = uo
                self.price_s = self._high[product][-1]
            # step 2: bearish Divergence forms meaning price forms a higher high while UO makes a lower high
            if self.sell_flag == 1:
                if uo < self.uolow_s:
                    self.uolow_s = uo
                if self._high[product][-1] > self.price_s and uo < self.uohigh_s and uo < highlevel:
                    self.sell_flag = 2
            # step 3: UO breaks below the low of the Divergence
            if self.sell_flag == 2:
                if uo < self.uolow_s:
                    if product not in self.sell_signal:
                        self.sell_signal[product] = []
                    self.sell_signal[product].append([self.timestamp, 's'])
                    print 'Sell:', md.timestamp, ' price:', md.lastPrice, ' volume:', md.lastVolume
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
                    self.sell_flag = 0
                    self.uohigh_s, self.uolow_s, self.price_s = 0.0, 0.0, 0.0


        def on_tradefeed(self, tf):
                print "tf:" + str(tf.price)
        
        def start(self):
                print "SampleEvtGenerator.start()"
                # time interval: 1min, 5min, ...
                self.OHLCV_length = 5
                self.timestamp = datetime(1995,1,1)
                self._ohlcv = {}
                self._high, self._low, self._close = {},{},{}
                self.count = 0
                
                # ------------- buy parameters -----------------
                self.buy_flag = 0
                self.uohigh_b, self.uolow_b, self.price_b = 0.0, 0.0, 0.0
                self.buy_signal = {}

                # ------------- sell parameters -----------------
                self.sell_flag = 0
                self.uohigh_s, self.uolow_s, self.price_s = 0.0, 0.0, 0.0
                self.sell_signal = {}
                
                #self._latest_md_price = {}
                #self._latest_md_vol = {}
