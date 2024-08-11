from datetime import datetime, timedelta
import numpy as np
import pandas as pd


class MakeTradingBars:

    def __init__(self, ticks):
        """

        NO MISSING VALUES IS MANDATORY !

        ticks (DataFrame):you need to have 5 columns in the dataframe:
        Timestamp(as index), bid, ask, volume, index(column from 0
        to number of line with a step of one to make some calculations easily).

        """

        self.ticks = ticks

    def make_slippage_price(self, start_date, price_type):
        """
        Find the slipped price of the associated price. There are two parameters:
        - start_date (Timestamp): the date of entry in position
        - price_type (str): 2 possibilities, 'bid' and 'ask'
        """
        if price_type not in ["bid", "ask"]:
            raise Exception("PRICE_TYPE must be 'bid' or 'ask'")

        # Define Timestamp from the index to 1s later
        end_date = start_date + timedelta(seconds=1)

        # Subsample the ticks from current date to 1s later
        period = self.ticks.loc[start_date:end_date][price_type]

        # We take the worst case using a 1second slippage
        if len(period) > 0:
            slippage_price = period.max() if price_type == "bid" else period.min()

        # There is no ticks in the subsample, we take the previous value
        else:
            index = self.ticks.loc[start_date:]["index"][0] - 1
            slippage_price = self.ticks.iloc[index][price_type]

        return slippage_price

    def time_bars_building(self, resample_factor="5T"):
        """
        resample_factor: Put a timeframe higher than 10s:
                s = second - T=minute - H = hour - D = day"
        """

        # Create an empty dataframe which will contains the bars
        self.time_bars = pd.DataFrame()

        # Sample every Xtime
        time_bars_bid = self.ticks.bid.resample(resample_factor)
        time_bars_ask = self.ticks.ask.resample(resample_factor)
        time_bars_vol = self.ticks.volume.resample(resample_factor)
        time_bars_inx = self.ticks["index"].resample(resample_factor)

        # Dataframe filling
        self.time_bars["open"] = (time_bars_bid.first() + time_bars_ask.first()) / 2
        self.time_bars["high"] = (time_bars_bid.max() + time_bars_ask.max()) / 2
        self.time_bars["low"] = (time_bars_bid.min() + time_bars_ask.min()) / 2
        self.time_bars["close"] = (time_bars_bid.last() + time_bars_ask.last()) / 2
        self.time_bars["volume"] = time_bars_vol.sum()
        self.time_bars["high_time"] = self.ticks.groupby(pd.Grouper(freq=resample_factor))['ask'].idxmax()
        self.time_bars["low_time"] = self.ticks.groupby(pd.Grouper(freq=resample_factor))['ask'].idxmin()


        # We fix the problem when the first value of the resampling is after the index time
        # EX: the first time value tick of the bar is at 16:15:52 but the bar index is 16:00:00. If we keep that, we have a problem
        self.time_bars["first_index"] = time_bars_inx.first()
        self.time_bars["first_time"] = time_bars_bid.apply(lambda x: x.index[0] if not x.empty else pd.NaT)

        for idx in self.time_bars.index:
            if idx < self.time_bars["first_time"][idx]:
                index = self.ticks.loc[idx:]["index"][0] - 1
                self.time_bars.loc[idx,"open"] = (self.ticks.iloc[index]["bid"] + self.ticks.iloc[index]["ask"]) / 2

        # Create empty series to fill them with the slippage prices
        open_bid_slippage = pd.Series(index=self.time_bars.index, dtype='float64')
        open_ask_slippage = pd.Series(index=self.time_bars.index, dtype='float64')

        # Fill empty series with slippage prices
        for idx in self.time_bars.index:
            open_bid_slippage[idx] = self.make_slippage_price(idx, "bid")
            open_ask_slippage[idx] = self.make_slippage_price(idx, "ask")

            # Create columns in time_bars dataframe with previous Series
        self.time_bars["open_bid_slippage"] = open_bid_slippage
        self.time_bars["open_ask_slippage"] = open_ask_slippage

    def tick_bars_building(self, N=1000):
        """
        N(int): number of ticks per candle
        """

        T = len(self.ticks)

        # Future list of lists to create the tick bars dataframe (created later)
        bars_values = []

        # We extract the OHLCV + timestamp data each N ticks
        for i in range(T // N):
            # Subsample period initialization
            start_period = N * i
            end_period = N * (i + 1)

            # Extract Bid price, Ask price and volume for the sample period
            ticks_sample = self.ticks.iloc[start_period:end_period, :]
            ticks_sample_price = (ticks_sample["bid"] + ticks_sample["ask"]) / 2
            ticks_sample_volume = ticks_sample["volume"]

            # Create OHLCV + Timestamp features
            timestamp = ticks_sample_price.index[0]
            open_price = ticks_sample_price.iloc[0]
            high_price = ticks_sample_price.max()
            low_price = ticks_sample_price.min()
            close_price = ticks_sample_price.iloc[-1]
            volume = ticks_sample_volume.sum()
            high_time = ticks_sample_price.idxmax()
            low_time = ticks_sample_price.idxmin()

            # One line OHLCV + Timestamp data
            bars_values.append([timestamp, open_price, high_price,
                                low_price, close_price, volume,
                                high_time, low_time])

        # Create tick bars dataframe
        self.tick_bars = pd.DataFrame(bars_values, columns=["time", "open", "high", "low", "close", "volume",
                                                            "high_time", "low_time"])
        self.tick_bars = self.tick_bars.set_index("time")

        # Create empty series to fill them with the slippage prices
        open_bid_slippage = pd.Series(index=self.tick_bars.index, dtype='float64')
        open_ask_slippage = pd.Series(index=self.tick_bars.index, dtype='float64')

        # Fill empty series with slippage prices
        for idx in self.tick_bars.index:
            open_bid_slippage[idx] = self.make_slippage_price(idx, "bid")
            open_ask_slippage[idx] = self.make_slippage_price(idx, "ask")

            # Create columns in time_bars dataframe with previous Series
        self.tick_bars["open_bid_slippage"] = open_bid_slippage
        self.tick_bars["open_ask_slippage"] = open_ask_slippage


    def tick_run_bars_building(self, expected_imbalance=100):

        # Create tick sign: -1 if var<0 and 1 if var>0
        self.ticks["price"] = (self.ticks.bid + self.ticks.ask) / 2
        #print(self.ticks)
        self.ticks["sign_var"] = np.sign(self.ticks.price.pct_change(1))
        self.ticks = self.ticks.dropna()

        # Parameters initialization
        start_date = self.ticks.index[0]
        bars_values = []
        rolling = False  # Allows us to obtain the start and end dates for each subsample
        nb_ticks = 0
        current_imbalance = 0

        for idx, sign in zip(self.ticks.index, self.ticks.sign_var):

            # Reset the start_date after a complete bar
            if rolling:
                rolling = False
                start_date = idx
                current_imbalance = 0
                nb_ticks = 0

            # Increments the imbalance by the sign of this tick
            current_imbalance += sign

            # Recrods the number of ticks (in the bar)
            nb_ticks += 1

            # Create a candle when the current imbalance is significative
            if abs(current_imbalance) > abs(expected_imbalance):
                # Define end_date for this bar
                end_date = idx

                # Extract Bid price, Ask price and volume for the sample period
                ticks_sample = self.ticks.loc[start_date:end_date, :]
                ticks_sample_price = (ticks_sample["bid"] + ticks_sample["ask"]) / 2
                ticks_sample_volume = ticks_sample["volume"]

                # Create OHLCV + Timestamp features
                timestamp = ticks_sample_price.index[0]
                open_price = ticks_sample_price.iloc[0]
                high_price = ticks_sample_price.max()
                low_price = ticks_sample_price.min()
                close_price = ticks_sample_price.iloc[-1]
                volume = ticks_sample_volume.sum()
                high_time = ticks_sample_price.idxmax()
                low_time = ticks_sample_price.idxmin()

                # One line OHLCV + Timestamp data
                bars_values.append([timestamp, open_price, high_price,
                                    low_price, close_price, volume, nb_ticks,
                                    high_time, low_time])

                # We activate the rolling to change the start date for the new bar
                rolling = True

        self.tick_run_bars = pd.DataFrame(bars_values,
                                          columns=["time", "open", "high", "low", "close", "volume", "number_ticks",
                                                   "high_time", "low_time"])
        self.tick_run_bars = self.tick_run_bars.set_index("time")

        # Create empty series to fill them with the slippage prices
        open_bid_slippage = pd.Series(index=self.tick_run_bars.index, dtype='float64')
        open_ask_slippage = pd.Series(index=self.tick_run_bars.index, dtype='float64')

        # Fill empty series with slippage prices
        for idx in self.tick_run_bars.index:
            open_bid_slippage[idx] = self.make_slippage_price(idx, "bid")
            open_ask_slippage[idx] = self.make_slippage_price(idx, "ask")

            # Create columns in time_bars dataframe with previous Series
        self.tick_run_bars["open_bid_slippage"] = open_bid_slippage
        self.tick_run_bars["open_ask_slippage"] = open_ask_slippage

