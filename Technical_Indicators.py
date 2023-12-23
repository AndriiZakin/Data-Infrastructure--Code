import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Historical_Pricing import DataStorageManager  

class MovingAverages:
    def __init__(self, pricing_data):
        self.pricing_data = pricing_data
        self.ma_data = None

    def compute_simple_moving_average(self, window_length):
        # Implementation details for computing simple moving average
        self.ma_data = self.pricing_data.groupby('instrument')['close'].rolling(window=window_length).mean().reset_index()
        self.ma_data = self.ma_data.rename(columns={'close': 'simple_moving_average'})

    def compute_exponential_moving_average(self, span):
        # Implementation details for computing exponential moving average
        self.ma_data = self.pricing_data.groupby('instrument')['close'].ewm(span=span, adjust=False).mean().reset_index()
        self.ma_data = self.ma_data.rename(columns={'close': 'exponential_moving_average'})

    def output_ma_time_series(self):
        # Implementation details for serializing MA time series
        DataStorageManager.serialize_to_cloud_storage(self.ma_data, 'ma_time_series.json')


class BollingerBands:
    def __init__(self, pricing_data):
        self.pricing_data = pricing_data
        self.bb_data = None

    def compute_bollinger_bands(self, lookback_period, deviation):
        # Implementation details for computing Bollinger Bands
        ma = self.pricing_data.groupby('instrument')['close'].rolling(window=lookback_period).mean().reset_index()
        std = self.pricing_data.groupby('instrument')['close'].rolling(window=lookback_period).std().reset_index()

        upper_band = ma['close'] + deviation * std['close']
        lower_band = ma['close'] - deviation * std['close']

        self.bb_data = pd.DataFrame({
            'instrument': self.pricing_data['instrument'],
            'date': self.pricing_data['date'],
            'upper_band': upper_band,
            'lower_band': lower_band
        })

    def output_bb_time_series(self):
        # Implementation details for serializing Bollinger Bands time series
        DataStorageManager.serialize_to_cloud_storage(self.bb_data, 'bb_time_series.json')


class RelativeStrengthIndex:
    def __init__(self, pricing_data):
        self.pricing_data = pricing_data
        self.rsi_data = None

    def calculate_rsi(self, window, overbought_threshold, oversold_threshold):
        # Implementation details for calculating Relative Strength Index
        delta = self.pricing_data.groupby('instrument')['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.rolling(window=window, min_periods=1).mean()
        avg_loss = loss.rolling(window=window, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        self.rsi_data = pd.DataFrame({
            'instrument': self.pricing_data['instrument'],
            'date': self.pricing_data['date'],
            'rsi': rsi
        })

        # Mark overbought and oversold conditions
        self.mark_overbought_oversold(overbought_threshold, oversold_threshold)

    def mark_overbought_oversold(self, overbought_threshold, oversold_threshold):
        # Mark overbought and oversold conditions in the RSI data
        self.rsi_data['overbought'] = (self.rsi_data['rsi'] > overbought_threshold).astype(int)
        self.rsi_data['oversold'] = (self.rsi_data['rsi'] < oversold_threshold).astype(int)

    def output_rsi_time_series(self):
        # Implementation details for serializing RSI time series
        DataStorageManager.serialize_to_cloud_storage(self.rsi_data, 'rsi_time_series.json')

class VisualVerifier:
    def __init__(self, pricing_data, technical_indicator_data):
        self.pricing_data = pricing_data
        self.technical_indicator_data = technical_indicator_data

    def plot_overlay(self, instrument, indicator_type):
        # Implementation details for visual verification
        # Plot raw price overlayed with the specified technical indicator
        instrument_data = self.pricing_data[self.pricing_data['instrument'] == instrument]
        indicator_data = self.technical_indicator_data[
            (self.technical_indicator_data['instrument'] == instrument) &
            (self.technical_indicator_data['indicator_type'] == indicator_type)
        ]

        plt.figure(figsize=(10, 6))
        plt.plot(instrument_data['date'], instrument_data['close'], label='Raw Price', color='blue')
        plt.plot(indicator_data['date'], indicator_data['value'], label=indicator_type, color='orange')
        plt.title(f'{instrument} - {indicator_type} Overlay')
        plt.xlabel('Date')
        plt.ylabel('Value')
        plt.legend()
        plt.show()

    def assist_parameter_tuning(self, indicator_type, parameter_range):
        # Implementation details for assisting parameter tuning
        # Plot the indicator for different parameter values within the specified range
        instrument = self.pricing_data['instrument'].iloc[0]
        plt.figure(figsize=(10, 6))

        for parameter_value in parameter_range:
            indicator_data = self.technical_indicator_data[
                (self.technical_indicator_data['instrument'] == instrument) &
                (self.technical_indicator_data['indicator_type'] == indicator_type) &
                (self.technical_indicator_data['parameter'] == parameter_value)
            ]
            plt.plot(indicator_data['date'], indicator_data['value'], label=f'{indicator_type} - {parameter_value}')

        plt.title(f'{instrument} - {indicator_type} Parameter Tuning')
        plt.xlabel('Date')
        plt.ylabel('Value')
        plt.legend()
        plt.show()

class TechnicalIndicatorsDataStorageManager:
    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        self.historical_storage_manager = DataStorageManager(connection_string, container_name)

    def serialize_indicator_time_series(self, indicator_data: pd.DataFrame, instrument: str, indicator_type: str):
        # Implementation details for serializing indicator time series
        DataStorageManager.serialize_to_cloud_storage(indicator_data, f'{instrument}_{indicator_type}_time_series.json')

    def support_incremental_updates(self, existing_data: pd.DataFrame, new_data: pd.DataFrame):
        # Implementation details for supporting incremental updates
        updated_data = pd.concat([existing_data, new_data]).drop_duplicates()
        return updated_data