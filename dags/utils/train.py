import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
import warnings
warnings.filterwarnings('ignore')

# def train():
#     import yfinance as yf
#     import pymongo
#     mongo_conn = Variable.get("MONGO")
#     client = pymongo.MongoClient(mongo_conn)
#     db = client["finance_data"]
#     collection = db["AAPL"]
#     data = pd.DataFrame(list(collection.find()))
#     predictor = TradingPredictor(data, target_col='Close')


class TradingPredictor:
    def __init__(self, data: pd.DataFrame = None, target_col: str = 'Close'):
        self.df = data.copy() if data is not None else None
        self.target_col = target_col
        self.model = None
        self.fitted_model = None
        self.has_seasonality = False
        self.seasonal_period = None
        
        if self.df is not None:
            if 'Date' in self.df.columns:
                self.df['Date'] = pd.to_datetime(self.df['Date'])
                self.df.set_index('Date', inplace=True)
            
            self.series = self.df[target_col].dropna()
        else:
            self.series = None
    
    def detect_seasonality(self, periods_to_check=[5, 21, 63, 252]):
        if len(self.series) < 2 * max(periods_to_check):
            periods_to_check = [p for p in periods_to_check if len(self.series) >= 2 * p]
        
        if not periods_to_check:
            print("Not enough data for seasonality detection")
            return False, None
        
        best_period = None
        best_strength = 0
        
        for period in periods_to_check:
            try:
                strength = self._calculate_seasonal_strength(period)
                print(f"Period {period}: Seasonal strength = {strength:.4f}")
                
                if strength > best_strength:
                    best_strength = strength
                    best_period = period
            except Exception as e:
                print(f"Period {period}: Could not compute - {e}")
                continue
        
        threshold = 0.64
        
        if best_strength >= threshold:
            self.has_seasonality = True
            self.seasonal_period = best_period
            print(f"\nSeasonality detected! Period: {best_period}, Strength: {best_strength:.4f}")
        else:
            self.has_seasonality = False
            self.seasonal_period = None
            print(f"\nNo significant seasonality. Best strength: {best_strength:.4f}")
        
        return self.has_seasonality, self.seasonal_period
    
    def _calculate_seasonal_strength(self, period):
        decomposition = seasonal_decompose(self.series, model='additive', period=period)
        
        seasonal = decomposition.seasonal
        residual = decomposition.resid.dropna()
        
        var_residual = np.var(residual)
        var_seasonal_residual = np.var(seasonal[residual.index] + residual)
        
        if var_seasonal_residual == 0:
            return 0
        
        strength = max(0, 1 - (var_residual / var_seasonal_residual))
        print(f"Calculated seasonal strength for period {period}: {strength:.4f}")
        return strength
    
    def check_stationarity(self):
        result = adfuller(self.series.dropna())
        p_value = result[1]
        
        print(f"ADF Statistic: {result[0]:.4f}")
        print(f"p-value: {p_value:.4f}")
        
        if p_value <= 0.05:
            print("Series is stationary")
            return True, 0
        else:
            print("Series is non-stationary, differencing needed")
            d = 0
            temp_series = self.series.copy()
            while p_value > 0.05 and d < 3:
                d += 1
                temp_series = temp_series.diff().dropna()
                result = adfuller(temp_series)
                p_value = result[1]
            print(f"Recommended differencing order (d): {d}")
            return False, d
    
    def find_best_order(self, max_p=5, max_q=5, d=None):
        if d is None:
            _, d = self.check_stationarity()
        
        best_aic = float('inf')
        best_order = (1, d, 1)
        
        print(f"\nSearching for best order...")
        
        for p in range(max_p + 1):
            for q in range(max_q + 1):
                try:
                    model = ARIMA(self.series, order=(p, d, q))
                    fitted = model.fit()
                    
                    if fitted.aic < best_aic:
                        best_aic = fitted.aic
                        best_order = (p, d, q)
                except:
                    continue
        
        print(f"Best order: {best_order} with AIC: {best_aic:.2f}")
        return best_order
    
    def build_model(self, order=None, seasonal_order=None):
        if not self.has_seasonality and self.seasonal_period is None:
            print("Running seasonality detection first...")
            self.detect_seasonality()
        
        if order is None:
            order = self.find_best_order()
        
        if self.has_seasonality:
            if seasonal_order is None:
                s = self.seasonal_period
                seasonal_order = (1, 1, 1, s)
            
            print(f"\nBuilding SARIMAX{order}x{seasonal_order}")
            self.model = SARIMAX(
                self.series,
                order=order,
                seasonal_order=seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False
            )
        else:
            print(f"\nBuilding ARIMA{order}")
            self.model = ARIMA(self.series, order=order)
        
        self.fitted_model = self.model.fit()
        print(f"Model AIC: {self.fitted_model.aic:.2f}")
        print(f"Model BIC: {self.fitted_model.bic:.2f}")
        
        return self.fitted_model
    
    def predict(self, steps=30):
        if self.fitted_model is None:
            raise ValueError("Model not built yet. Call build_model() first.")
        
        forecast = self.fitted_model.get_forecast(steps=steps)
        predictions = forecast.predicted_mean
        conf_int = forecast.conf_int()
        
        results = pd.DataFrame({
            'Prediction': predictions,
            'Lower_CI': conf_int.iloc[:, 0],
            'Upper_CI': conf_int.iloc[:, 1]
        })
        
        return results
    
    def save_model(self, filepath):
        import pickle
        
        if self.fitted_model is None:
            raise ValueError("No model to save. Call build_model() first.")
        
        model_data = {
            'fitted_model': self.fitted_model,
            'has_seasonality': self.has_seasonality,
            'seasonal_period': self.seasonal_period,
            'target_col': self.target_col
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath):
        import pickle
        
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.fitted_model = model_data['fitted_model']
        self.has_seasonality = model_data['has_seasonality']
        self.seasonal_period = model_data['seasonal_period']
        self.target_col = model_data['target_col']
        
        print(f"Model loaded from {filepath}")
    
    def summary(self):
        if self.fitted_model:
            print(self.fitted_model.summary())
        else:
            print("No model built yet.")