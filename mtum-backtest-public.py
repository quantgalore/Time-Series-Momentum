# -*- coding: utf-8 -*-
"""
Created in 2025

@author: Quant Galore
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import gspread
import sqlalchemy
import mysql.connector

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"

# =============================================================================
# Date Management - Rebalancing Every Month
# =============================================================================

calendar = get_calendar("NYSE")
trading_dates = calendar.schedule(start_date = "2019-01-01", end_date = (datetime.today() - timedelta(days=1))).index.strftime("%Y-%m-%d").values

# =============================================================================
# Base Point-in-Time Universe + Benchmark Generation
# =============================================================================

engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@localhost:3306/my_database')
universe = pd.read_sql("historical_liquid_tickers_polygon", con = engine).drop_duplicates(subset=["date", "ticker"])

benchmark_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2017-01-01/{trading_dates[-1]}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
benchmark_data.index = pd.to_datetime(benchmark_data.index, unit="ms", utc=True).tz_convert("America/New_York")
benchmark_data["date"] = benchmark_data.index.strftime("%Y-%m-%d")

# =============================================================================
# Backtesting - Feature Generation + Forward Return Calc 
# =============================================================================

full_data_list = []
top_decile_list = []
bot_decile_list = []

times = []

months = universe["date"].drop_duplicates().values

# month = months[:-1][0]
for month in months[:-1]:
    
    try:
        
        start_time = datetime.now()
        
        point_in_time_dates = np.sort(universe[universe["date"] <= month]["date"].drop_duplicates().values)
        point_in_time_date = point_in_time_dates[-1]
        
        point_in_time_universe = universe[universe["date"] == point_in_time_date].drop_duplicates(subset=["ticker"], keep = "last")
        
        tickers = point_in_time_universe["ticker"].drop_duplicates().values
        
        start_date = (pd.to_datetime(month) - timedelta(days = 365+60)).strftime("%Y-%m-%d")
        end_date = month
        
        next_month_date = np.sort(months[months > month])[0]
        
        last_month_date = (pd.to_datetime(month) - timedelta(days = 30)).strftime("%Y-%m-%d")
        
        monthly_ticker_list = []
            
        # ticker = tickers[np.random.randint(0, len(tickers))]
        for ticker in tickers:
            
            try:
                
                underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{next_month_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
                underlying_data.index = pd.to_datetime(underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
                underlying_data["date"] = underlying_data.index.strftime("%Y-%m-%d")
                
                underlying_data["year"] = underlying_data.index.year
                underlying_data["month"] = underlying_data.index.month
                
                twelve_minus_one_data = underlying_data[underlying_data["date"] < last_month_date].copy().tail(252)
                
                twelve_minus_one_data["year"] = twelve_minus_one_data.index.year
                twelve_minus_one_data["month"] = twelve_minus_one_data.index.month
                
                twelve_minus_one_return = round(((twelve_minus_one_data["c"].iloc[-1] - twelve_minus_one_data["c"].iloc[0]) / twelve_minus_one_data["c"].iloc[0]) * 100 , 2)
                
                benchmark_and_underlying = pd.merge(left=benchmark_data[["c", "date"]], right = twelve_minus_one_data[["c","date"]], on = "date")
                benchmark_and_underlying["benchmark_pct_change"] = round(benchmark_and_underlying["c_x"].pct_change() * 100, 2).fillna(0)
                benchmark_and_underlying["ticker_pct_change"] = round(benchmark_and_underlying["c_y"].pct_change() * 100, 2).fillna(0)
            
                covariance_matrix = np.cov(benchmark_and_underlying["ticker_pct_change"], benchmark_and_underlying["benchmark_pct_change"])
                covariance_ticker_benchmark = covariance_matrix[0, 1]
                variance_benchmark = np.var(benchmark_and_underlying["benchmark_pct_change"])
                beta = covariance_ticker_benchmark / variance_benchmark
                
                ticker_return_over_period = round(((benchmark_and_underlying["c_y"].iloc[-1] - benchmark_and_underlying["c_y"].iloc[0]) / benchmark_and_underlying["c_y"].iloc[0]) * 100, 2)    
                std_of_returns = benchmark_and_underlying["ticker_pct_change"].std() * np.sqrt(252)
                
                sharpe = ticker_return_over_period / std_of_returns
                
                theo_expected = beta * sharpe
                
                next_period_underlying_data = underlying_data[(underlying_data["date"] >= month) & (underlying_data["date"] <= next_month_date)].copy().sort_index()
                
                next_period_returns = round(((next_period_underlying_data["c"].iloc[-1] - next_period_underlying_data["c"].iloc[0]) / next_period_underlying_data["c"].iloc[0])*100, 2)
                
                ticker_data = pd.DataFrame([{"entry_date": month, "ticker": ticker, "beta": beta, "sharpe": sharpe, "12-1_return": ticker_return_over_period, "mom_score": theo_expected, "forward_returns": next_period_returns, "exit_date": next_month_date}])
                
                monthly_ticker_list.append(ticker_data)
                
            except Exception as error:
                print(error)
                continue
            
        full_period_ticker_data = pd.concat(monthly_ticker_list)
        
        top_decile = full_period_ticker_data.sort_values(by="mom_score", ascending = False).head(10)
        bot_decile = full_period_ticker_data.sort_values(by="mom_score", ascending = True).head(10)
        
        full_data_list.append(full_period_ticker_data)
        top_decile_list.append(top_decile)
        bot_decile_list.append(bot_decile)
        
        end_time = datetime.now()    
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(months==month)[0][0]/len(months))*100,2)
        iterations_remaining = len(months) - np.where(months==month)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
        
    except Exception as macro_error:
        print(macro_error)
        continue

full_dataset = pd.concat(full_data_list)

top_decile_dataset = pd.concat(top_decile_list)
bot_decile_dataset = pd.concat(bot_decile_list)

# =============================================================================
# Backtest
# =============================================================================

covered_dates = full_dataset["entry_date"].drop_duplicates().values

trade_list = []

# covered_date = covered_dates[0]
for covered_date in covered_dates:
    
    # The backtest is crude, so the arithmetic average forward return of the respective basket is used as the return.
    
    long_uni = top_decile_dataset[top_decile_dataset["entry_date"] == covered_date].copy()
    short_uni = bot_decile_dataset[bot_decile_dataset["entry_date"] == covered_date].copy()
    
    trade_data = pd.DataFrame([{"date": covered_date, "long": long_uni["forward_returns"].mean(), "short": short_uni["forward_returns"].mean()*-1}])
    trade_list.append(trade_data)

all_trades = pd.concat(trade_list)

all_trades["long_pnl"] = all_trades["long"].cumsum()
all_trades["short_pnl"] = all_trades["short"].cumsum()

all_trades["portfolio_pnl"] = all_trades["long_pnl"] + all_trades["short_pnl"]

plt.figure(figsize=(10, 6),dpi=200)
plt.xticks(rotation=45)
plt.suptitle(f"Gross Cumulative Performance")
plt.title(f"Monthly Rebalancing")
plt.plot(pd.to_datetime(all_trades["date"]), all_trades["long_pnl"])
plt.plot(pd.to_datetime(all_trades["date"]), all_trades["short_pnl"])
plt.plot(pd.to_datetime(all_trades["date"]), all_trades["portfolio_pnl"])
plt.legend(["Top Decile", "Bottom Decile", "Long-Short"])
plt.xlabel("Date")
plt.ylabel("Cumulative % Returns")
plt.show()
plt.close()