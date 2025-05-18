# -*- coding: utf-8 -*-
"""
Created in 2025

@author: Quant Galore
"""

import pandas as pd
import numpy as np
import requests
import sqlalchemy
import mysql.connector

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"

engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@localhost:3306/my_database')

# =============================================================================
# Generation of monthly dates.
# =============================================================================

calendar = get_calendar("NYSE")
trading_dates = calendar.schedule(start_date = "2019-01-01", end_date = (datetime.today() - timedelta(days=1))).index.strftime("%Y-%m-%d").values

all_dates = pd.DataFrame({"date": pd.to_datetime(trading_dates)})
all_dates["year"] = all_dates["date"].dt.year
all_dates["month"] = all_dates["date"].dt.month

start_of_the_months = all_dates.drop_duplicates(subset = ["year", "month"], keep = "first").copy()
start_of_the_months["date"] = start_of_the_months["date"].dt.strftime("%Y-%m-%d")

monthly_dates = start_of_the_months["date"].drop_duplicates().values

# =============================================================================
# Retrieval of optionable stocks
# =============================================================================

weekly_ticker_data_list = []
times = []

# date = monthly_dates[0]
for date in monthly_dates:
    
    pit_data_list = []
    pit_url_list = []
    
    pit_request_0 = requests.get(f"https://api.polygon.io/v3/reference/tickers?date={date}&type=CS&market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={polygon_api_key}").json()
    pit_next_url = pit_request_0["next_url"]
    
    pit_data = pd.json_normalize(pit_request_0["results"])
    
    pit_url_list.append(pit_next_url)
    pit_data_list.append(pit_data)
    
    for pit_iteration in range(0, 10):
        
        try:
            
            pit_request_n = requests.get(f"{pit_url_list[-1]}&apiKey={polygon_api_key}").json()
            pit_data_n = pd.json_normalize(pit_request_n["results"])
            pit_data_list.append(pit_data_n)
            
            pit_request_n_next_url = pit_request_n["next_url"]
            pit_url_list.append(pit_request_n_next_url)
            
        except Exception as error:
            print(error)
            break
    
    tickers_on_that_date = pd.concat(pit_data_list)
    
    grouped_ticker_data_request = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_api_key}").json()["results"])
    grouped_ticker_data = grouped_ticker_data_request.copy().rename(columns={"T":"ticker"}) 
    
    grouped_ticker_data["date"] = pd.to_datetime(grouped_ticker_data["t"], unit = "ms", utc = True).dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
    
    grouped_ticker_data["notional_volume"] = grouped_ticker_data["vw"] * grouped_ticker_data["v"]
    
    grouped_valid_tickers = pd.merge(left = grouped_ticker_data[["date", "c", "notional_volume", "ticker"]], right = tickers_on_that_date[["ticker", "name"]], on="ticker")
    
    # at least $50mm in volume that day
    valid_monthly_universe = grouped_valid_tickers[grouped_valid_tickers["notional_volume"] >= 5e+7].copy().sort_values(by="notional_volume", ascending=False)
    
    tickers = valid_monthly_universe["ticker"].drop_duplicates().values
    
    # ticker = tickers[0]
    for ticker in tickers:
        
        start_time = datetime.now()
        
        try:
            
            all_contracts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&contract_type=call&as_of={date}&limit=1000&apiKey={polygon_api_key}").json()["results"])    
            all_expiration_dates = np.sort(all_contracts["expiration_date"].drop_duplicates().values)
            
            exp_date_data = pd.DataFrame({"date": all_expiration_dates})
            
            if len(exp_date_data) < 5:
                continue
            
            exp_date_data["date"] = pd.to_datetime(exp_date_data["date"]).dt.tz_localize("America/New_York")
            exp_date_data["days_between"] = exp_date_data["date"].diff().dt.days
    
            avg_days_between_exps = exp_date_data["days_between"][1:5].mean()
            
            if avg_days_between_exps >= 9:
                continue
            
            weekly_ticker_data = pd.DataFrame([{"date": date, "ticker": ticker, "avg_days_between": avg_days_between_exps}])
            
            weekly_ticker_data_list.append(weekly_ticker_data)
    
        except Exception as error:
            print(error)
            continue
        
        end_time = datetime.now()    
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(tickers==ticker)[0][0]/len(tickers))*100,2)
        iterations_remaining = len(tickers) - np.where(tickers==ticker)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
        
# =============================================================================
# Final data storage.       
# =============================================================================

full_weekly_ticker_data = pd.concat(weekly_ticker_data_list).drop_duplicates(subset=["date", "ticker"], keep = "last")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@localhost:3306/my_database')

full_weekly_ticker_data.to_sql("historical_liquid_tickers_polygon", con = engine, if_exists = "append")