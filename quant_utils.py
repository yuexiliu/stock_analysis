import akshare as ak
from datetime import datetime
import pandas as pd
from datetime import date
import tushare as ts
from pathlib import Path
import numpy as np

BY_DATE_DIR = Path("data/by_date")
BY_STOCK_DIR = Path("data/by_stock")

today = date.today().strftime("%Y%m%d")

ts.set_token("103ef331836d4a8aa5d7485665c8f7503b8632c23748f1ba985c5713")
pro = ts.pro_api("103ef331836d4a8aa5d7485665c8f7503b8632c23748f1ba985c5713")

#最低成交额（可设置）

MIN_AMT = 2e8

#中长线进场点；可随时修改纪律，扫描全市场的股票
def midterm_entry_signal(df):
    df = df.copy()
    
    # 计算均线
    df['MA20'] = df['收盘'].rolling(20).mean()
    df['MA60'] = df['收盘'].rolling(60).mean()
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    # ===== 趋势条件 =====
    trend = (
        latest['收盘'] > latest['MA60'] and
        latest['MA20'] > latest['MA60'] and
        df['MA60'].iloc[-1] > df['MA60'].iloc[-5] and
        df['MA20'].iloc[-1] > df['MA20'].iloc[-5]
    )
    
    # ===== 回调条件 =====
    recent_low = df['最低'].iloc[-8:].min()
    pullback_ok = recent_low >= latest['MA20']
    
    # ===== 缩量 =====
    recent_vol = df['成交量'].iloc[-5:].mean()
    base_vol = df['成交量'].iloc[-20:].mean()
    volume_contract = recent_vol < base_vol
    
    # ===== 再启动 =====
    rebound = latest['收盘'] > prev['收盘']
    
    conds = {
        "trend_ok": trend,
        "pullback_ok": pullback_ok,
        "volume_contract": volume_contract,
        "rebound": rebound
    }
    
    return conds

# 回测版中长线入场点，用于测试

def midterm_entry_signal_backtrack(df, i):
    if i < 70:
        return False

    close = df["close"]
    ma20 = df["MA20"]
    ma60 = df["MA60"]
    low = df["low"]
    vol = df["vol"]

    latest_close = close.iloc[i]
    prev_close = close.iloc[i - 1]
    latest_ma20 = ma20.iloc[i]
    latest_ma60 = ma60.iloc[i]

    trend = (
        latest_close > latest_ma60 and
        latest_ma20 > latest_ma60 and
        ma60.iloc[i] > ma60.iloc[i - 4] and
        ma20.iloc[i] > ma20.iloc[i - 4]
    )

    recent_low = low.iloc[i - 7:i + 1].min()
    pullback_ok = recent_low >= latest_ma20

    recent_vol = vol.iloc[i - 4:i + 1].mean()
    base_vol = vol.iloc[i - 19:i + 1].mean()
    volume_contract = recent_vol < base_vol

    rebound = latest_close > prev_close

    return trend and pullback_ok and volume_contract and rebound


# 中长线触到20日线退出纪律
def backtest_single_stock(df):
    returns = []

    for i in range(70, len(df)-1):
        if midterm_entry_signal_backtrack(df, i):
            entry_price = df.iloc[i]["close"]

            # 数据安全检查
            if not np.isfinite(entry_price) or entry_price <= 0:
                continue

            # 开始往后找卖出点
            for j in range(i+1, len(df)):
                close_price = df.iloc[j]["close"]
                ma20_price = df.iloc[j]["MA20"]

                # 安全检查
                if not np.isfinite(close_price) or not np.isfinite(ma20_price):
                    continue

                # 如果跌破 MA20 -> 卖出
                if close_price < ma20_price:
                    exit_price = close_price
                    ret = (exit_price - entry_price) / entry_price
                    returns.append(ret)
                    break  # 找到卖点后退出这笔交易

    return returns


def filter_stock(stock_name):
    stock_name["board"] = "其他"

    stock_name.loc[
    stock_name["code"].str.startswith(("60")),
    "board"
    ] = "沪主板"

    stock_name.loc[
    stock_name["code"].str.startswith(("00")),
    "board"
    ] = "深主板"

    stock_name.loc[
    stock_name["code"].str.startswith(("300","301","302")),
    "board"
    ] = "创业板"

    #科创板股票代码以688为始，存托凭证代码以689为始
    stock_name.loc[
    stock_name["code"].str.startswith(("688","689")),
    "board"
    ] = "科创板"

    stock_name.loc[
    stock_name["code"].str.startswith(("920")), 
    "board"] = "北交所"

    # 找出未识别股票
    unknown_stock = stock_name[stock_name["board"] == "其他"]

    if not unknown_stock.empty:
        print("警告：发现未识别板块的股票，请检查编码规则！")
        print("未识别股票列表如下：")
        print(unknown_stock[["code", "name"]].to_string(index=False))

    return stock_name

#主板股票
def filter_main_board(stock_name):
    df = stock_name.copy()
    df = df[df["board"].isin(["深主板", "沪主板"])]
    return df
    
#不要*ST和ST
def filter_main_board_no_st(stock_name):
    df = stock_name.copy()
    df = df[df["board"].isin(["深主板", "沪主板"])]
    df = df[~df["name"].str.contains("ST")]
    return df

def get_list_date_from_hist(stock):
    today = datetime.today().strftime("%Y%m%d")
    df = ak.stock_zh_a_hist(
        symbol=stock,
        start_date="19900101",
        end_date= today,
        adjust=""
    )
    if df.empty:
        return None

    return pd.to_datetime(df.iloc[0]['日期'])
    

def add_ts_code(stock_name):
    df = stock_name.copy()
    df["ts_code"] = df["code"].apply(
        lambda x: f"{x}.SH" if x.startswith("60") else f"{x}.SZ"
    )
    return df

def get_trade_dates(start_date="19901219", end_date= today):
    """
    用 AKShare 获取 A 股交易日历，返回 YYYYMMDD 字符串列表
    """
    cal = ak.tool_trade_date_hist_sina()
    cal["trade_date"] = pd.to_datetime(cal["trade_date"], errors="coerce")
    cal = cal.dropna(subset=["trade_date"])

    start_dt = pd.to_datetime(start_date, format="%Y%m%d")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d")

    cal = cal[(cal["trade_date"] >= start_dt) & (cal["trade_date"] <= end_dt)].copy()
    cal = cal.sort_values("trade_date").reset_index(drop=True)

    return cal["trade_date"].dt.strftime("%Y%m%d").tolist()

#update by date, add date file directly and then append to stock files

def fetch_one_day_all_stocks_fast(stock_df, trade_date):
    """
    一次性抓取某一天全市场 daily + adj_factor，再和 stock_df 合并
    stock_df 需要包含: code, name, board, ts_code
    trade_date 格式: YYYYMMDD
    返回df
    """
    daily_df = pro.daily(trade_date=trade_date)
    adj_df = pro.adj_factor(trade_date=trade_date)

    if daily_df is None or daily_df.empty:
        return pd.DataFrame()
    if adj_df is None or adj_df.empty:
        return pd.DataFrame()

    df = daily_df.merge(
        adj_df[["ts_code", "trade_date", "adj_factor"]],
        on=["ts_code", "trade_date"],
        how="left",
    )

    stock_info = stock_df.copy()
    stock_info["code"] = stock_info["code"].astype(str).str.zfill(6)

    df = df.merge(
        stock_info[["code", "name", "board", "ts_code"]],
        on="ts_code",
        how="inner",
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["trade_date"])
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    return df


def update_one_day_store(one_day_df):
    if one_day_df is None or one_day_df.empty:
        print("当天没有数据可更新")
        return

    trade_date_str = one_day_df["trade_date"].iloc[0].strftime("%Y-%m-%d")

    # 按日期存
    date_path = BY_DATE_DIR / f"{trade_date_str}.parquet"
    one_day_df.to_parquet(date_path, index=False)

    # 按股票增量存
    for ts_code, sub_df in one_day_df.groupby("ts_code"):
        stock_path = BY_STOCK_DIR / f"{ts_code}.parquet"

        if stock_path.exists():
            old_df = pd.read_parquet(stock_path)
            merged = pd.concat([old_df, sub_df], ignore_index=True)
            merged = (
                merged.sort_values("trade_date")
                .drop_duplicates(subset=["trade_date"], keep="last")
                .reset_index(drop=True)
            )
        else:
            merged = (
                sub_df.sort_values("trade_date")
                .drop_duplicates(subset=["trade_date"], keep="last")
                .reset_index(drop=True)
            )

        merged.to_parquet(stock_path, index=False)

def run_daily_update_fast(stock_df, trade_date):
    one_day_df = fetch_one_day_all_stocks_fast(stock_df, trade_date)
    update_one_day_store(one_day_df)
    return one_day_df




#update by stock, add stock file directly and then append to date files

def fetch_one_stock_full_history(ts_code, stock_meta_df):
    daily_df = pro.daily(ts_code=ts_code)
    adj_df = pro.adj_factor(ts_code=ts_code)

    if daily_df is None or daily_df.empty:
        return pd.DataFrame()
    if adj_df is None or adj_df.empty:
        return pd.DataFrame()

    df = daily_df.merge(
        adj_df[["ts_code", "trade_date", "adj_factor"]],
        on=["ts_code", "trade_date"],
        how="left",
    )

    meta = stock_meta_df[stock_meta_df["ts_code"] == ts_code][["code", "name", "board", "ts_code"]].copy()

    df = df.merge(meta, on="ts_code", how="inner")

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["trade_date"])
    df = df.sort_values(["trade_date"]).reset_index(drop=True)
    return df


def update_stock_and_dates(stock_df):
    if stock_df is None or stock_df.empty:
        return

    ts_code = stock_df["ts_code"].iloc[0]

    # Update by_stock
    stock_path = BY_STOCK_DIR / f"{ts_code}.parquet"
    if stock_path.exists():
        old_df = pd.read_parquet(stock_path)
        merged_stock = pd.concat([old_df, stock_df], ignore_index=True)
        merged_stock = (
            merged_stock
            .sort_values("trade_date")
            .drop_duplicates(subset=["trade_date"], keep="last")
            .reset_index(drop=True)
        )
    else:
        merged_stock = (
            stock_df
            .sort_values("trade_date")
            .drop_duplicates(subset=["trade_date"], keep="last")
            .reset_index(drop=True)
        )

    merged_stock.to_parquet(stock_path, index=False)

    # Update by_date
    for trade_date, sub_df in stock_df.groupby("trade_date"):
        date_str = trade_date.strftime("%Y-%m-%d")
        date_path = BY_DATE_DIR / f"{date_str}.parquet"

        if date_path.exists():
            old_date_df = pd.read_parquet(date_path)
            merged_date = pd.concat([old_date_df, sub_df], ignore_index=True)
            merged_date = (
                merged_date
                .sort_values(["ts_code", "trade_date"])
                .drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
                .reset_index(drop=True)
            )
        else:
            merged_date = (
                sub_df
                .sort_values(["ts_code", "trade_date"])
                .drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
                .reset_index(drop=True)
            )

        merged_date.to_parquet(date_path, index=False)