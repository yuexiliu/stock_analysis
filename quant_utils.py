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

#回测版中长线入场点，用于测试
def midterm_entry_signal_backtrack(df, i):
    if i < 70:
        return False
    
    sub = df.iloc[:i+1].copy()
    
    latest = sub.iloc[-1]
    prev = sub.iloc[-2]
    
    trend = (
        latest['收盘'] > latest['MA60'] and
        latest['MA20'] > latest['MA60'] and
        sub['MA60'].iloc[-1] > sub['MA60'].iloc[-5] and
        sub['MA20'].iloc[-1] > sub['MA20'].iloc[-5]
    )
    
    recent_low = sub['最低'].iloc[-8:].min()
    pullback_ok = recent_low >= latest['MA20']
    
    recent_vol = sub['成交量'].iloc[-5:].mean()
    base_vol = sub['成交量'].iloc[-20:].mean()
    volume_contract = recent_vol < base_vol
    
    rebound = latest['收盘'] > prev['收盘']
    
    return trend and pullback_ok and volume_contract and rebound

#中长线触到20日线退出纪律
def backtest_single_stock(df):
    returns = []
    
    for i in range(70, len(df)-1):
        
        if midterm_entry_signal_backtrack(df, i):
            
            entry_price = df.iloc[i]['收盘']
            
            # ===== 数据安全检查 =====
            if not np.isfinite(entry_price) or entry_price <= 0:
                continue

            # ===== 开始往后找卖出点 =====
            for j in range(i+1, len(df)):
                
                close_price = df.iloc[j]['收盘']
                ma20_price = df.iloc[j]['MA20']
                
                # 安全检查
                if not np.isfinite(close_price) or not np.isfinite(ma20_price):
                    continue
                
                # 如果跌破 MA20 → 卖出
                if close_price < ma20_price:
                    
                    exit_price = close_price
                    ret = (exit_price - entry_price) / entry_price
                    returns.append(ret)
                    
                    break   # 找到卖点后退出这笔交易
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
    
def trend_stock(stock_name):
