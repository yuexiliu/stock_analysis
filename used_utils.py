
def build_full_history(stock_df, start_date="19901219", end_date=today):
    """
    抓取a股历史以来所有日线历史，失败报错
    """
    trade_dates = get_trade_dates(start_date=start_date, end_date=end_date)

    for trade_date in tqdm(trade_dates, desc="全历史抓取"):
        try:
            one_day_df = fetch_one_day_all_stocks_fast(stock_df, trade_date)
            update_one_day_store(one_day_df)
        except Exception as e:
            print(f"{trade_date} 失败: {e}")





