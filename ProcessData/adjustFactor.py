import os
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
output_dir = os.path.join(PROJECT_ROOT, "data", "processed")

# 读取配置文件
config_path = os.path.join(os.path.dirname(__file__), "factor_args.json")
with open(config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

# 股票代码
ts_codes = config["ts_code"]
if isinstance(ts_codes, str):
    ts_codes = [ts_codes]

# 日期范围
start_date = config["startdate"]
end_date = config["enddate"]

# 复权类型
adjust_type = config.get("adjust_type", "前复权")  # 默认前复权

# 数据目录
raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))

# 错误记录
error_records = []

adjust_types = ["前复权", "后复权"]

# 遍历每个股票代码
for ts_code in ts_codes:
    daily_file = None
    daily_basic_file = None
    adj_factor_file = None

    for fname in os.listdir(raw_dir):
        if ts_code in fname:
            if fname.endswith("_daily.csv") and "_daily_basic" not in fname:
                daily_file = os.path.join(raw_dir, fname)
            elif fname.endswith("_daily_basic.csv"):
                daily_basic_file = os.path.join(raw_dir, fname)
            elif fname.endswith("_adj_factor.csv"):
                adj_factor_file = os.path.join(raw_dir, fname)

    if not all([daily_file, daily_basic_file, adj_factor_file]):
        print(f"[WARN] {ts_code} 缺少文件，跳过")
        continue

    # 读取数据
    df_daily = pd.read_csv(daily_file, dtype={"trade_date": str})
    df_basic = pd.read_csv(daily_basic_file, dtype={"trade_date": str})
    df_adj = pd.read_csv(adj_factor_file, dtype={"trade_date": str})

    # 过滤日期范围
    df_daily = df_daily[(df_daily["trade_date"] >= start_date) & (df_daily["trade_date"] <= end_date)]
    df_basic = df_basic[(df_basic["trade_date"] >= start_date) & (df_basic["trade_date"] <= end_date)]
    df_adj = df_adj[(df_adj["trade_date"] >= start_date) & (df_adj["trade_date"] <= end_date)]

    # 合并 daily 和 daily_basic
    merged_df = pd.merge(df_daily, df_basic, on=["ts_code", "trade_date"], how="inner", suffixes=("", "_dup"))
    merged_df = merged_df[[col for col in merged_df.columns if not col.endswith("_dup")]]

    # 合并复权因子
    merged_df = pd.merge(merged_df, df_adj[["trade_date", "adj_factor"]], on="trade_date", how="left")

    # 缺失 adj_factor 记录
    missing_adj = merged_df[merged_df["adj_factor"].isna()]
    if not missing_adj.empty:
        for _, row in missing_adj.iterrows():
            error_records.append({"ts_code": ts_code, "trade_date": row["trade_date"]})

    for adjust_type in adjust_types:
        df_adj_price = merged_df.copy()
        if df_adj_price["adj_factor"].isna().all():
            # 全缺就不调整了
            pass
        else:
            if adjust_type == "前复权":
                latest_adj = df_adj_price["adj_factor"].dropna().iloc[-1]
                price_cols = ["open", "high", "low", "close", "pre_close"]
                for col in price_cols:
                    if col in df_adj_price.columns:
                        df_adj_price[col] = df_adj_price[col] * df_adj_price["adj_factor"] / latest_adj
                if "amount" in df_adj_price.columns:
                    df_adj_price["amount"] = df_adj_price["amount"] * df_adj_price["adj_factor"] / latest_adj
                if "vol" in df_adj_price.columns:
                    df_adj_price["vol"] = df_adj_price["vol"] * latest_adj / df_adj_price["adj_factor"]
                if "close" in df_adj_price.columns:
                    df_adj_price["adj_close"] = df_adj_price["close"]
            elif adjust_type == "后复权":
                first_adj = df_adj_price["adj_factor"].dropna().iloc[0]
                price_cols = ["open", "high", "low", "close", "pre_close"]
                for col in price_cols:
                    if col in df_adj_price.columns:
                        df_adj_price[col] = df_adj_price[col] * df_adj_price["adj_factor"] / first_adj
                if "amount" in df_adj_price.columns:
                    df_adj_price["amount"] = df_adj_price["amount"] * df_adj_price["adj_factor"] / first_adj
                if "vol" in df_adj_price.columns:
                    df_adj_price["vol"] = df_adj_price["vol"] * first_adj / df_adj_price["adj_factor"]
                if "close" in df_adj_price.columns:
                    df_adj_price["adj_close"] = df_adj_price["close"]

        fixed_cols = ["ts_code", "trade_date"]
        other_cols = [col for col in df_adj_price.columns if col not in fixed_cols]
        df_adj_price = df_adj_price[fixed_cols + other_cols]

        output_name = f"{ts_code}_{start_date}_{end_date}_daily_{adjust_type}_adjusted.csv"
        output_path = os.path.join(output_dir, output_name)
        df_adj_price.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"[OK] 保存 {output_path}")

# 保存错误记录
if error_records:
    error_df = pd.DataFrame(error_records)
    error_df.to_csv(os.path.join(os.path.dirname(__file__), "error.csv"), index=False, encoding="utf-8-sig")
    print("[WARN] 存在缺失 adj_factor 的记录，已保存到 error.csv")