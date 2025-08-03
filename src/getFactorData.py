import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# ===== 1. 配置 Tushare Token =====
load_dotenv()
token = os.getenv('TUSHARE_TOKEN')
ts.set_token(token)
pro = ts.pro_api()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
output_dir = os.path.join(PROJECT_ROOT, "output", "daily")

# 输入文件路径
input_csv = os.path.join(PROJECT_ROOT, "output", "top20.csv")
os.makedirs(output_dir, exist_ok=True)

# 今天日期
today_str = datetime.today().strftime('%Y%m%d')

# 读取输入CSV
df_input = pd.read_csv(input_csv, dtype=str)
if not {'ts_code', 'list_date'}.issubset(df_input.columns):
    raise ValueError("输入CSV必须包含 'ts_code' 和 'list_date' 列")

# 遍历每只股票
for _, row in df_input.iterrows():
    ts_code = row['ts_code']
    start_date = row['list_date']
    end_date = today_str

    print(f"=== 处理 {ts_code} 从 {start_date} 到 {end_date} ===")

    # ===== 1. 获取复权因子 =====
    try:
        df_adj = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df_adj.empty:
            file_name = f"{ts_code}_{start_date}_{end_date}_adj_factor.csv"
            df_adj.to_csv(os.path.join(output_dir, file_name), index=False, encoding="utf-8-sig")
            print(f"[OK] 保存 {file_name}")
        else:
            print(f"[WARN] {ts_code} adj_factor 无数据")
    except Exception as e:
        print(f"[ERROR] adj_factor 获取 {ts_code} 失败: {e}")
