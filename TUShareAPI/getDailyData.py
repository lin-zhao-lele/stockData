import tushare as ts
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# call  daily_basic 和  daily 获取股票数据

# ===== 1. 配置 Tushare Token =====
load_dotenv()
token = os.getenv('TUSHARE_TOKEN')
ts.set_token(token)
pro = ts.pro_api()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
output_dir = os.path.join(PROJECT_ROOT, "output", "daily")


# 输入 CSV 文件路径
input_csv = os.path.join(PROJECT_ROOT, "output", "top20.csv")
os.makedirs(output_dir, exist_ok=True)

# 获取今天日期
today_str = datetime.today().strftime('%Y%m%d')

# 读取 CSV
df_input = pd.read_csv(input_csv, dtype=str)

# 检查必需列
if not {'ts_code', 'list_date'}.issubset(df_input.columns):
    raise ValueError("输入 CSV 必须包含 'ts_code' 和 'list_date' 列")

# 遍历每条记录
for _, row in df_input.iterrows():
    ts_code = row['ts_code']
    start_date = row['list_date']
    end_date = today_str

    # ====== daily_basic ======
    try:
        df_daily_basic = pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df_daily_basic.empty:
            file_name = f"{ts_code}_{start_date}_{end_date}_daily_basic.csv"
            df_daily_basic.to_csv(os.path.join(output_dir, file_name), index=False, encoding='utf-8-sig')
            print(f"[OK] 保存 {file_name}")
        else:
            print(f"[WARN] {ts_code} daily_basic 无数据")
    except Exception as e:
        print(f"[ERROR] daily_basic 获取 {ts_code} 失败: {e}")

    # ====== daily ======
    try:
        df_index_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df_index_daily.empty:
            file_name = f"{ts_code}_{start_date}_{end_date}_daily.csv"
            df_index_daily.to_csv(os.path.join(output_dir, file_name), index=False, encoding='utf-8-sig')
            print(f"[OK] 保存 {file_name}")
        else:
            print(f"[WARN] {ts_code} index_daily 无数据")
    except Exception as e:
        print(f"[ERROR] index_daily 获取 {ts_code} 失败: {e}")
