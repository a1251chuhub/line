import requests
from datetime import datetime, timedelta
import mysql.connector
import time
import os
from pathlib import Path
from dotenv import load_dotenv

# 載入.env檔案（上一層目錄）
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# LINE Channel Access Token（從.env讀取）
ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")

# MySQL連線設定（從.env讀取）
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}
def fetch_follower_insight(date_str):
    url = f'https://api.line.me/v2/bot/insight/followers?date={date_str}'
    response = requests.get(url, headers=headers)
    return response.json() if response.status_code == 200 else None

def get_last_db_data(before_date):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    sql = """
        SELECT insight_date, total_followers, total_blocks
        FROM line_follower_insight
        WHERE insight_date < %s
        ORDER BY insight_date DESC
        LIMIT 1
    """
    cursor.execute(sql, (before_date,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row

def check_date_exists(date_str):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    sql = "SELECT 1 FROM line_follower_insight WHERE insight_date = %s LIMIT 1"
    cursor.execute(sql, (date_str,))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists

def insert_insight(data):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    sql = '''
        INSERT INTO line_follower_insight
        (insight_date, total_followers, reachable_users, total_blocks, new_followers, new_blocks, net_gain)
        VALUES (%(insight_date)s, %(total_followers)s, %(reachable_users)s, %(total_blocks)s, %(new_followers)s, %(new_blocks)s, %(net_gain)s)
    '''
    cursor.execute(sql, data)
    conn.commit()
    cursor.close()
    conn.close()

def main():
    # 自動取得昨天往前七天
    today = datetime.now().date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    print(f"自動取得 {start_date} ~ {end_date} 的LINE粉絲資料...")

    # 取得前一天資料
    prev_db = get_last_db_data(start_date.strftime('%Y-%m-%d'))
    prev_followers = prev_db['total_followers'] if prev_db else None
    prev_blocks = prev_db['total_blocks'] if prev_db else None

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        db_date_str = current_date.strftime('%Y-%m-%d')
        print(f"\n📅 日期：{current_date}")

        # 若已存在則跳過
        if check_date_exists(db_date_str):
            print("  ⚠️ 此日期已存在於資料庫，略過寫入。")
            # 仍需更新 prev_followers/blocks 以便正確計算下一天
            follower_data = fetch_follower_insight(date_str)
            if follower_data and follower_data.get("status") == "ready":
                prev_followers = follower_data.get('followers')
                prev_blocks = follower_data.get('blocks')
            current_date += timedelta(days=1)
            time.sleep(1)
            continue

        # 若無法取得資料則重試直到成功
        while True:
            follower_data = fetch_follower_insight(date_str)
            if follower_data and follower_data.get("status") == "ready":
                break
            print("  ⚠️ 無法取得粉絲資料，1秒後重試...")
            time.sleep(1)

        total_follower = follower_data.get('followers')
        total_blocks = follower_data.get('blocks')
        reachable_users = follower_data.get('targetedReaches')

        if prev_followers is not None and prev_blocks is not None:
            new_followers = total_follower - prev_followers
            new_blocks = total_blocks - prev_blocks
            net_gain = new_followers - new_blocks
        else:
            new_followers = None
            new_blocks = None
            net_gain = None

        print(f"  👥 總好友數(total_follower)：{total_follower}")
        print(f"  🚫 總封鎖數(total_blocks)：{total_blocks}")
        print(f"  📣 可傳送好友數(reachable_users)：{reachable_users}")
        print(f"  ➕ 新增好友數(new_followers)：{new_followers if new_followers is not None else '--（起始日）'}")
        print(f"  ➖ 新增封鎖數(new_blocks)：{new_blocks if new_blocks is not None else '--（起始日）'}")
        print(f"  🔄 淨增減數(net_gain)：{net_gain if net_gain is not None else '--（起始日）'}")

        # 寫入資料庫
        insert_insight({
            'insight_date': db_date_str,
            'total_followers': total_follower,
            'reachable_users': reachable_users,
            'total_blocks': total_blocks,
            'new_followers': new_followers,
            'new_blocks': new_blocks,
            'net_gain': net_gain
        })
        prev_followers = total_follower
        prev_blocks = total_blocks
        current_date += timedelta(days=1)
        time.sleep(1)

if __name__ == "__main__":
    main()
