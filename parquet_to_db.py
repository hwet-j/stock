import os
import pandas as pd
import psycopg2
import argparse
from datetime import datetime

# PostgreSQL 연결 정보
DB_CONFIG = {
    "dbname": "hwechang",
    "user": "hwechang",
    "password": "hwechang",
    "host": "10.0.1.160",
    "port": "5432"
}

LOG_FILE_PATH = "/home/hwechang_jeong/stock/exe/parquet_files.log"

def create_partition_table(conn, table_name):
    """파티션 테이블이 없으면 생성"""
    try:
        cur = conn.cursor()
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            dividends NUMERIC,
            stock_splits NUMERIC,
            PRIMARY KEY (date, ticker)
        ) PARTITION BY RANGE (date);
        """
        cur.execute(query)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[Error] 파티션 테이블 생성 실패: {e}")

def create_partition(conn, table_name, year, month):
    """해당 월의 파티션이 없으면 생성"""
    partition_name = f"{table_name}_{year}_{month:02d}"
    try:
        cur = conn.cursor()
        query = f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF {table_name}
        FOR VALUES FROM ('{year}-{month:02d}-01') TO ('{year}-{month+1:02d}-01');
        """
        cur.execute(query)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[Error] 파티션 생성 실패: {e}")

def store_parquet_to_db(parquet_file):
    """Parquet 파일을 PostgreSQL에 저장"""
    start_time = datetime.now()

    if not os.path.exists(parquet_file):
        print(f"[Error] 파일이 존재하지 않음: {parquet_file}")
        return

    try:
        df = pd.read_parquet(parquet_file)

        df.columns = [col.lower() for col in df.columns]

        # 'date' 컬럼 확인
        if "date" not in df.columns:
            print(f"[Error] 'date' 컬럼이 없습니다: {parquet_file}")
            print(f"[DEBUG] Parquet 컬럼 목록: {df.columns.tolist()}")
            return

        df["date"] = pd.to_datetime(df["date"]).dt.date

        conn = psycopg2.connect(**DB_CONFIG)
        table_name = "stock_data"

        create_partition_table(conn, table_name)

        cur = conn.cursor()
        insert_query = f"""
        INSERT INTO {table_name} (date, ticker, open, high, low, close, volume, dividends, stock_splits)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, ticker) DO NOTHING;
        """

        for _, row in df.iterrows():
            year, month = row["date"].year, row["date"].month
            create_partition(conn, table_name, year, month)

            cur.execute(insert_query, (
                row["date"], row["ticker"],
                row["open"], row["high"], row["low"], row["close"],
                row["volume"], row["dividends"], row["stock splits"]
            ))

        conn.commit()
        cur.close()
        conn.close()

        end_time = datetime.now()
        print(f"[DB 저장 완료] {parquet_file} (소요 시간: {end_time - start_time})")

    except Exception as e:
        print(f"[Error] {parquet_file}: {e}")

def store_logged_parquet_to_db(log_file=LOG_FILE_PATH):
    """로그 파일의 모든 Parquet 파일을 PostgreSQL에 저장"""
    if not os.path.exists(log_file):
        print(f"[INFO] 로그 파일 없음: {log_file}")
        return

    with open(log_file, "r") as f:
        parquet_files = f.read().splitlines()

    if not parquet_files:
        print(f"[INFO] 로그 파일에 저장된 Parquet 파일 없음")
        return

    for parquet_file in parquet_files:
        store_parquet_to_db(parquet_file)

    # 로그 파일 삭제 (성공적으로 저장된 경우)
    os.remove(log_file)
    print(f"[INFO] 로그 파일 삭제됨: {log_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parquet 파일을 DB에 저장하는 프로그램")

    parser.add_argument("--parquet_file", type=str, help="저장할 단일 Parquet 파일 경로")
    parser.add_argument("--log_file", type=str, default=LOG_FILE_PATH, help="로그 파일 경로 (기본값: parquet_files.log)")

    args = parser.parse_args()

    if args.parquet_file:
        # 특정 Parquet 파일만 저장
        store_parquet_to_db(args.parquet_file)
    else:
        # 로그 파일에 기록된 Parquet 파일 전체 저장
        store_logged_parquet_to_db(args.log_file)
