import os
import psycopg2
import argparse
import subprocess
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
PGFUTTER_PATH = "/usr/local/bin/pgfutter"  # pgfutter 실행 파일 경로


def log_to_db(step, log_type, message, start_time, end_time=None, result=None):
    """DB 로그 저장"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        query = """
        INSERT INTO stock_data_log (step, log_type, message, start_time, end_time, result, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW());
        """
        cur.execute(query, (step, log_type, message, start_time, end_time, result))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB 로그 오류] {e}")
    finally:
        if conn:
            conn.close()


def create_main_table(conn):
    """기본 테이블(stock_data) 생성"""
    try:
        cur = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS stock_data (
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
        print("[INFO] 기본 테이블(stock_data) 확인 완료")
    except Exception as e:
        print(f"[Error] 기본 테이블 생성 실패: {e}")


def create_temp_table(conn):
    """임시 테이블(stock_data_temp) 생성"""
    try:
        cur = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS stock_data_temp (
            date DATE NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            dividends NUMERIC,
            stock_splits NUMERIC
        );
        """
        cur.execute(query)
        conn.commit()
        cur.close()
        print("[INFO] 임시 테이블(stock_data_temp) 확인 완료")
    except Exception as e:
        print(f"[Error] 임시 테이블 생성 실패: {e}")


def load_data_with_pgfutter(parquet_file):
    """pgfutter를 이용해 Parquet 데이터를 임시 테이블에 저장"""
    start_time = datetime.now()

    if not os.path.exists(parquet_file):
        print(f"[ERROR] 파일 없음: {parquet_file}")
        log_to_db("pgfutter 실행", "ERROR", f"파일 없음: {parquet_file}", start_time, result="실패")
        return

    cmd = [
        PGFUTTER_PATH, "csv",
        "--dbname", DB_CONFIG["dbname"],
        "--host", DB_CONFIG["host"],
        "--port", DB_CONFIG["port"],
        "--user", DB_CONFIG["user"],
        "--pw", DB_CONFIG["password"],
        "--schema", "public",
        "--table", "stock_data_temp",
        parquet_file
    ]
    try:
        subprocess.run(cmd, check=True)
        print(f"[INFO] pgfutter로 {parquet_file} 임시 테이블에 적재 완료")
        log_to_db("pgfutter 실행", "INFO", f"{parquet_file} 적재 완료", start_time, result="성공")
    except subprocess.CalledProcessError as e:
        print(f"[Error] pgfutter 실행 실패: {e}")
        log_to_db("pgfutter 실행", "ERROR", str(e), start_time, result="실패")


def move_data_to_main_table(conn):
    """임시 테이블 데이터를 실제 테이블(stock_data)로 이동"""
    start_time = datetime.now()
    try:
        cur = conn.cursor()

        # ✅ 데이터 확인을 위해 먼저 SELECT 실행
        cur.execute("SELECT * FROM stock_data_temp LIMIT 5;")
        rows = cur.fetchall()
        if rows:
            print("[DEBUG] 임시 테이블 데이터 샘플:")
            for row in rows:
                print(row)
        else:
            print("[DEBUG] stock_data_temp에 데이터 없음")

        # ✅ 데이터를 이동하는 INSERT 실행
        query = """
        INSERT INTO stock_data (date, ticker, open, high, low, close, volume, dividends, stock_splits)
        SELECT date, ticker, open, high, low, close, volume, dividends, stock_splits FROM stock_data_temp
        ON CONFLICT (date, ticker) DO NOTHING;
        """
        cur.execute(query)
        conn.commit()
        cur.close()
        print("[INFO] 임시 테이블 데이터가 stock_data로 이동 완료")
        log_to_db("데이터 이동", "INFO", "데이터 이동 완료", start_time, result="성공")
    except Exception as e:
        print(f"[Error] 데이터 이동 실패: {e}")
        log_to_db("데이터 이동", "ERROR", str(e), start_time, result="실패")


def drop_temp_table(conn):
    """임시 테이블 삭제"""
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS stock_data_temp;")
        conn.commit()
        cur.close()
        print("[INFO] 임시 테이블 삭제 완료")
    except Exception as e:
        print(f"[Error] 임시 테이블 삭제 실패: {e}")


def process_parquet(parquet_file):
    """Parquet 데이터를 처리하는 전체 과정"""
    start_time = datetime.now()
    conn = psycopg2.connect(**DB_CONFIG)

    create_main_table(conn)
    create_temp_table(conn)
    load_data_with_pgfutter(parquet_file)
    move_data_to_main_table(conn)
    # drop_temp_table(conn)

    conn.close()
    end_time = datetime.now()
    print(f"[INFO] 전체 작업 완료 (소요 시간: {end_time - start_time})")
    log_to_db("전체 프로세스", "INFO", "작업 완료", start_time, end_time, "성공")


def process_log_file(log_file_path):
    """로그 파일을 읽고 Parquet 파일 목록을 처리"""
    if not os.path.exists(log_file_path):
        print(f"[ERROR] 로그 파일 없음: {log_file_path}")
        return

    with open(log_file_path, "r") as log_file:
        parquet_files = [line.strip().split(" - ")[-1] for line in log_file.readlines()]

    for parquet_file in parquet_files:
        print(f"[INFO] 처리 중: {parquet_file}")
        process_parquet(parquet_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parquet 파일을 DB에 저장하는 프로그램")
    parser.add_argument("--log_file", type=str, default=LOG_FILE_PATH, help="Parquet 파일 목록이 저장된 로그 파일 경로")
    args = parser.parse_args()

    process_log_file(args.log_file)
