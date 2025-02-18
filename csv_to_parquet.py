import csv
import os
import pandas as pd
import argparse
import psycopg2
from datetime import datetime
import subprocess

DEFAULT_PARQUET_LOG_FILE = "/home/hwechang_jeong/stock/exe/parquet_files.log"


# PostgreSQL 연결 정보
DB_CONFIG = {
    "dbname": "hwechang",   # 사용할 PostgreSQL 데이터베이스 이름
    "user": "hwechang",     # 데이터베이스 접속 사용자명
    "password": "hwechang", # 데이터베이스 비밀번호
    "host": "10.0.1.160",   # 데이터베이스 서버의 IP 주소 또는 호스트네임
    "port": "5432"          # PostgreSQL 포트
}

DEFAULT_LOG_FILE_PATH = "/home/hwechang_jeong/stock/exe/csv_files.log"
DEFAULT_PARQUET_FOLDER = "parquet"  # 기본 Parquet 저장 폴더

# PostgreSQL 연결 정보
DB_CONFIG = {
    "dbname": "hwechang",
    "user": "hwechang",
    "password": "hwechang",
    "host": "10.0.1.160",
    "port": "5432"
}

def log_parquet_conversion_to_file(parquet_file):
    """
    변환된 Parquet 파일을 텍스트 파일 (parquet_files.log)에 기록
    """
    try:
        with open(DEFAULT_PARQUET_LOG_FILE, "a") as log_file:
            log_file.write(parquet_file + "\n")
    except Exception as e:
        print(f"[파일 로그 오류] {e}")

def log_to_db(step, log_type, ticker, message, from_date=None, to_date=None, start_time=None, end_time=None, result=None):
    """
    변환 과정의 로그를 stock_data_log 테이블에 저장

    :param step: 단계 (예: 'Parquet 변환', 'CSV 삭제')
    :param log_type: 로그 레벨 (INFO, ERROR)
    :param ticker: 종목 코드 (없으면 None)
    :param message: 상세 메시지
    :param from_date: 데이터 조회 시작 날짜 (없으면 None)
    :param to_date: 데이터 조회 종료 날짜 (없으면 None)
    :param start_time: 프로세스 시작 시간
    :param end_time: 프로세스 종료 시간
    :param result: 변환 결과 (성공 / 실패)
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        query = """
        INSERT INTO stock_data_log (step, log_type, ticker, message, from_date, to_date, start_time, end_time, result, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
        """
        cur.execute(query, (step, log_type, ticker, message, from_date, to_date, start_time, end_time, result))

        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB 로그 오류] {e}")
    finally:
        if conn:
            conn.close()

def create_stock_data_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        date DATE,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        dividends NUMERIC,
        stock_splits NUMERIC,
        ticker TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()



def convert_csv_to_parquet(csv_file, delete_csv=False):
    """
    지정된 CSV 파일을 Parquet 파일로 변환하여 기본 폴더(parquet/)에 저장

    :param csv_file: 변환할 CSV 파일 경로
    :param delete_csv: 변환 후 CSV 파일 삭제 여부
    """
    start_time = datetime.now()     # 변환 시작 시간 기록

    if not os.path.exists(csv_file):
        print(f"[Error] 파일이 존재하지 않음: {csv_file}")
        log_to_db("Parquet 변환", "ERROR", "ALL", "파일이 존재하지 않음", start_time=start_time, result="실패")
        return

    if not csv_file.endswith(".csv"):
        print(f"[Error] CSV 파일이 아님: {csv_file}")
        log_to_db("Parquet 변환", "ERROR", "ALL", "CSV 파일이 아님", start_time=start_time, result="실패")
        return

    # 원본 CSV 파일이 있는 폴더 구조 유지하여 Parquet 저장
    csv_folder, csv_filename = os.path.split(csv_file)
    relative_path = os.path.relpath(csv_folder, start="csv")  # "csv/" 폴더 기준 상대 경로
    parquet_folder = os.path.join(DEFAULT_PARQUET_FOLDER, relative_path)

    # Parquet 폴더 생성
    os.makedirs(parquet_folder, exist_ok=True)

    # 날짜 범위 추출 (파일명에서 from_date, to_date 파악)
    try:
        date_part = csv_filename.split("_")[-1].replace(".csv", "")
        from_date = to_date = datetime.strptime(date_part, "%Y-%m-%d").date()
    except ValueError:
        from_date = to_date = None

    # Parquet 파일 경로 설정
    parquet_file = os.path.join(parquet_folder, csv_filename.replace(".csv", ".parquet"))

    try:
        df = pd.read_csv(csv_file)
        df.to_parquet(parquet_file, engine="pyarrow", compression="snappy")

        end_time = datetime.now()
        print(f"[Parquet 변환 완료] {parquet_file}")
        log_to_db("Parquet 변환", "INFO", "ALL", f"변환 완료: {parquet_file}", from_date, to_date, start_time, end_time, "성공")
        log_parquet_conversion_to_file(parquet_file)

        if delete_csv:
            os.remove(csv_file)
            print(f"[CSV 삭제] {csv_file}")
            log_to_db("CSV 삭제", "INFO", "ALL", "CSV 파일 삭제 완료", from_date, to_date, end_time, end_time, "성공")

    except Exception as e:
        end_time = datetime.now()
        print(f"[Error] {csv_file}: {e}")
        log_to_db("Parquet 변환", "ERROR", "ALL", f"오류 발생: {e}", from_date, to_date, start_time, end_time, "실패")


def fix_csv_headers(input_file, output_file):
    """
    CSV 파일의 헤더에서 공백을 언더스코어(_)로 변경
    """
    with open(input_file, newline='', encoding='utf-8') as infile, open(output_file, "w", newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # (1) 헤더 수정: 공백을 언더스코어(_)로 변경
        header = next(reader)
        new_header = [col.replace(" ", "_") for col in header]  # 공백 → "_"
        writer.writerow(new_header)

        # (2) 데이터 그대로 복사
        for row in reader:
            writer.writerow(row)



def store_csv_to_db_with_pgfutter(csv_file, target_table="stock_data"):
    """
    pgfutter를 사용하여 CSV 데이터를 PostgreSQL에 저장한 후, 원하는 테이블로 데이터 이동.

    :param csv_file: 변환할 CSV 파일 경로
    :param target_table: 최종 저장할 PostgreSQL 테이블명
    """
    conn = None
    schema = "public"
    table_name = target_table + '_temp'

    fixed_csv_file = csv_file.replace(".csv", "_fixed.csv")
    fix_csv_headers(csv_file, fixed_csv_file)


    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 환경 변수 설정
        env = os.environ.copy()
        env["DB_NAME"] = DB_CONFIG["dbname"]
        env["DB_USER"] = DB_CONFIG["user"]
        env["DB_PASS"] = DB_CONFIG["password"]
        env["DB_HOST"] = DB_CONFIG["host"]
        env["DB_PORT"] = str(DB_CONFIG["port"])
        env["DB_SCHEMA"] = schema
        env["DB_TABLE"] = table_name

        # pgfutter 실행 명령어
        command = [
                "pgfutter", "csv", 
            fixed_csv_file  # 삽입할 CSV 파일
        ]

        try:
            subprocess.run(command, check=True, env=env)  # Python에서 실행
            print(f"[INFO] CSV 데이터를 '{schema}.{table_name}' 테이블에 저장 완료")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] pgfutter 실행 실패: {e}")
            return False

        # ✅ (2) 중복 데이터 제거 후, target_table로 이동
        cur.execute(f"""
            DELETE FROM {table_name} 
            WHERE (ticker, date::TEXT) IN (SELECT ticker, date::TEXT FROM {target_table});
        """)
        conn.commit()
        print(f"[INFO] 중복 데이터 제거 완료")

        cur.execute(f"""
            INSERT INTO {target_table} (date, open, high, low, close, volume, dividends, stock_splits, ticker)
            SELECT 
                date::DATE, 
                NULLIF(REPLACE(open, '\r', ''), '')::NUMERIC, 
                NULLIF(REPLACE(high, '\r', ''), '')::NUMERIC, 
                NULLIF(REPLACE(low, '\r', ''), '')::NUMERIC, 
                NULLIF(REPLACE(close, '\r', ''), '')::NUMERIC, 
                NULLIF(REPLACE(volume, '\r', ''), '')::NUMERIC, 
                NULLIF(REPLACE(dividends, '\r', ''), '')::NUMERIC, 
                NULLIF(REPLACE(stock_splits, '\r', ''), '')::NUMERIC, 
                REPLACE(ticker, '\r', '')
            FROM {table_name};
        """)
        conn.commit()

        print(f"[INFO] 데이터 `{target_table}`로 이동 완료")

        # ✅ (3) 원본 테이블 삭제
        cur.execute(f"DROP TABLE {table_name};")
        conn.commit()
        print(f"[INFO] 자동 생성된 테이블 `{table_name}` 삭제 완료")

        return True

    except subprocess.CalledProcessError as e:
        print(f"[Error] pgfutter 실행 실패: {e}")
        return False

    except Exception as e:
        print(f"[Error] 데이터베이스 작업 중 오류 발생: {e}")
        return False

    finally:
        if conn:
            conn.close()


def convert_logged_csv_to_parquet(log_file=DEFAULT_LOG_FILE_PATH, delete_csv=False):
    """
    로그 파일에서 변환할 CSV 목록을 읽어 Parquet 변환
    """
    if not os.path.exists(log_file):
        print("[INFO] 변환할 CSV 파일 없음")
        return

    with open(log_file, "r") as f:
        csv_files = f.read().splitlines()

    if not csv_files:
        print("[INFO] 변환할 CSV 파일 없음")
        return

    for csv_file in csv_files:
        convert_csv_to_parquet(csv_file)
        store_csv_to_db_with_pgfutter(csv_file)

    # 변환 완료 후 로그 파일 초기화
    os.remove(log_file)

def convert_all_csv_to_parquet(root_folder="csv", delete_csv=False):
    """
    지정된 폴더 내의 모든 CSV 파일을 찾아 Parquet 파일로 변환하여 기본 폴더(parquet/)에 저장

    :param root_folder: CSV 파일이 저장된 루트 디렉토리
    :param delete_csv: 변환 후 CSV 파일 삭제 여부
    """
    for root, _, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                convert_csv_to_parquet(csv_path, delete_csv=delete_csv)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV 파일을 Parquet으로 변환하는 프로그램")

    parser.add_argument("--csv_file", type=str, help="변환할 단일 CSV 파일 경로")
    parser.add_argument("--folder", type=str, help="CSV 파일이 저장된 폴더")
    parser.add_argument("--log_file", type=str, default=DEFAULT_LOG_FILE_PATH, help="CSV 파일 로그 파일 경로")

    args = parser.parse_args()

    create_stock_data_table()

    if args.csv_file:
        # 특정 CSV 파일 변환
        convert_csv_to_parquet(args.csv_file)
    elif args.folder:
        # 특정 폴더의 모든 CSV 변환
        convert_all_csv_to_parquet(root_folder=args.folder)
    else:
        # 로그 파일 기반으로 가장 최근 변환된 파일만 처리
        convert_logged_csv_to_parquet(log_file=args.log_file)


"""
# 가장 마지막 실행으로 생성된 csv파일 변환
python csv_to_parquet.py
# 특정 csv파일 하나 변환
python csv_to_parquet.py --csv_file "csv/2024/02/stock_data_2024-02-06.csv"
# 폴더를 지정해 폴더내 csv 변환
python csv_to_parquet.py --folder "csv/2024/02"
# csv 파일 삭제 추가
python csv_to_parquet.py --folder "csv/2024/02" --delete_csv

"""
