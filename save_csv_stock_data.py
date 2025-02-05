import yfinance as yf
import pandas as pd
import psycopg2
import argparse
import os
from datetime import datetime, timedelta
import pandas_market_calendars as mcal

# PostgreSQL 연결 정보
DB_CONFIG = {
    "dbname": "hwechang",
    "user": "hwechang",
    "password": "hwechang",
    "host": "10.0.1.160",
    "port": "5432"
}

# 한국 이름 매핑
ticker_to_korean = {
    "AAPL": "애플", "MSFT": "마이크로소프트", "GOOGL": "구글", "AMZN": "아마존",
    "TSLA": "테슬라", "META": "메타 (구 페이스북)", "NVDA": "엔비디아", "BRK-B": "버크셔 해서웨이 B",
    "V": "비자", "JNJ": "존슨앤드존슨", "PG": "프록터 앤드 갬블", "UNH": "유나이티드헬스",
    "HD": "홈디포", "MA": "마스터카드", "PFE": "화이자", "KO": "코카콜라",
    "DIS": "디즈니", "PEP": "펩시코", "BAC": "뱅크오브아메리카", "XOM": "엑슨모빌",
}

# 기본 종목 리스트
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK-B",
    "V", "JNJ", "PG", "UNH", "HD", "MA", "PFE", "KO", "DIS", "PEP",
    "BAC", "XOM"
]

def create_log_tables_if_not_exists():
    """ PostgreSQL에 테이블 및 인덱스가 없을 경우 생성 """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data_log (
                log_id SERIAL PRIMARY KEY,
                step VARCHAR(50) NOT NULL,
                log_type VARCHAR(50) NOT NULL,
                ticker VARCHAR(20),
                message TEXT,
                from_date DATE,
                to_date DATE,
                start_time TIMESTAMP DEFAULT NOW(),
                end_time TIMESTAMP,
                result VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_data_log_step ON stock_data_log(step);
            CREATE INDEX IF NOT EXISTS idx_stock_data_log_log_type ON stock_data_log(log_type);
            CREATE INDEX IF NOT EXISTS idx_stock_data_log_ticker ON stock_data_log(ticker);
        """)

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[테이블 생성 오류] {e}")


def log_to_db(step, log_type, ticker, message, from_date, to_date, start_time=None, end_time=None, result=None):
    """ PostgreSQL에 로그 저장 """
    log_msg = f"[{step}] {log_type} | {ticker or '전체'} | {message} | {from_date} ~ {to_date}"
    print(log_msg)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stock_data_log (step, log_type, ticker, message, from_date, to_date, start_time, end_time, result, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (step, log_type, ticker, message, from_date, to_date, start_time, end_time, result)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[로그 저장 실패] {step} - {log_type}: {e}")


def is_market_closed(date):
    """주어진 날짜가 미국 증시 휴장일인지 확인 (공식 휴장일 + 주말)"""
    nyse = mcal.get_calendar("NYSE")
    holidays = nyse.holidays().holidays  # 공식 휴장일 목록

    # 주말 여부 확인 (토요일: 5, 일요일: 6)
    is_weekend = date.weekday() in [5, 6]

    # 휴장일 또는 주말이면 True 반환
    return date in holidays or is_weekend


def save_csv(data, from_date):
    """ CSV 파일을 저장할 폴더를 생성하고 저장 """
    try:
        year, month, _ = str(from_date).split("-")
        folder_path = os.path.join("data", year, month)
        os.makedirs(folder_path, exist_ok=True)

        file_name = f"stock_data_{from_date}.csv"
        file_path = os.path.join(folder_path, file_name)
        data.to_csv(file_path, index=False)

        return file_path
    except Exception as e:
        return print(e)


def fetch_stock_data(tickers, from_date, to_date):
    """ 주식 데이터를 가져오고 CSV 및 DB에 저장 """
    start_time = datetime.now()
    log_to_db("시작", "INFO", None, "데이터 수집 프로세스 시작", from_date, to_date, start_time=start_time, result="진행 중")

    current_date = datetime.strptime(from_date, "%Y-%m-%d")

    while current_date <= datetime.strptime(to_date, "%Y-%m-%d"):
        check_date = current_date.date()
        print(f"[날짜 확인] {check_date} 데이터 수집 시작")
        all_data = []
        if is_market_closed(check_date):
            log_to_db("휴장", "INFO", None, f"{check_date} 휴장일(주말포함)", from_date, to_date)
        else:
            for ticker in tickers:
                try:
                    # print(f"[데이터 수집] {ticker} | {check_date} 데이터 가져오는 중...")
                    stock_data = yf.Ticker(ticker).history(start=str(check_date),
                                                           end=str(check_date + timedelta(days=1)))

                    if stock_data.empty:
                        log_to_db("추출", "ERROR", ticker, f"{check_date} 데이터 없음", from_date, to_date, result="실패")
                        continue

                    stock_data = stock_data.reset_index()
                    stock_data["Ticker"] = ticker
                    stock_data["Korean Name"] = ticker_to_korean.get(ticker, "Unknown")

                    all_data.append(stock_data)

                    log_to_db("추출", "INFO", ticker, f"{check_date} 데이터 가져오기 완료", from_date, to_date, result="성공")

                except Exception as e:
                    log_to_db("추출", "ERROR", ticker, f"{check_date} 오류: {e}", from_date, to_date, result="실패")

            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                file_path = save_csv(combined_data, check_date)

                if file_path:
                    log_to_db("CSV 저장", "INFO", None, f"파일 저장 완료: {file_path}", from_date, to_date, result="성공")
                else:
                    log_to_db("CSV 저장", "ERROR", None, "CSV 저장 실패", from_date, to_date, result="실패")
            else:
                log_to_db("시작", "ERROR", None, "수집된 데이터 없음", from_date, to_date, result="실패")

        current_date += timedelta(days=1)

    end_time = datetime.now()
    log_to_db("완료", "INFO", None, "데이터 수집 프로세스 완료", from_date, to_date, start_time=start_time, end_time=end_time, result="성공")

def main():
    """ 실행 코드: 커맨드라인 인자 처리 및 데이터 수집 실행 """
    parser = argparse.ArgumentParser(description="주식 데이터를 가져와 저장하는 프로그램")

    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS, help="조회할 종목 코드 리스트 (예: AAPL MSFT TSLA)")
    parser.add_argument("--from_date", type=str, default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
                        help="시작 날짜 (예: 2024-01-01)")
    parser.add_argument("--to_date", type=str, default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
                        help="종료 날짜 (예: 2024-01-05)")

    args = parser.parse_args()

    create_log_tables_if_not_exists()  # 로그 테이블 생성
    fetch_stock_data(args.tickers, args.from_date, args.to_date)


if __name__ == "__main__":
    main()
