import yfinance as yf
import pandas as pd
import psycopg2
import argparse
import os
from datetime import datetime, timedelta

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
    "AAPL": "애플",
    "MSFT": "마이크로소프트",
    "GOOGL": "구글",
    "AMZN": "아마존",
    "TSLA": "테슬라",
    "META": "메타 (구 페이스북)",
    "NVDA": "엔비디아",
    "BRK-B": "버크셔 해서웨이 B",
    "V": "비자",
    "JNJ": "존슨앤드존슨",
    "PG": "프록터 앤드 갬블",
    "UNH": "유나이티드헬스",
    "HD": "홈디포",
    "MA": "마스터카드",
    "PFE": "화이자",
    "KO": "코카콜라",
    "DIS": "디즈니",
    "PEP": "펩시코",
    "BAC": "뱅크오브아메리카",
    "XOM": "엑슨모빌",
}

def log_to_db(step, status, message):
    """ PostgreSQL에 로그 저장 """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO stock_data_log (timestamp, step, status, message) VALUES (NOW(), %s, %s, %s)",
            (step, status, message)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[로그 저장 실패] {step} - {status}: {e}")

def fetch_stock_data(tickers, from_date, to_date):
    """ 주식 데이터를 가져오고 CSV 및 DB에 저장 """
    log_to_db("시작", "진행 중", "데이터 수집 프로세스 시작")

    all_data = []
    for ticker in tickers:
        try:
            log_to_db("추출", "진행 중", f"{ticker} 데이터 가져오는 중")
            stock_data = yf.Ticker(ticker).history(start=from_date, end=to_date)

            if stock_data.empty:
                log_to_db("추출", "실패", f"{ticker} - 휴장일 또는 데이터 없음")
                continue

            stock_data = stock_data.reset_index()
            stock_data["Ticker"] = ticker
            stock_data["Korean Name"] = ticker_to_korean.get(ticker, "Unknown")
            all_data.append(stock_data)
            log_to_db("추출", "성공", f"{ticker} 데이터 가져오기 완료")

        except Exception as e:
            log_to_db("추출", "실패", f"{ticker} 데이터 가져오기 오류: {e}")

    if all_data:
        try:
            combined_data = pd.concat(all_data, ignore_index=True)
            file_name = f"stock_data_{from_date}.csv"
            combined_data.to_csv(file_name, index=False)
            log_to_db("CSV 저장", "성공", f"파일 저장 완료: {file_name}")
        except Exception as e:
            log_to_db("CSV 저장", "실패", f"CSV 저장 오류: {e}")
            return

    else:
        log_to_db("시작", "실패", "수집된 데이터 없음")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="주식 데이터 수집기")
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.today().strftime("%Y-%m-%d")

    parser.add_argument("--from", type=str, default=yesterday, help="시작 날짜 (예: 2024-01-01)")
    parser.add_argument("--to", type=str, default=today, help="종료 날짜 (예: 2024-01-31)")
    args = vars(parser.parse_args())

    tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK-B",
        "V", "JNJ", "PG", "UNH", "HD", "MA", "PFE", "KO", "DIS", "PEP", "BAC", "XOM"
    ]
    fetch_stock_data(tickers, args["from"], args["to"])
