import yfinance as yf                       # Yahoo Finance에서 주식 데이터를 가져오는 라이브러리
import pandas as pd                         # 데이터프레임을 다루기 위한 라이브러리
import psycopg2                             # PostgreSQL과 연결하여 데이터베이스 작업을 수행하는 라이브러리
import argparse                             # 커맨드라인에서 인자를 받을 수 있도록 도와주는 라이브러리
import os                                   # 파일 및 디렉터리 조작을 위한 기본 라이브러리
from datetime import datetime, timedelta    # 날짜 및 시간 관련 작업을 위한 라이브러리
import pandas_market_calendars as mcal      # 주식 시장의 휴장일을 확인할 수 있는 라이브러리

# PostgreSQL 연결 정보
DB_CONFIG = {
    "dbname": "hwechang",   # 사용할 PostgreSQL 데이터베이스 이름
    "user": "hwechang",     # 데이터베이스 접속 사용자명
    "password": "hwechang", # 데이터베이스 비밀번호
    "host": "10.0.1.160",   # 데이터베이스 서버의 IP 주소 또는 호스트네임
    "port": "5432"          # PostgreSQL 포트
}


# 기본 종목 리스트
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "CB", "JPM", "JNJ", "V", "WMT",
    "PG", "NVDA", "HD", "UNH", "DIS", "PYPL", "BAC", "VZ", "NFLX", "KO",
    "PEP", "INTC", "CSCO", "MRK", "XOM", "T", "PFE", "ABBV", "CRM", "MCD",
    "WFC", "ACN", "DHR", "TXN", "LLY", "ORCL", "NKE", "LIN", "MDT", "AMD",
    "COST", "HON", "UPS", "TMO", "C", "LMT", "GS", "NOW", "ADP", "SBUX",
    "AMGN", "CAT", "ISRG", "MS", "DE", "CVS", "BMY", "LOW", "ABT", "CI",
    "EQIX", "SCHW", "BKNG", "CME", "CHTR", "PLD", "SQ", "SPGI", "MMC", "EL",
    "BLK", "ADI", "REGN", "CB", "BSX", "ZTS", "ECL", "AXP", "HCA", "KMB",
    "SNPS", "PGR", "APD", "CDNS", "AON", "MCO", "MELI", "KLAC", "IDXX", "ROP",
    "TRV", "CRWD", "GPN", "VRTX", "FDX", "AZO", "BIIB", "ORLY", "WBA", "DXCM",
    "005930.KS", "000660.KS", "035420.KS", "207940.KS", "005380.KS",
    "051910.KS", "035720.KS", "068270.KS", "028260.KS", "006400.KS",
    "000270.KS", "012330.KS", "055550.KS", "105560.KS", "323410.KS",
    "086790.KS", "033780.KS", "009150.KS", "034220.KS", "011170.KS",
    "018260.KS", "032640.KS", "024110.KS", "011200.KS", "000810.KS",
    "138930.KS", "088980.KS", "010140.KS", "005490.KS", "008770.KS",
    "002790.KS", "003490.KS", "002380.KS", "003670.KS", "021240.KS",
    "298020.KS", "000100.KS", "097950.KS", "010620.KS", "271560.KS",
    "000120.KS", "010130.KS", "004020.KS", "023530.KS", "069960.KS",
    "011780.KS", "004370.KS", "005300.KS", "011090.KS", "000880.KS",
    "263800.KQ", "108320.KQ", "041510.KQ", "035900.KQ", "122990.KQ",
    "089970.KQ", "290660.KQ", "232140.KQ", "226330.KQ", "039030.KQ",
    "200470.KQ", "095700.KQ", "078600.KQ", "064290.KQ", "054920.KQ",
    "025900.KQ", "085670.KQ", "290520.KQ", "143240.KQ", "214320.KQ",
    "059100.KQ", "052790.KQ", "121800.KQ", "080220.KQ", "251970.KQ",
    "035760.KQ", "263750.KQ", "293490.KQ", "278280.KQ", "068760.KQ",
    "298540.KQ", "047810.KQ", "100130.KQ", "030200.KQ", "196170.KQ",
    "138040.KQ", "131370.KQ", "025980.KQ", "096530.KQ", "278650.KQ",
    "054180.KQ", "067160.KQ", "032190.KQ", "121600.KQ", "073490.KQ",
    "214260.KQ", "278990.KQ", "046210.KQ", "178320.KQ", "192080.KQ"
]

def create_log_tables_if_not_exists():
    """ PostgreSQL에 테이블 및 인덱스가 없을 경우 생성 """
    try:
        conn = psycopg2.connect(**DB_CONFIG)    # DB 연결
        cur = conn.cursor()                     # 커서 생성
        
        # 로그 테이블 생성 쿼리
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

        # 인덱스 추가 (성능 최적화를 위해)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_data_log_step ON stock_data_log(step);
            CREATE INDEX IF NOT EXISTS idx_stock_data_log_log_type ON stock_data_log(log_type);
            CREATE INDEX IF NOT EXISTS idx_stock_data_log_ticker ON stock_data_log(ticker);
        """)
        
        conn.commit()   # 변경 사항 저장
        cur.close()     # 커서 닫기
        conn.close()    # DB 연결 종료
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
        print(f"[ERROR] 로그 입력 실패: {e}")  # ← 에러 메시지 출력 추가
        # DB 저장 실패 시 log_backup.txt에 백업
        with open("log_backup.txt", "a") as log_file:
            log_file.write(f"{log_msg} | [로그 저장 실패] {e}\n")


def is_market_closed(date):
    """주어진 날짜가 미국 증시 휴장일인지 확인 (공식 휴장일 + 주말)"""
    nyse = mcal.get_calendar("NYSE")        # 뉴욕 증권거래소(NYSE) 캘린더 가져오기
    holidays = nyse.holidays().holidays     # 휴장일 목록

    # 주말 여부 확인 (토요일: 5, 일요일: 6)
    is_weekend = date.weekday() in [5, 6]

    # 휴장일 또는 주말이면 True 반환
    return date in holidays or is_weekend


def save_csv(data, from_date):
    """ CSV 파일을 저장할 폴더를 생성하고 데이터를 저장하는 함수 """
    try:
        # `from_date`를 문자열로 변환 후 `-`를 기준으로 연도(year), 월(month), 일(day) 분리
        year, month, _ = str(from_date).split("-")

        # 저장할 폴더 경로 생성 (예: `csv/2025/01/`)
        folder_path = os.path.join("csv", year, month)

        # `os.makedirs()`를 사용하여 폴더 생성 (`exist_ok=True`로 이미 존재하면 무시)
        os.makedirs(folder_path, exist_ok=True)

        # CSV 파일명 생성 (예: `stock_data_2025-01-14.csv`)
        file_name = f"stock_data_{from_date}.csv"

        # 폴더 경로와 파일명을 결합하여 최종 저장 경로 설정
        file_path = os.path.join(folder_path, file_name)

        # `Capital Gains` 컬럼이 있으면 제거 (대소문자 정확히 맞춰야 함)
        data = data.drop(columns=["Capital Gains"], errors="ignore")

        # 데이터프레임을 CSV 파일로 저장 (index=False로 인덱스는 저장하지 않음)
        data.to_csv(file_path, index=False)

        # CSV 파일 경로를 로그 파일(`csv_files.log`)에 저장 (Parquet 변환을 위해)
        log_file_path = "/home/hwechang_jeong/stock/exe/csv_files.log"
        with open(log_file_path, "a") as log_file:
            log_file.write(file_path + "\n")  # 파일 경로를 한 줄씩 추가 기록

        return file_path  # 저장된 파일 경로 반환
    except Exception as e:
        print(f"[ERROR] CSV 저장 실패: {e}")  # 예외 발생 시 오류 메시지 출력
        return None  # 실패 시 None 반환


def fetch_stock_data(tickers, from_date, to_date):
    """ 주식 데이터를 가져오고 CSV 및 DB에 저장 """
    start_time = datetime.now()  # 데이터 수집 시작 시간 기록
    log_to_db("시작", "INFO", "ALL", "데이터 수집 프로세스 시작", from_date, to_date, start_time=start_time, end_time=start_time,
              result="진행 중")

    current_date = datetime.strptime(from_date, "%Y-%m-%d")

    data_found = False  # 🔥 최소 1개라도 데이터를 저장했는지 확인하는 플래그

    while current_date <= datetime.strptime(to_date, "%Y-%m-%d"):
        check_date = current_date.date()
        extract_start_time = datetime.now()  # 데이터 수집 시작 시간
        print(f"[날짜 확인] {check_date} 데이터 수집 시작")

        if is_market_closed(check_date):
            log_to_db("휴장", "INFO", "ALL", f"{check_date} 휴장일(주말포함)", check_date, check_date,
                      start_time=extract_start_time, end_time=datetime.now(), result="휴장")
        else:
            all_data = []  # 수집된 데이터를 저장할 리스트

            for ticker in tickers:
                try:
                    stock = yf.Ticker(ticker)
                    stock_data = stock.history(start=str(check_date), end=str(check_date + timedelta(days=1)))

                    if stock_data.empty:
                        log_to_db("추출", "ERROR", ticker, f"데이터 없음", check_date, check_date,
                                  start_time=extract_start_time, end_time=datetime.now(), result="실패")
                        continue

                    stock_data = stock_data.reset_index()
                    stock_data["Ticker"] = ticker
                    all_data.append(stock_data)

                    log_to_db("추출", "INFO", ticker, f"데이터 가져오기 완료", check_date, check_date,
                              start_time=extract_start_time, end_time=datetime.now(), result="성공")

                except Exception as e:
                    log_to_db("추출", "ERROR", ticker, f"오류: {e}", check_date, check_date, start_time=extract_start_time,
                              end_time=datetime.now(), result="실패")

            if all_data:
                csv_start_time = datetime.now()
                combined_data = pd.concat(all_data, ignore_index=True)
                file_path = save_csv(combined_data, check_date)

                if file_path:
                    log_to_db("CSV 저장", "INFO", "ALL", f"파일 저장 완료: {file_path}", check_date, check_date,
                              start_time=csv_start_time, end_time=datetime.now(), result="성공")
                    data_found = True  # ✅ 최소 1개라도 데이터 저장이 되었음
                else:
                    log_to_db("CSV 저장", "ERROR", "ALL", "CSV 저장 실패", check_date, check_date, start_time=csv_start_time,
                              end_time=datetime.now(), result="실패")
            else:
                log_to_db("추출", "ERROR", "ALL", "수집된 데이터 없음", check_date, check_date, start_time=extract_start_time,
                          end_time=datetime.now(), result="실패")

        current_date += timedelta(days=1)

    end_time = datetime.now()

    # ✅ 전체 과정에서 단 한 개라도 데이터가 저장되었는지 확인
    if data_found:
        log_to_db("완료", "INFO", "ALL", "데이터 수집 프로세스 완료", from_date, to_date, start_time=start_time, end_time=end_time,
                  result="성공")
    else:
        log_to_db("완료", "ERROR", "ALL", "모든 날짜에 대해 데이터 없음", from_date, to_date, start_time=start_time,
                  end_time=end_time, result="실패")



def main():
    """ 실행 코드: 커맨드라인 인자 처리 및 데이터 수집 실행 """
    parser = argparse.ArgumentParser(description="주식 데이터를 가져와 저장하는 프로그램")

    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS, help="조회할 종목 코드 리스트 (예: AAPL MSFT TSLA)")
    # parser.add_argument("--from_date", type=str, default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    #                     help="시작 날짜 (예: 2024-01-01)")
    # parser.add_argument("--to_date", type=str, default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    #                     help="종료 날짜 (예: 2024-01-05)")

    parser.add_argument("--from_date", type=str, default='2025-01-14',
                        help="시작 날짜 (예: 2024-01-01)")
    parser.add_argument("--to_date", type=str, default='2025-01-22',
                        help="종료 날짜 (예: 2024-01-05)")
    args = parser.parse_args()

    create_log_tables_if_not_exists()  # 로그 테이블 생성
    fetch_stock_data(args.tickers, args.from_date, args.to_date)


if __name__ == "__main__":
    main()



"""
python3 stock_fetch.py --tickers AAPL MSFT TSLA --from_date 2024-02-01 --to_date 2024-02-07

"""