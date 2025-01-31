import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import argparse

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


def fetch_stock_data_with_dates(tickers, from_date, to_date):
    """
    주어진 날짜 범위로 주식 데이터를 가져와 파일명에 from_date 정보를 포함해 CSV로 저장합니다.
    :param tickers: 주식 티커 리스트
    :param from_date: 데이터 시작 날짜 (예: '2024-01-01')
    :param to_date: 데이터 종료 날짜 (예: '2024-12-31')
    """
    # 파일명에 사용할 타임스탬프를 `from_date`로 설정
    output_file = f"stock_data_{from_date}.csv"

    all_data = []
    for ticker in tickers:
        print(f"Fetching data for {ticker} from {from_date} to {to_date}...")
        try:
            stock_data = yf.Ticker(ticker).history(start=from_date, end=to_date)
            if not stock_data.empty:
                stock_data = stock_data.reset_index().dropna()
                stock_data["Ticker"] = ticker
                stock_data["Korean Name"] = ticker_to_korean.get(ticker, "Unknown")
                all_data.append(stock_data)
            else:
                print(f"No data found for {ticker} in the given date range.")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        combined_data = combined_data[
            ["Ticker", "Korean Name", "Date", "Open", "High", "Low", "Close", "Volume"]
        ]
        combined_data.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        print(combined_data)
    else:
        print("No data fetched.")


if __name__ == "__main__":
    # 어제 날짜와 오늘 날짜 계산
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    # argparse를 사용하여 외부 매개변수 처리 (기본값: 어제부터 오늘까지)
    parser = argparse.ArgumentParser(description="Fetch stock data from yfinance.")
    parser.add_argument(
        "--from_date", type=str, default=yesterday, help="From date in YYYY-MM-DD format (default: yesterday)."
    )
    parser.add_argument(
        "--to_date", type=str, default=today, help="To date in YYYY-MM-DD format (default: today)."
    )
    args = parser.parse_args()

    tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK-B",
        "V", "JNJ", "PG", "UNH", "HD", "MA", "PFE", "KO", "DIS", "PEP", "BAC", "XOM"
    ]

    fetch_stock_data_with_dates(tickers, args.from_date, args.to_date)
