import yfinance as yf
import pandas as pd

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


def fetch_stock_data(tickers, period="1mo", interval="1d", output_file="stock_data_with_korean.csv"):
    """
    주식 데이터를 yfinance에서 가져와 CSV로 저장하고 출력
    :param tickers: 주식 티커 리스트 (예: ['AAPL', 'MSFT', 'GOOGL'])
    :param period: 데이터 범위 (예: '1d', '5d', '1mo', '1y', 'max')
    :param interval: 데이터 간격 (예: '1d', '1h', '5m')
    :param output_file: 저장할 CSV 파일 이름
    """
    
    # 출력 생략 방지 설정
    # pd.set_option("display.max_rows", None)  # 최대 행 표시
    # pd.set_option("display.max_columns", None)  # 최대 열 표시
    # pd.set_option("display.expand_frame_repr", False)  # 데이터 프레임 가로로 펼쳐서 표시
    # pd.set_option("display.float_format", "{:.2f}".format)  # 소수점 형식 지정

    all_data = []
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        try:
            stock_data = yf.Ticker(ticker).history(period=period, interval=interval)
            if not stock_data.empty:
                stock_data = stock_data.reset_index()
                stock_data["Ticker"] = ticker   # Ticker 컬럼 추가
                stock_data["Korean Name"] = ticker_to_korean.get(ticker, "Unknown")  # 한국 이름 추가
                all_data.append(stock_data)
            else:
                print(f"No data found for {ticker}")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        # 필요한 컬럼만 선택
        """
        Date: 날짜
        Open: 시가
        High: 고가
        Low: 저가
        Close: 종가
        Volume: 거래량
        Dividends: 배당금
        Stock Splits: 주식 분할 정보
        """
        combined_data = combined_data[
            ["Ticker", "Korean Name", "Date", "Open", "High", "Low", "Close", "Volume"]
        ]
        # CSV로 저장
        combined_data.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        print(combined_data)  # 전체 데이터 출력
    else:
        print("No data fetched.")


if __name__ == "__main__":
    tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK-B",
        "V", "JNJ", "PG", "UNH", "HD", "MA", "PFE", "KO", "DIS", "PEP", "BAC", "XOM"
    ]
    fetch_stock_data(tickers, period="1y", interval="1d", output_file="stock_data_with_korean.csv")
