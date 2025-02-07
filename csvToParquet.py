import os
import pandas as pd
import argparse

DEFAULT_LOG_FILE_PATH = "/home/hwechang_jeong/stock/exe/csv_files.log"
DEFAULT_PARQUET_FOLDER = "parquet"  # 기본 Parquet 저장 폴더

def convert_csv_to_parquet(csv_file, delete_csv=False):
    """
    지정된 CSV 파일을 Parquet 파일로 변환하여 기본 폴더(parquet/)에 저장

    :param csv_file: 변환할 CSV 파일 경로
    :param delete_csv: 변환 후 CSV 파일 삭제 여부
    """
    if not os.path.exists(csv_file):
        print(f"[Error] 파일이 존재하지 않음: {csv_file}")
        return

    if not csv_file.endswith(".csv"):
        print(f"[Error] CSV 파일이 아님: {csv_file}")
        return

    # 원본 CSV 파일이 있는 폴더 구조 유지하여 Parquet 저장
    csv_folder, csv_filename = os.path.split(csv_file)
    relative_path = os.path.relpath(csv_folder, start="csv")  # "csv/" 폴더 기준 상대 경로
    parquet_folder = os.path.join(DEFAULT_PARQUET_FOLDER, relative_path)

    # Parquet 폴더 생성
    os.makedirs(parquet_folder, exist_ok=True)

    # Parquet 파일 경로 설정
    parquet_file = os.path.join(parquet_folder, csv_filename.replace(".csv", ".parquet"))

    try:
        df = pd.read_csv(csv_file)
        df.to_parquet(parquet_file, engine="pyarrow", compression="snappy")

        print(f"[Parquet 변환 완료] {parquet_file}")

        if delete_csv:
            os.remove(csv_file)
            print(f"[CSV 삭제] {csv_file}")

    except Exception as e:
        print(f"[Error] {csv_file}: {e}")

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
        convert_csv_to_parquet(csv_file, delete_csv=delete_csv)

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
    parser.add_argument("--delete_csv", action="store_true", help="변환 후 CSV 파일 삭제 여부")

    args = parser.parse_args()

    if args.csv_file:
        # 특정 CSV 파일 변환
        convert_csv_to_parquet(args.csv_file, delete_csv=args.delete_csv)
    elif args.folder:
        # 특정 폴더의 모든 CSV 변환
        convert_all_csv_to_parquet(root_folder=args.folder, delete_csv=args.delete_csv)
    else:
        # 로그 파일 기반으로 가장 최근 변환된 파일만 처리
        convert_logged_csv_to_parquet(log_file=args.log_file, delete_csv=args.delete_csv)


"""
# 가장 마지막 실행으로 생성된 csv파일 변환
python csvToParquet.py
# 특정 csv파일 하나 변환
python csvToParquet.py --csv_file "csv/2024/02/stock_data_2024-02-06.csv"
# 폴더를 지정해 폴더내 csv 변환
python csvToParquet.py --folder "csv/2024/02"
# csv 파일 삭제 추가
python csvToParquet.py --folder "csv/2024/02" --delete_csv

"""