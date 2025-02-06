import os
import pandas as pd
import argparse

def convert_csv_to_parquet(csv_file, parquet_root="parquet", delete_csv=False):
    """
    지정된 CSV 파일을 Parquet 파일로 변환하여 별도 폴더에 저장

    :param csv_file: 변환할 CSV 파일 경로
    :param parquet_root: Parquet 파일을 저장할 루트 폴더 (기본값: "parquet/")
    :param delete_csv: 변환 후 CSV 파일 삭제 여부 (기본값: False)
    """
    if not os.path.exists(csv_file):
        print(f"[Error] The file does not exist: {csv_file}")
        return

    if not csv_file.endswith(".csv"):
        print(f"[Error] Not a CSV file: {csv_file}")
        return

    # 원본 CSV 파일이 있는 폴더 구조 유지하여 Parquet 저장
    csv_folder, csv_filename = os.path.split(csv_file)
    relative_path = os.path.relpath(csv_folder, start="csv")  # "csv/" 폴더 기준 상대 경로
    parquet_folder = os.path.join(parquet_root, relative_path)

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

def convert_all_csv_to_parquet(root_folder="csv", parquet_root="parquet", delete_csv=False):
    """
    지정된 폴더 내의 모든 CSV 파일을 찾아 Parquet 파일로 변환하여 별도 폴더에 저장

    :param root_folder: CSV 파일이 저장된 루트 디렉토리 (기본값: "csv/")
    :param parquet_root: Parquet 파일을 저장할 루트 폴더 (기본값: "parquet/")
    :param delete_csv: 변환 후 CSV 파일 삭제 여부 (기본값: False)
    """
    for root, _, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                convert_csv_to_parquet(csv_path, parquet_root=parquet_root, delete_csv=delete_csv)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV 파일을 Parquet으로 변환하여 별도 폴더에 저장하는 프로그램")

    parser.add_argument("--csv_file", type=str, help="변환할 단일 CSV 파일 경로")
    parser.add_argument("--folder", type=str, default="csv", help="CSV 파일이 저장된 폴더")
    parser.add_argument("--parquet_folder", type=str, default="parquet", help="Parquet 파일을 저장할 폴더")
    parser.add_argument("--delete_csv", action="store_true", help="변환 후 CSV 파일 삭제 여부")

    args = parser.parse_args()

    if args.csv_file:
        convert_csv_to_parquet(args.csv_file, parquet_root=args.parquet_folder, delete_csv=args.delete_csv)
    else:
        convert_all_csv_to_parquet(root_folder=args.folder, parquet_root=args.parquet_folder, delete_csv=args.delete_csv)
