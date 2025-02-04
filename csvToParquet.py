import os
import pandas as pd

def convert_csv_to_parquet(csv_file, delete_csv=False):
    """
    지정된 CSV 파일을 Parquet 파일로 변환

    :param csv_file: 변환할 csv 파일 경로
    :param delete_csv: 변환 후 csv 파일 삭제 여부 (기본값: False)
    """
    if not os.path.exists(csv_file):
        print(f"[Error] The File does not exist: {csv_file}")
        return

    if not csv_file.endswith(".csv"):
        print(f"[Error] The File is not csv file: {csv_file}")
        return

    # 변환할 파일명 지정
    parquet_file = csv_file.replace(".csv",".parquet")


