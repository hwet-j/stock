import pandas as pd

file_path = "parquet/2025/02/stock_data_2025-02-10.parquet"
df = pd.read_parquet(file_path)

print(df.head())  # 상위 5개 행 출력
print(df.columns) # 전체 컬럼 확인
