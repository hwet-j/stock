#!/bin/bash

# 로그 설정
LOG_DIR="/home/hwechang_jeong/stock/exe"
LOG_FILE="$LOG_DIR/stock_data.log"

# CSV 및 Parquet 관련 설정
CSV_DIR="/home/hwechang_jeong/stock/csv"
PARQUET_SCRIPT="/home/hwechang_jeong/stock/csvToParquet.py"
PYTHON_BIN="/home/hwechang_jeong/.pyenv/versions/3.9.16/bin/python"

# 환경 변수 설정
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"

echo "[INFO] 실행 시작" | tee -a "$LOG_FILE"

# 1️⃣ 주식 데이터 저장 (save_csv_stock_data.py 실행)
echo "[INFO] 주식 데이터 저장 시작" | tee -a "$LOG_FILE"
$PYTHON_BIN /home/hwechang_jeong/stock/save_csv_stock_data.py "$@" >> "$LOG_FILE" 2>&1

# 2️⃣ 생성된 CSV 파일 목록 가져오기
echo "[INFO] 생성된 CSV 파일 확인" | tee -a "$LOG_FILE"
CSV_FILES=$(find "$CSV_DIR" -type f -name "*.csv" -mtime -1)  # 최근 1일 내 변경된 CSV 파일 찾기

if [ -z "$CSV_FILES" ]; then
    echo "[INFO] 변환할 CSV 파일 없음" | tee -a "$LOG_FILE"
else
    echo "[INFO] Parquet 변환 시작" | tee -a "$LOG_FILE"
    for csv_file in $CSV_FILES; do
        echo "[INFO] 변환 중: $csv_file" | tee -a "$LOG_FILE"
        $PYTHON_BIN "$PARQUET_SCRIPT" --csv_file "$csv_file" >> "$LOG_FILE" 2>&1
    done
fi

echo "[INFO] 실행 완료" | tee -a "$LOG_FILE"

