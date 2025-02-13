#!/bin/bash

# 전달된 인자 확인
ARGS=""

if [ ! -z "$1" ]; then
    ARGS="$ARGS --tickers $1"
fi

if [ ! -z "$2" ]; then
    ARGS="$ARGS --from_date $2"
fi

if [ ! -z "$3" ]; then
    ARGS="$ARGS --to_date $3"
fi

echo "[INFO] 실행 시작 - 인자: $ARGS"

# Pyenv Setting
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
pyenv activate hwetenv

# 주식 데이터 추출  (인자 포함)
echo "[INFO] save_csv_stock_data.py 실행 중..."
# /home/hwechang_jeong/.pyenv/versions/3.9.16/bin/python /home/hwechang_jeong/stock/fetch_stock_data.py $ARGS >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1

python /home/hwechang_jeong/stock/fetch_stock_data.py $ARGS >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1
# 실행 결과 확인
if [ $? -ne 0 ]; then
    echo "[ERROR] fetch_stock_data.py 실행 실패" >> /home/hwechang_jeong/stock/exe/stock_data.log
    exit 1
fi

# CSV 파일 Parquet파일 변환
echo "[INFO] csv_to_parquet.py 실행 중..."
#/home/hwechang_jeong/.pyenv/versions/3.9.16/bin/python /home/hwechang_jeong/stock/csv_to_parquet.py >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1

python /home/hwechang_jeong/stock/csv_to_parquet.py >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1
if [ $? -ne 0 ]; then
    echo "[ERROR] csv_to_parquet.py 실행 실패" >> /home/hwechang_jeong/stock/exe/stock_data.log
    exit 1
fi

# Parquet파일 DB 저장
echo "[INFO] parquet_to_db.py 실행 중..."
#/home/hwechang_jeong/.pyenv/versions/3.9.16/bin/python /home/hwechang_jeong/stock/parquet_to_db.py >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1

python /home/hwechang_jeong/stock/parquet_to_db.py >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1
if [ $? -ne 0 ]; then
    echo "[ERROR] parquet_to_db.py 실행 실패" >> /home/hwechang_jeong/stock/exe/stock_data.log
    exit 1
fi

echo "[INFO] 실행 완료"


