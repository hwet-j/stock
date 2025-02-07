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

# 가상환경 초기화 및 실행
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)" 

/home/hwechang_jeong/.pyenv/versions/3.9.16/bin/python /home/hwechang_jeong/stock/save_csv_stock_data.py $ARGS >> /home/hwechang_jeong/stock/exe/stock_data.log 2>&1

/home/hwechang_jeong/.pyenv/versions/3.9.16/bin/python /home/hwechang_jeong/stock/csvToParquet.py


echo "[INFO] 실행 완료"

