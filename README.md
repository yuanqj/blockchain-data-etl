# Blockchain Data ETL

This is Python script to extract and transform blockchain data, and load formatted data into AlibabaCloud SLS (aka "Log Service").


## Bitcoin
It's inspired by open source project [Bitcoin ETL](https://github.com/blockchain-etl/bitcoin-etl).

Usage for Bitcoin data ETL:
```
python bitcoin-etl.py \
  --ak-id=LTA******* \
  --ak-secret=******** \
  --endpoint=cn-shanghai.log.aliyuncs.com \
  --project=my-blockchain-data \
  --logstore=bitcoin \
  --btc-provider="http://username:password@localhost:8332" \
  --start-block=0 \
  --end-block=10000 \
  --workers=5
```
