from utils import MyConfigParser
from datetime import datetime
from io import StringIO
from utils import (
    get_json_objects,
    process_ds,
    flatten_json,
    request_api,
)
import pandas as pd
import boto3
import json


def create_ohlc_uri(pair: str, exchange: str, key: str, dt_format="%Y-%m-%d") -> str:
    """
    TODO...
    """
    uri = "https://api.cryptowat.ch/markets/"
    uri += f"{exchange}/{pair}/ohlc?after={after}&before={before}&apikey={key}"
    return uri


def process_asset_ohlc(s3, bucket: str, series: dict, key: str):
    """
    TODO...
    """
    pair = series["markets_pair"]
    exchange = series["markets_exchange"]
    uri = create_ohlc_uri(pair, exchange, key)
    res = request_api(uri)
    ohlc_data = json.loads(res)
    header = [
        "close_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_volume",
    ]

    csv_buffer = StringIO()
    exchange = series["markets_exchange"]
    pair = series["markets_pair"]

    # filter daily
    daily_data = ohlc_data["result"]["86400"]
    if len(daily_data) > 0:
        df = pd.DataFrame(data=daily_data, columns=header)
        # update df to include series data
        for k, v in series.items():
            df[k] = v
        df.to_csv(csv_buffer, index=False)

        try:
            s3.Object(
                bucket, f"ohlc-candlestick/{exchange}/{pair}/{year}/{month}.csv"
            ).put(Body=csv_buffer.getvalue())
        except Exception as e:
            msg = "ERROR: Could not dump market pairs data in S3."
            print(msg, e)
            return
    else:
        print(f"`{exchange}/{pair}/{year}/{month}` has no daily observations.")
        return


def dump_to_s3(s3, bucket: str, pair_name: str, key: str):
    """"""
    asset_pair_url = f"https://api.cryptowat.ch/pairs/{pair_name}?apikey={key}"
    pair_details = get_json_objects(asset_pair_url)
    flat_pair = flatten_json(pair_details["result"])

    for row in flat_pair["markets"]:
        new_pair = {
            **{k: v for k, v in flat_pair.items() if k != "markets"},
            **{f"markets_{k}": v for k, v in row.items()},
        }

        process_asset_ohlc(s3, bucket, new_pair, key)


def CryptowatchToS3(ds, pair_base, pair_curr, **kwargs):
    """
    TODO...
    """
    my_config = MyConfigParser()

    global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    global CRYPTO_ACCESS_KEY_ID, S3_BUCKET

    AWS_SECRET_ACCESS_KEY = my_config.aws_secret_access_key()
    AWS_ACCESS_KEY_ID = my_config.aws_access_key_id()
    CRYPTO_ACCESS_KEY_ID = my_config.crypto_access_key_id()
    AWS_REGION = my_config.aws_region()
    S3_BUCKET = my_config.s3_bucket()

    global after, before, month, year
    after, before = process_ds(ds)
    month = datetime.strptime(ds, "%Y-%m-%d").month
    year = datetime.strptime(ds, "%Y-%m-%d").year

    pairs = [f"{base}{curr}" for base in pair_base for curr in pair_curr]

    s3 = boto3.resource(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    for pair in pairs:
        dump_to_s3(s3, S3_BUCKET, pair, CRYPTO_ACCESS_KEY_ID)
    return
