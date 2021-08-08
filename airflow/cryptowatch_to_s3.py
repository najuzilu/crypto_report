from datetime import datetime, timedelta
from typing import Dict
from pathlib import Path
from io import StringIO
import pandas as pd
import configparser
import requests
import boto3
import json


def config_parser():
    """
    TODO...
    """
    config = configparser.ConfigParser()
    config.read(f"{Path(__file__).parent.resolve().parents[1]}/dwh.cfg")

    global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    global CRYPTO_ACCESS_KEY_ID, S3_BUCKET

    # Load global vars from file
    AWS_ACCESS_KEY_ID = config.get("AWS", "AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    AWS_REGION = config.get("AWS", "AWS_REGION")
    CRYPTO_ACCESS_KEY_ID = config.get("CRYPTO", "CRYPTO_ACCESS_KEY_ID")
    S3_BUCKET = config.get("S3", "S3_BUCKET")
    return


def _datetime_to_timestamp(s: str, dt_format="%Y-%m-%d") -> int:
    tstamp = datetime.strptime(s, dt_format).timestamp()
    return int(tstamp)


def get_date_of_month(ds: str) -> str:
    """
    TODO...
    """
    first_day_of_month = datetime.strptime(ds, "%Y-%m-%d").replace(day=1)
    last_day_prev_month = first_day_of_month - timedelta(1)
    start_day_of_prev_month = first_day_of_month - timedelta(
        days=last_day_prev_month.day
    )
    return start_day_of_prev_month.strftime("%Y-%m-%d")


def flatten_json(obj: Dict) -> Dict:
    """
    TODO...
    """
    out = {}

    def flatten(obj, name=""):
        if type(obj) is dict:
            for key, value in obj.items():
                flatten(value, name + key + "_")
        else:
            out[name[:-1]] = obj

    flatten(obj)
    return out


def request_api(uri: str) -> str:
    """
    TODO...
    """
    try:
        res = requests.get(uri)
    except requests.exceptions as err:
        msg = f"ERROR: Could not GET uri: {uri}."
        print(msg, err)
        return

    if res.status_code == 200:
        return res.text
    else:
        msg = f"ERROR {res.status_code}: Could not GET uri: {uri}."
        print(msg)
        return


def create_ohlc_uri(pair: str, exchange: str, key: str, dt_format="%Y-%m-%d") -> str:
    """
    TODO...
    """
    uri = "https://api.cryptowat.ch/markets/"
    uri += f"{exchange}/{pair}/ohlc?after={AFTER}&before={BEFORE}&apikey={key}"
    return uri


def get_asset_pair_details(name: str, key: str) -> str:
    """
    TODO...
    """
    asset_pair_url = f"https://api.cryptowat.ch/pairs/{name}?apikey={key}"
    res = request_api(asset_pair_url)
    try:
        obs = json.loads(res)
    except Exception as e:
        msg = f"ERROR: Could not parse data as JSON."
        print(msg, e)
        return
    return obs


def process_asset_ohlc(s3, bucket: str, pair: str, exchange: str, key: str):
    """
    TODO...
    """
    uri = create_ohlc_uri(pair, exchange, key)
    res = request_api(uri)
    ohlc_data = json.loads(res)

    csv_buffer = StringIO()
    header = [
        "close_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_volume",
    ]

    # filter daily
    daily_data = ohlc_data["result"]["86400"]
    if len(daily_data) > 0:
        df = pd.DataFrame(data=daily_data, columns=header)
        df["markets_pair"] = pair
        df["markets_exchange"] = exchange
        df.to_csv(csv_buffer, index=False)

        try:
            s3.Object(
                bucket, f"ohlc_candlestick/{exchange}/{pair}/{YEAR}/{MONTH}.csv"
            ).put(Body=csv_buffer.getvalue())
        except Exception as e:
            msg = "ERROR: Could not put market pairs data in S3."
            print(msg, e)
            return
    else:
        print(f"`{exchange}/{pair}/{YEAR}/{MONTH}` has no daily observations.")
        return


def dump_to_s3(s3, bucket: str, pair_name: str, key: str):
    """"""
    pair_details = get_asset_pair_details(pair_name, key)
    flat_pair = flatten_json(pair_details["result"])

    data = []

    for row in flat_pair["markets"]:
        new_pair = {
            **{k: v for k, v in flat_pair.items() if k != "markets"},
            **{f"markets_{k}": v for k, v in row.items()},
        }

        process_asset_ohlc(
            s3, bucket, new_pair["markets_pair"], new_pair["markets_exchange"], key
        )
        data.append(new_pair)

    csv_buffer = StringIO()
    df = pd.DataFrame(data)
    df.to_csv(csv_buffer, index=False)

    try:
        s3.Object(bucket, f"pairs/{pair_name}.csv").put(Body=csv_buffer.getvalue())
    except Exception as e:
        msg = "ERROR: Could not put market pairs data in S3."
        print(msg, e)
        return


def CryptowatchToS3(ds, pair_base, pair_curr, **kwargs):
    """
    TODO...
    """
    # global vars
    config_parser()

    global AFTER, BEFORE, MONTH, YEAR
    # execute_date
    AFTER = _datetime_to_timestamp(get_date_of_month(ds))
    BEFORE = _datetime_to_timestamp(ds)
    MONTH = datetime.strptime(ds, "%Y-%m-%d").month
    YEAR = datetime.strptime(ds, "%Y-%m-%d").year

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
