"""created at: 2024-01-24"""
import os
import traceback
import datetime as dt
import random
import json
from dotenv import load_dotenv
import pandas as pd
import pymysql

load_dotenv()
SAVE_PATH = "{SAVE_PATH}"


def get_json():
    """
    Make raw file from S3
    """
    base_url = os.getenv("BASE_URL")

    table = ""
    df = pd.DataFrame()

    num = 1
    while True:
        try:
            df_json = pd.read_json(f"{base_url}/{table}/recent/{table}_{num}.json", lines=True)
            df = pd.concat([df, df_json])
            print(f"Done: channel_{num}")
            num += 1
        except Exception:
            print(traceback.format_exc())
            print(f"page: {num}")
            df.to_json(f"{SAVE_PATH}/{table}.json", orient="records", lines=True)
    return {}


def make_json():
    """
    Make json files from raw files
    """
    records = pd.read_csv(f"{SAVE_PATH}/booking.csv")
    channel = pd.read_json(f"{SAVE_PATH}/channel.json", lines=True)

    channel = channel.loc[channel["type"] == "instagram"]
    channel = channel[["id", "created_at"]]
    channel["created_at"] = channel["created_at"].apply(
        lambda x: x.strftime("%Y-%m-%d %H:%M:%S+00:00")
    )
    channel["opening_id"] = None
    channel["action_type"] = "sign_up"
    channel.rename(columns={"id": "user_id", "created_at": "date"}, inplace=True)

    records = pd.melt(
        frame=records[["channel_id", "campaign_id", "created_at", "selected_at", "canceled_at"]],
        id_vars=["channel_id", "campaign_id"],
        var_name="action_type",
        value_name="date",
    )
    records = records.loc[~records["date"].isnull()]
    records.rename(columns={"channel_id": "user_id", "campaign_id": "opening_id"}, inplace=True)
    records.loc[records["action_type"] == "created_at", "action_type"] = "booking"
    records.loc[records["action_type"] == "selected_at", "action_type"] = "selection"
    records.loc[records["action_type"] == "canceled_at", "action_type"] = "cancel"
    records = pd.concat([records, channel])
    records = records.groupby("user_id").filter(lambda x: x["date"].min() >= "2017-01-01")
    update_chn = records.groupby("user_id").min().reset_index()
    update_chn = update_chn.loc[update_chn["action_type"] == "booking"].to_dict(orient="records")
    new_records = []
    for line in update_chn:
        ts = dt.datetime.strptime(line["date"], "%Y-%m-%d %H:%M:%S+00:00")
        new_date = ts - dt.timedelta(days=random.randint(1, 7))
        if new_date < dt.datetime(2017, 1, 1):
            new_date = dt.datetime(2017, 1, 1)
        new_records.append(
            {
                "id": None,
                "user_id": line["user_id"],
                "opening_id": None,
                "action_type": "sign_up",
                "date": new_date.strftime("%Y-%m-%d %H:%M:%S+00:00"),
            }
        )
    records = pd.concat([records, pd.DataFrame(new_records)])
    records.sort_values(by="date", ascending=True, inplace=True)
    records.reset_index(inplace=True, drop=True)
    records["id"] = records.index
    print(records.head())
    amount = 100000
    num = len(records) // amount
    print(f"dataframe has {len(records)} records | there will be {num+1} files.")
    for page in range(num + 1):
        records.loc[
            page * amount : (page + 1) * amount,
            ["id", "user_id", "opening_id", "action_type", "date"],
        ].to_json(f"{SAVE_PATH}/data_{page}.json", orient="records", lines=True)

    return {}


def init_db():
    """
    Main function
    """
    file_name = ""

    list_file = [file for file in os.listdir(f"{SAVE_PATH}/") if file_name in file]
    con = pymysql.connect(
        host="localhost", port=3306, user="tester", password="1234", db="test", charset="utf8"
    )
    cursor = con.cursor()
    for file in list_file:
        with open(f"{SAVE_PATH}/{file}", "r", encoding="utf-8") as json_file:
            record = json.load(json_file)
        for data in record:
            cursor.execute(
                f"INSERT INTO logs(id, user_id, opening_id, action_type, date) VALUES({data['id']}, {data['user_id']}, {data['opening_id']}, '{data['action_type']}', '{str(data['date']).split('+', maxsplit=1)[0]}')"
            )
        con.commit()
    con.close()
    return {}


if __name__ == "__main__":
    get_json()
