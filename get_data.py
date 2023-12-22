"""create for initialize DB"""
import os
import json
import pandas as pd
import pymysql


def make_json():
    """
    Main function
    """
    records = pd.read_csv(".tmp/booking.csv")
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
    records.sort_values(by="date", ascending=True, inplace=True)
    records.reset_index(inplace=True, drop=True)
    records["id"] = records.index

    amount = 100000
    num = len(records) // amount
    for page in range(num + 1):
        records.loc[
            page * amount : (page + 1) * amount,
            ["id", "user_id", "opening_id", "action_type", "date"],
        ].to_json(f".tmp/transfered_{page}.json", orient="records")

    return {}


def init_db():
    """
    Main function
    """
    list_file = [file for file in os.listdir(".tmp/") if "transfered" in file]
    con = pymysql.connect(
        host="localhost", port=3306, user="tester", password="1234", db="test", charset="utf8"
    )
    cursor = con.cursor()
    for file in list_file:
        with open(f".tmp/{file}", "r", encoding="utf-8") as json_file:
            record = json.load(json_file)
        for data in record:
            cursor.execute(
                f"INSERT INTO logs(id, user_id, opening_id, action_type, date) VALUES({data['id']}, {data['user_id']}, {data['opening_id']}, '{data['action_type']}', '{str(data['date']).split('+')[0]}')"
            )
        con.commit()
    con.close()
    return {}


if __name__ == "__main__":
    make_json()
    init_db()
