import pandas as pd
from io import BytesIO
import datetime
import requests
from requests.utils import rewind_body

EPOCH = datetime.datetime.fromtimestamp(0, datetime.UTC)

def unix_time_millis(dt):
    return int((dt - EPOCH.replace(tzinfo=None)).total_seconds() * 1000)

if __name__ == "__main__":
    df = pd.read_csv('train.csv', index_col=0, nrows=100)

    header = df.columns.values.tolist()

    for index, row in df.iterrows():
        enum = zip(header, row)


        put = f"http://127.0.0.1:6969/INSERT::"
        for e in enum:
            put += f"{e[0]}={e[1]},"

        time = unix_time_millis(datetime.datetime.strptime(str(index), "%Y-%m-%d"))

        put = put[:-1]
        put += f"::TIMESTAMP={time}::HIDE"

        status = requests.get(put, timeout=1).status_code

        if status != 200:
            print(status)