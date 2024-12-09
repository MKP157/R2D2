import pandas as pd
from io import BytesIO
import datetime
import requests
from matplotlib.pyplot import title
from requests.utils import rewind_body
import time
import sqlite3
import pprint

EPOCH = datetime.datetime.fromtimestamp(0, datetime.UTC)

def unix_time_millis(dt):
    return int((dt - EPOCH.replace(tzinfo=None)).total_seconds() * 1000)

def output_results (logs: dict, db_size : int, chunk_size : int) :
    import numpy as np
    import matplotlib.pyplot as plt

    fig, axs = plt.subplots(ncols=2, nrows=2, figsize=(13, 10),
                            layout="constrained")


    # find line of best fit
    x = np.array(logs["insert"]["row_total"])
    y_r = np.array(logs["insert"]["r2d2"])
    y_s = np.array(logs["insert"]["sql"])

    a_r, b_r = np.polyfit(x, y_r, 1)
    a_s, b_s = np.polyfit(x, y_s, 1)


    axs[0, 0].plot(
        logs["insert"]["row_total"],
        logs["insert"]["sql"],
        "rs",
        label=f"SQLite : y={b_s:.4f}+{a_s:.2E}x",
    )
    axs[0, 0].plot(
        logs["insert"]["row_total"],
        logs["insert"]["r2d2"],
        "bs",
        label=f"R2D2 : y={b_r:.4f}+{a_r:.2E}x",
    )

    # add line of best fit to plot
    axs[0,0].plot(x, a_r * x + b_r, color='navy', linestyle='--', linewidth=2)
    axs[0,0].plot(x, a_s * x + b_s, color='maroon', linestyle='--', linewidth=2)

    axs[0, 0].set_title("Insert in Random Order")
    axs[0, 0].set_xlabel("Database Size")
    axs[0, 0].set_ylabel("Time (ms)")
    axs[0, 0].legend()


    # find line of best fit
    x = np.array(logs["query_one"]["key"])
    y_r = np.array(logs["query_one"]["r2d2"])
    y_s = np.array(logs["query_one"]["sql"])

    a_r, b_r = np.polyfit(x, y_r, 1)
    a_s, b_s = np.polyfit(x, y_s, 1)

    axs[0, 1].plot(
        logs["query_one"]["key"],
        logs["query_one"]["sql"],
        "rs",
        label=f"SQLite : y={b_s:.4f}+{a_s:.2E}x",
    )

    axs[0, 1].plot(
        logs["query_one"]["key"],
        logs["query_one"]["r2d2"],
        "bs",
        label=f"R2D2 : y={b_r:.4f}+{a_r:.2E}x",
    )

    # add line of best fit to plot
    axs[0, 1].plot(x, a_r * x + b_r, color='navy', linestyle='--', linewidth=2)
    axs[0, 1].plot(x, a_s * x + b_s, color='maroon', linestyle='--', linewidth=2)

    axs[0, 1].set_title("Randomized Singular Select")
    axs[0, 1].set_xlabel("Row's Key Value (Database Timestamp; ms)")
    axs[0, 1].set_ylabel("Time (ms)")
    axs[0, 1].legend()



    # find line of best fit
    x = np.array([abs(r[0] - r[1])/1000 for r in logs["query_range"]["ranges"]])
    y_r = np.array(logs["query_range"]["r2d2"])
    y_s = np.array(logs["query_range"]["sql"])

    a_r, b_r = np.polyfit(x, y_r, 1)
    a_s, b_s = np.polyfit(x, y_s, 1)

    axs[1, 0].plot(
        x,
        logs["query_range"]["sql"],
        "rs",
        label=f"SQLite : y={b_s:.4f}+{a_s:.2E}x",
    )
    axs[1, 0].plot(
        x,
        logs["query_range"]["r2d2"],
        "bs",
        label=f"R2D2 : y={b_r:.4f}+{a_r:.2E}x",
    )

    # add line of best fit to plot
    axs[1, 0].plot(x, a_r * x + b_r, color='navy', linestyle='--', linewidth=2)
    axs[1, 0].plot(x, a_s * x + b_s, color='maroon', linestyle='--', linewidth=2)

    axs[1, 0].set_title("Randomized Range Queries")
    axs[1, 0].set_xlabel("Range Width (Database Timestamp; ms)")
    axs[1, 0].set_ylabel("Time (ms)")
    axs[1, 0].legend()



    # find line of best fit
    x = np.array(logs["delete"]["row_total"])
    y_r = np.array(logs["delete"]["r2d2"])
    y_s = np.array(logs["delete"]["sql"])

    a_r, b_r = np.polyfit(x, y_r, 1)
    a_s, b_s = np.polyfit(x, y_s, 1)

    axs[1, 1].plot(
        logs["delete"]["row_total"],
        logs["delete"]["sql"],
        "rs",
        label=f"SQLite : y={b_s:.4f}+{a_s:.2E}x",
    )
    axs[1, 1].plot(
        logs["delete"]["row_total"],
        logs["delete"]["r2d2"],
        "bs",
        label=f"R2D2 : y={b_r:.4f}+{a_r:.2E}x",
    )

    # add line of best fit to plot
    axs[1, 1].plot(x, a_r * x + b_r, color='navy', linestyle='--', linewidth=2)
    axs[1, 1].plot(x, a_s * x + b_s, color='maroon', linestyle='--', linewidth=2)

    axs[1, 1].set_title("Delete in Random Order")
    axs[1, 1].set_xlabel("Database Size")
    axs[1, 1].set_ylabel("Time (ms)")
    axs[1, 1].invert_xaxis()
    axs[1, 1].legend()

    fig.suptitle(f'Metrics for Sample Data Size {db_size}, Operation Chunk Size {int(chunk_size)}')
    plt.show()

def test_insert_r2d2 (dataframe, logging_threshold=1000.0) :
    print("*****TESTING R2D2 INSERT*****")

    i = 0
    took = 0
    for index, row in dataframe.iterrows():

        put = f"http://127.0.0.1:6969/INSERT::"

        # Zip header names and row values for query convention
        enum = zip(header, row)
        for e in enum:
            put += f"{e[0]}={e[1]},"

        # Remove last comma in list
        put = put[:-1]

        # Get UNIX timestamp from CSV's ISO codes
        unix_time = unix_time_millis(datetime.datetime.strptime(str(index), "%Y-%m-%d"))
        put += f"::TIMESTAMP={unix_time}::HIDE"

        # Submit and get status code
        last_time = time.time()
        status = requests.get(put, timeout=120).status_code
        took += time.time() - last_time

        # Print error codes, if any
        if status != 200:
            print(status)

        # Update timing very 1000 submissions
        if i > 0 and i % logging_threshold == 0:
            print(f"\033[34m (+{took:.4}s)  Inserted {i} of {len(dataframe)} entries...\033[00m")
            results['insert']['row_total'].append(i),
            results['insert']['r2d2'].append(took * 1000)
            took = 0
        i += 1

def test_insert_sql (dataframe, logging_threshold=1000.0) :
    print("*****TESTING SQL3 INSERT*****")

    i = 0
    last_time = time.time()
    for index, row in dataframe.iterrows():
        # Get UNIX timestamp from CSV's ISO codes
        # unix_time = unix_time_millis(datetime.datetime.strptime(str(index), "%Y-%m-%d"))

        # s = f"{i},{unix_time}" + "".join([f",{c}" for c in row])
        #print(s)

        s = f"{i},{index}" + "".join([f",{c}" for c in row])


        cursor.execute(f"""INSERT INTO train VALUES ({s});""")
        conn.commit()

        # Update timing very 1000 submissions
        if i > 0 and i % logging_threshold == 0:
            took = time.time() - last_time
            last_time = time.time()
            print(f"\033[31m (+{took:.4}s)  Inserted {i} of {len(dataframe)} entries...\033[00m")
            results['insert']['sql'].append(took * 1000)
        i += 1

def test_query_one (dataframe, logging_threshold=1000.0) :
    print("*****TESTING QUERY ONE*****")

    i = 0
    took_r2d2 = 0
    took_sql = 0
    last_time = 0
    for index, row in dataframe.iterrows():
        # R2D2 ################################################################################
        # unix_time = unix_time_millis(datetime.datetime.strptime(str(index), "%Y-%m-%d"))
        unix_time = index

        r2d2_put = f"http://127.0.0.1:6969/LIST::ONE::{unix_time}::HIDE"

        # Submit and get status code
        last_time = time.time()
        status = requests.get(r2d2_put, timeout=120).status_code
        took_r2d2 += time.time() - last_time

        # Print error codes, if any
        if status != 200:
            print(status)
        #######################################################################################

        # SQL3 #################################################################################
        last_time = time.time()
        cursor.execute(f"""SELECT * FROM train WHERE timestamp={unix_time};""")
        s = cursor.fetchall()
        took_sql += time.time() - last_time
        #######################################################################################

        # Update timing very 1000 submissions
        if i > 0 and i % logging_threshold == 0:
            print(f"\033[34m (+{took_r2d2:.4}s) R2D2 Queried {i} of {len(dataframe)} entries...\033[00m")
            print(f"\033[31m (+{took_sql:.4}s) SQL3 Queried {i} of {len(dataframe)} entries...\033[00m")

            results['query_one']['key'].append(unix_time),
            results['query_one']['r2d2'].append(took_r2d2 * 1000)
            results['query_one']['sql'].append(took_sql * 1000)

            took_r2d2 = 0
            took_sql = 0

        i += 1

def test_query_range (dataframe, num_tests = 1000) :
    print("*****TESTING QUERY RANGE*****")

    i = 0
    took_r2d2 = 0
    took_sql = 0
    last_time = 0
    for i in range(num_tests):
        random_row_pair = dataframe.sample(2)
        pair = []

        for index, _ in random_row_pair.iterrows():
            #unix_time = unix_time_millis(datetime.datetime.strptime(str(index), "%Y-%m-%d"))
            unix_time = index
            pair.append(unix_time)

        pair.sort()

        # R2D2 ################################################################################

        r2d2_put = f"http://127.0.0.1:6969/LIST::RANGE{pair[0]},{pair[1]}::HIDE"

        # Submit and get status code
        last_time = time.time()
        status = requests.get(r2d2_put, timeout=120).status_code
        took_r2d2 += time.time() - last_time

        # Print error codes, if any
        if status != 200:
            print(status)
        #######################################################################################

        # SQL3 #################################################################################
        last_time = time.time()
        cursor.execute(f"""SELECT * FROM train WHERE timestamp BETWEEN {pair[0]} and {pair[1]};""")
        conn.commit()
        s = cursor.fetchall()
        took_sql += time.time() - last_time
        #######################################################################################

        print(f"\033[34m (+{took_r2d2:.4}s) for R2D2 for pair {i}:{pair}...\033[00m")
        print(f"\033[31m (+{took_sql:.4}s) for SQL3 for pair {i}:{pair}...\033[00m")

        results['query_range']['ranges'].append(pair),
        results['query_range']['r2d2'].append(took_r2d2 * 1000)
        results['query_range']['sql'].append(took_sql * 1000)

        took_r2d2 = 0
        took_sql = 0

        i += 1

def test_delete (dataframe, logging_threshold=1000.0) :
    print("*****TESTING DELETE*****")

    i = 0
    took_r2d2 = 0
    took_sql = 0
    last_time = 0
    for index, row in dataframe.iterrows():
        # R2D2 ################################################################################
        #unix_time = unix_time_millis(datetime.datetime.strptime(str(index), "%Y-%m-%d"))
        #print("removing : ", index)
        unix_time = index

        r2d2_put = f"http://127.0.0.1:6969/REMOVE::ONE::TIMESTAMP={unix_time}::HIDE"

        # Submit and get status code
        last_time = time.time()
        status = requests.get(r2d2_put, timeout=120).status_code
        took_r2d2 += time.time() - last_time

        # Print error codes, if any
        if status != 200:
            print(status)
        #######################################################################################

        # SQL3 #################################################################################
        last_time = time.time()
        cursor.execute(f"""DELETE FROM train WHERE timestamp={unix_time};""")
        s = cursor.fetchall()
        took_sql += time.time() - last_time
        #######################################################################################

        # Update timing very 1000 submissions
        if i > 0 and i % logging_threshold == 0:
            print(f"\033[34m (+{took_r2d2:.4}s) R2D2 removed {i} of {len(dataframe)} entries...\033[00m")
            print(f"\033[31m (+{took_sql:.4}s) SQL3 removed {i} of {len(dataframe)} entries...\033[00m")

            results['delete']['row_total'].append(LIMIT - i),
            results['delete']['r2d2'].append(took_r2d2 * 1000)
            results['delete']['sql'].append(took_sql * 1000)

            took_r2d2 = 0
            took_sql = 0

        i += 1

if __name__ == "__main__":
    for database_size in [
        #10,
        100,
        1000,
        10000,
        100000,
        1000000,
    ]:
        CHUNK_SIZE = 1000 if database_size > 10000 else database_size / 100
        LIMIT = database_size

        results = {
            "insert": {
                "row_total": [],
                "sql": [],
                "r2d2": []
            },

            "query_one": {
                "key": [],
                "sql": [],
                "r2d2": []
            },

            "query_range": {
                "ranges": [],
                "sql": [],
                "r2d2": []
            },

            "delete": {
                "row_total": [],
                "sql": [],
                "r2d2": []
            }
        }

        original_df = pd.read_csv('train.csv', index_col=0, nrows=LIMIT)
        header = original_df.columns.values.tolist()

        # Set up SQL3ite test database ##########################################
        conn = sqlite3.connect(f'sql_test_{LIMIT}.db')
        cursor = conn.cursor()

        # Drop all existing tables
        cursor.execute("DROP TABLE IF EXISTS train")

        print(header)

        # Create a new table
        cursor.execute(f"""
            CREATE TABLE train (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                {"".join([f"{col} INTEGER," for col in header])[:-1]}
            );
        """)

        conn.commit()

        # Set up R2D2 test database ##########################################
        r2d2_put = f"http://127.0.0.1:6969/LOAD::empty.r2d2"

        # Submit and get status code
        status = requests.get(r2d2_put, timeout=120).status_code

        # Print error codes, if any
        if status != 200:
            print(status)

        # Insert tests
        test_insert_r2d2(original_df, CHUNK_SIZE)

        # When R2D2 has to insert a key which already exists, it will handle
        # this collision implicitly by incrementing the value of the key
        # until it finds an unused value (like a hash table). Because of this,
        # the values in the R2D2 database will **not** match exactly with the
        # original CSV. So, we'll use the SAVE::CSV command, which straight up
        # dumps the R2D2 database to CSV, and load that instead for the remainder
        # of the tests. This is why the SQL3 insert tests are done separately from
        # R2D2's.

        r2d2_put = f"http://127.0.0.1:6969/SAVE::CSV"

        # Submit and get status code
        status = requests.get(r2d2_put, timeout=120).status_code

        # Print error codes, if any
        if status != 200:
            print(status)

        r2d2_put = f"http://127.0.0.1:6969/SAVE::test_{LIMIT}.r2d2"

        # Submit and get status code
        status = requests.get(r2d2_put, timeout=120).status_code

        # Print error codes, if any
        if status != 200:
            print(status)

        new_df = pd.read_csv('../data/dump.csv', index_col=0, nrows=LIMIT)

        test_insert_sql(new_df, CHUNK_SIZE)

        # Reshuffle
        new_df = new_df.sample(frac=1)

        # Query tests
        test_query_one(new_df, CHUNK_SIZE)
        test_query_range(dataframe=new_df, num_tests=int(LIMIT/100 if LIMIT > 1000 else 100))

        #input('\033[32m >>> Check for rows, then press enter to continue...\033[00m')
        test_delete(new_df, CHUNK_SIZE)
        #input('\033[32m >>> Check for empty, then press enter to continue...\033[00m')

        conn.close()

        # Graphing
        output_results(results, LIMIT, CHUNK_SIZE)

        pprint.pp(results)
        time.sleep(5)

    print("Done!")
    exit(1)