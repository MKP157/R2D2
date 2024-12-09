import os
from math import trunc

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

labels = []
results = {
    "csv" : [],
    "sql" : [],
    "r2d2" : [],
}

for size in [
	#100,
	#1000,
	10_000,
	100_000,
	1_000_000
]:
    df_csv = pd.read_csv("train.csv", nrows=size)
    df_csv.to_csv(f"train_{size}.csv")

    csv_size = round(os.stat(f"train_{size}.csv").st_size / 1_000_000, 2)
    sql_size = round(os.stat(f"sql_test_{size}.db").st_size / 1_000_000, 2)
    r2d2_size= round(os.stat(f"../data/test_{size}.r2d2").st_size / 1_000_000, 2)

    labels.append(str(df_csv.shape[0]))
    results["csv"].append(csv_size)
    results["sql"].append(sql_size)
    results["r2d2"].append(r2d2_size)

print(results)

x = np.arange(len(labels))  # the label locations
width = 0.25  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(layout='constrained')

for attribute, measurement in results.items():
    offset = width * multiplier
    rects = ax.bar(x + offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3)
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Database Size (n)')
ax.set_ylabel('File Size (megabytes)')
ax.set_title('File Size by Data Storage Type')
ax.set_xticks(x + width, labels=labels)
ax.legend(loc='upper left', ncols=3)

plt.show()