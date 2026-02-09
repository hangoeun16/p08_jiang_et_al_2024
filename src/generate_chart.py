from pathlib import Path

import pull_wrds
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

import seaborn as sns
from matplotlib import pyplot as plt
import pandas as pd

sns.set_theme()

df = pull_wrds.load_RCFD_series_1(data_dir=DATA_DIR)
df.rename(columns={"rcfd0010": "cash"}, inplace=True)
df.groupby("rssd9999")["cash"].sum()

# Plot the Cash over time (column "rssd9999" is the date)
plt.figure(figsize=(12, 6))
sns.lineplot(data=df, x="rssd9999", y="cash", marker="o")
plt.title("Total Cash Over Time")
plt.xlabel("Date")
plt.ylabel("Total Cash")
plt.xticks(rotation=45)
plt.tight_layout()

filename = OUTPUT_DIR / "example_plot.png"
plt.savefig(filename)
