"""Silver stage — clean, filter and de-duplicate the bronze events.

TODO (Завдання 2 і 3): реалізуйте build_silver() і write_silver_partitioned().
Контракт: див. CONTRACTS.md → "silver" і "silver partitioned".

build_silver():
  * залиште тільки типи з config.TARGET_EVENT_TYPES
  * приберіть рядки з порожнім/відсутнім repo_name, відсутнім event_id чи created_at
  * гарантуйте унікальність по event_id (.unique(subset=["event_id"]))
  * запишіть у config.SILVER_FILE і поверніть DataFrame

write_silver_partitioned():
  * запишіть silver як Hive-партиціонований датасет за event_type
  * директорія: config.SILVER_PARTITIONED_DIR
  * підказка: df.write_parquet(dir, partition_by="event_type")
"""

from __future__ import annotations

import glob
import os
import polars as pl

from . import config


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    
    lazy = pl.scan_parquet(config.BRONZE_FILE)
    lazy = lazy.filter(pl.col("event_type").is_in(config.TARGET_EVENT_TYPES))
    lazy = lazy.filter(pl.col("repo_name").is_not_null() & pl.col("event_id").is_not_null() & pl.col("created_at").is_not_null())
    lazy = lazy.unique(subset=["event_id"])
    
    df_silver = lazy.collect()
    
    output_dir = os.path.dirname(config.SILVER_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    df_silver.write_parquet(config.SILVER_FILE)
    
    return df_silver

    with pl.Config(tbl_cols=-1):
        print(f"rows in df: {len(df_silver)}")
        print(df_silver.head(5))

def write_silver_partitioned(silver: pl.DataFrame) -> None:
    silver.write_parquet(
        config.SILVER_PARTITIONED_DIR,  
        use_pyarrow=True,
        pyarrow_options={"partition_cols": ["event_type"], "existing_data_behavior": "delete_matching"},
    )
    
    search_path = os.path.join(config.SILVER_PARTITIONED_DIR, "**", "*.parquet")
    file_paths = glob.glob(search_path, recursive=True)
    
    counts_list = []
    for path in file_paths:
        row_count = pl.read_parquet(path).height
        counts_list.append({"file_path": path, "row_count": row_count})

    total_rows = pl.DataFrame(counts_list)["row_count"].sum()
    print(f"rows in file: {total_rows}")

    
    file_counts = (
        pl.DataFrame(counts_list)
        .sort("row_count", descending=True)
    )
      
    with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=100):
        print(file_counts)
        print(f"\nrows in df: {len(silver)}")
        print(silver.head(5))
    
    
    return silver 

