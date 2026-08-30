"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

import os
import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
  
    gold_df = (
    silver.group_by("repo_name")
    .agg(
        pl.count().alias("event_count"),
        pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types")
    )
    .sort("event_count", descending=True)
    .with_columns(pl.col("event_count").cast(pl.Int64))
)
    
    output_dir = os.path.dirname(config.GOLD_REPO_ACTIVITY)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    gold_df.write_parquet(config.GOLD_REPO_ACTIVITY)
    
    if os.path.exists(config.GOLD_REPO_ACTIVITY):

        saved_file = pl.read_parquet(config.GOLD_REPO_ACTIVITY)     
       
        
        total_event_sum = saved_file["event_count"].sum()
    else:
            total_rows_in_file = 0
            total_event_sum = 0

    with pl.Config(tbl_cols=-1):
        print(gold_df.head(5))
        print(f"\nrows in df: {len(gold_df)}")
   
        print(f"sum event_count: {total_event_sum}")
        
    return gold_df


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    
    gold_df = (
    silver.with_columns(pl.col("created_at").dt.truncate("1m").alias("minute"))
    .group_by("minute")
    .agg(pl.count().cast(pl.Int64).alias("event_count"))
    .sort("minute")
)
    
    output_dir = os.path.dirname(config.GOLD_ACTIVITY_PER_MINUTE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    gold_df.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE)
    
    if os.path.exists(config.GOLD_ACTIVITY_PER_MINUTE):

        saved_file = pl.read_parquet(config.GOLD_ACTIVITY_PER_MINUTE)     
       
        total_event_sum = saved_file["event_count"].sum()
    else:
            total_rows_in_file = 0
            total_event_sum = 0

    with pl.Config(tbl_cols=-1):
        print(gold_df.head(5))
        print(f"sum event_count: {total_event_sum}")
        
    return gold_df

def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    
    gold_df = ( 
        silver.filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg(
            pl.count().cast(pl.Int64).alias("push_events"),
            pl.col("commit_count").sum().cast(pl.Int64).alias("total_commits")
        )
        

    )

    output_dir = os.path.dirname(config.GOLD_PUSH_COMMITS)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    gold_df.write_parquet(config.GOLD_PUSH_COMMITS)
    
    if os.path.exists(config.GOLD_PUSH_COMMITS):
        saved_file = pl.read_parquet(config.GOLD_PUSH_COMMITS)     
        total_rows_in_file = saved_file.height
        total_push_sum = saved_file["total_commits"].sum()
    else:
            total_rows_in_file = 0
            total_push_sum = 0

    with pl.Config(tbl_cols=-1):
        print(gold_df.head(15))
        print(f"\nrows in df: {len(gold_df)}")
        print(f"sum push_count: {total_push_sum}")

        
    return gold_df

   