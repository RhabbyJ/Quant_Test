import glob
import os
from typing import Iterable

import duckdb
import pandas as pd


def list_channel_files(channel: str, base_dir: str = "data_warehouse") -> list[str]:
    pattern = os.path.join(base_dir, channel, "**", "*.parquet")
    return [p.replace("\\", "/") for p in glob.glob(pattern, recursive=True)]


def load_channel(
    channel: str,
    base_dir: str = "data_warehouse",
    limit: int | None = None,
    files: Iterable[str] | None = None,
) -> pd.DataFrame:
    file_list = list(files) if files is not None else list_channel_files(channel, base_dir=base_dir)
    if not file_list:
        return pd.DataFrame()

    con = duckdb.connect()
    try:
        try:
            if limit is None:
                query = "SELECT * FROM read_parquet(?, union_by_name=true)"
                return con.execute(query, [file_list]).df()
            query = "SELECT * FROM read_parquet(?, union_by_name=true) ORDER BY exchange_ts DESC LIMIT ?"
            return con.execute(query, [file_list, int(limit)]).df()
        except Exception:
            # Fallback: scan per file and skip unreadable/schema-broken files.
            chunks = []
            for path in sorted(file_list):
                try:
                    df = con.execute("SELECT * FROM read_parquet(?, union_by_name=true)", [path]).df()
                except Exception:
                    try:
                        con.rollback()
                    except Exception:
                        pass
                    continue
                if not df.empty:
                    chunks.append(df)
            if not chunks:
                return pd.DataFrame()
            merged = pd.concat(chunks, ignore_index=True)
            if limit is not None and "exchange_ts" in merged.columns:
                merged = merged.sort_values("exchange_ts", ascending=False).head(int(limit))
            return merged
    finally:
        con.close()


def load_default_channels(base_dir: str = "data_warehouse") -> dict[str, pd.DataFrame]:
    return {
        "spot": load_channel("spot", base_dir=base_dir),
        "trade": load_channel("trade", base_dir=base_dir),
        "orderbook_delta": load_channel("orderbook_delta", base_dir=base_dir),
        "paper_fill": load_channel("paper_fill", base_dir=base_dir),
        "lifecycle": load_channel("lifecycle", base_dir=base_dir),
        "market_meta": load_channel("market_meta", base_dir=base_dir),
    }
