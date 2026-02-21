import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any
import asyncio
from dataclasses import asdict

class BufferedParquetWriter:
    """
    Buffers events in memory and flushes them to partitioned Parquet files.
    This avoids the "death by tiny files" problem in high-throughput tick data.
    """
    def __init__(self, data_dir: str, flush_interval_sec: float = 2.0, max_buffer_size: int = 1000):
        self.data_dir = data_dir
        self.flush_interval_sec = flush_interval_sec
        self.max_buffer_size = max_buffer_size
        self.buffers: Dict[str, List[Dict[str, Any]]] = {
            "orderbook_delta": [],
            "trade": [],
            "lifecycle": [],
            "market_meta": [],
            "spot": [],
            "paper_fill": []
        }
        self.last_flush_time = time.time()
        self._lock = asyncio.Lock()
        
        # Ensure directories exist
        for channel in self.buffers.keys():
            os.makedirs(os.path.join(self.data_dir, channel), exist_ok=True)

    async def ingest_event(self, channel: str, event: Any):
        """
        Ingests a dataclass event into the buffer. Trigger flush if thresholds met.
        """
        async with self._lock:
            if channel not in self.buffers:
                raise ValueError(f"Unknown channel: {channel}")
            
            # Convert dataclass to dict handling Enum serialization
            event_dict = asdict(event)
            if 'side' in event_dict and hasattr(event_dict['side'], "value"):
                event_dict['side'] = event_dict['side'].value
                
            self.buffers[channel].append(event_dict)
            
            should_flush = (
                len(self.buffers[channel]) >= self.max_buffer_size or 
                (time.time() - self.last_flush_time) >= self.flush_interval_sec
            )
            
        if should_flush:
            await self.flush()

    async def flush(self):
        """
        Flushes all buffered events to Parquet partitioned loosely by date/hour.
        """
        async with self._lock:
            current_time = pd.Timestamp.utcnow()
            date_str = current_time.strftime('%Y-%m-%d')
            hour_str = current_time.strftime('%H')
            
            for channel, records in self.buffers.items():
                if not records:
                    continue
                    
                df = pd.DataFrame(records)
                table = pa.Table.from_pandas(df)
                
                # Partition path logic: data_dir/channel/date=YYYY-MM-DD/hour=HH/
                partition_dir = os.path.join(self.data_dir, channel, f"date={date_str}", f"hour={hour_str}")
                os.makedirs(partition_dir, exist_ok=True)
                
                # File name uses timestamp to avoid overwrites
                file_name = f"{int(time.time() * 1000)}.parquet"
                file_path = os.path.join(partition_dir, file_name)

                # Atomic write: write to temp file first, then replace.
                # This prevents readers from observing partially written parquet files.
                tmp_path = file_path + ".tmp"
                try:
                    pq.write_table(table, tmp_path)
                    os.replace(tmp_path, file_path)
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                
                # Clear buffer
                self.buffers[channel] = []
                
            self.last_flush_time = time.time()
