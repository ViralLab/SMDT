"""
I/O utilities for network data, such as writing edges to Parquet.
"""

import pyarrow as pa
import pyarrow.parquet as pq


def write_edges_to_parquet_from_chunks(chunks, output_path, compression="snappy"):
    """Write edge chunks to a Parquet file.

    Args:
        chunks: Iterable of DataFrames (edge chunks).
        output_path: Path to the output Parquet file.
        compression: Compression algorithm (default: "snappy").
    """
    writer = None
    for chunk in chunks:
        if chunk.empty:
            continue
        table = pa.Table.from_pandas(chunk)
        if writer is None:
            writer = pq.ParquetWriter(
                output_path, table.schema, compression=compression
            )
        writer.write_table(table)
    if writer is not None:
        writer.close()
        print(f"[io_utils] Wrote edges → {output_path}")
    else:
        print(f"[io_utils] No edges to write → {output_path}")
