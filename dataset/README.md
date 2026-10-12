# Dataset

The raw AIS file is **not** in this repository. It is 4.9 GB (4,899,356,398 bytes) and
GitHub rejects any single file over 100 MB.

## Source

Automatic Identification System (AIS) position reports for US coastal and inland waters,
a NOAA MarineCadastre-style vessel-traffic extract.

| Property | Value |
|---|---|
| File name | `marine_ais_dataset.csv` |
| Size on disk | 4.9 GB |
| Rows | 47,958,624 |
| Columns | 17 |
| Distinct MMSIs | 24,688 |
| Time window | 2025-01-01 00:00:00 – 2025-01-07 23:59:59 UTC |
| Format | CSV, comma separated, single header row, UTC timestamps |

## Columns

| # | Column | Meaning |
|---|---|---|
| 1 | `mmsi` | Maritime Mobile Service Identity (vessel identifier) |
| 2 | `base_date_time` | Timestamp of the position report (UTC) |
| 3 | `longitude` | Decimal degrees |
| 4 | `latitude` | Decimal degrees |
| 5 | `sog` | Speed over ground, knots |
| 6 | `cog` | Course over ground, degrees |
| 7 | `heading` | True heading, degrees |
| 8 | `vessel_name` | Reported vessel name |
| 9 | `imo` | IMO number |
| 10 | `call_sign` | Radio call sign |
| 11 | `vessel_type` | AIS numeric ship-type code |
| 12 | `status` | AIS navigational status code |
| 13 | `length` | Metres |
| 14 | `width` | Metres |
| 15 | `draft` | Metres |
| 16 | `cargo` | AIS cargo-type code |
| 17 | `transceiver` | Transceiver class, `A` or `B` |

`heading`, `imo`, `call_sign`, `draft` and `cargo` are frequently null; the cleaning stage
records exactly how many values it nullified or dropped. See [../docs/dataset.md](../docs/dataset.md)
for the full data profile.

## Sample in this repository

`sample_ais.csv` holds the **first 1,000 data rows** of the raw file (plus the header,
1,001 lines, ~104 KB). It exists so the schema and value ranges can be inspected and the
unit tests can run without the full file. It is **not** enough to reproduce any result.

## Placing the full file locally

1. Obtain `marine_ais_dataset.csv` from the group's shared drive (it is not redistributed here).
2. Put it in the **project root**, next to `README.md`:

   ```
   BDS_Assignment/
     marine_ais_dataset.csv   <- here
     README.md
     dataset/
     src/
   ```

3. `.gitignore` already excludes it, so it can never be committed by accident.
4. To keep it elsewhere, point the `MX_RAW_INPUT` environment variable at it instead
   of moving the file:

   ```powershell
   $env:MX_RAW_INPUT = "D:\ais\marine_ais_dataset.csv"
   ```

## Where the data lives at runtime

| Item | Location |
|---|---|
| Raw AIS CSV (local dev) | `marine_ais_dataset.csv` in the project root (`MX_RAW_INPUT`) |
| Raw AIS CSV (cluster) | `hdfs:///maritime/raw/ais/` |
| Local data lake, mirrors the HDFS layout | `output/lake/` (`MX_DATA_ROOT`), ~12 GB, not committed |
| Port reference (38 curated geofences) | `config/ports.csv` |

### Uploading to HDFS

```powershell
. .\scripts\env_cluster.ps1
hdfs dfs -mkdir -p /maritime/raw/ais
hdfs dfs -put marine_ais_dataset.csv /maritime/raw/ais/
hdfs dfs -du -h /maritime/raw/ais
```
