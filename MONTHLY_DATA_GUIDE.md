# CHELSA Monthly Data Support

## Overview

The CHELSA download tool has been extended to support monthly climate variables in addition to the existing bioclim data. This includes both present-day climatologies (1981-2010) and paleoclimate data from TraCE21k.

## Available Monthly Variables

### Variables
- **pr**: Precipitation (mm/month)
- **tasmin**: Minimum temperature (°C)
- **tasmax**: Maximum temperature (°C)

### Data Types

#### 1. Present-Day Monthly (1981-2010)
- **Source**: CHELSA v2.1 climatologies
- **Time period**: 1981-2010 averages
- **Resolution**: 30 arc-seconds (~1km)
- **Coverage**: 12 months (January through December)
- **Remote**: `chelsa02_climatologies`
- **URL pattern**: `chelsa02/chelsa/global/climatologies/{var}/1981-2010/CHELSA_{var}_{month}_1981-2010_V.2.1.tif`

#### 2. TraCE21k Monthly Centennial
- **Source**: CHELSA TraCE21k centennial data
- **Time coverage**: -200 to 20 years BP (Before Present, where 0 = 1950 CE)
- **Time steps**: Centennial (every 100 years)
- **Resolution**: 30 arc-seconds (~1km)
- **Coverage**: 12 months per time slice
- **Remote**: `chelsa01_trace_centennial`
- **URL pattern**: `chelsa01/chelsa_trace21k/global/centennial/{var}/CHELSA_TraCE21k_{var}_{month}_{timeslice}_V.1.0.tif`

## Unit Conversions

The tool automatically applies unit normalization when `--unit-normalize` is enabled (default):

### Temperature (tasmin, tasmax)
- **Raw format**: Kelvin × 10 (integer)
- **Normalized**: Degrees Celsius (float)
- **Conversion**: `(value / 10.0) - 273.15`
- **Example**: Raw value 2881 → 15.1°C

### Precipitation (pr), present monthly
- **Raw format**: mm × 10 (integer)
- **Normalized**: mm (float)
- **Conversion**: `value / 10.0`

### Precipitation (pr), TraCE21k monthly
- **Raw format**: mm (integer/float, no scale factor)
- **Normalized**: mm (float)
- **Conversion**: `value`
- **Example**: Raw value 1234 → 123.4 mm

## Configuration

### Automatic Setup
The tool automatically configures monthly data support with sensible defaults:

```toml
[present_monthly]
remote = "chelsa02_climatologies"
prefix = ""
lists_subdir = "present_monthly"
output_dir = "./outputs/present_monthly"
nodata_value = -9999.0

[trace_monthly]
remote = "chelsa01_trace_centennial"
prefix = ""
lists_subdir = "trace_monthly"
output_dir = "./outputs/trace_monthly"
nodata_value = -9999.0
```

### rclone Configuration
The `envicloud.conf` includes the necessary remote configurations:

```ini
[chelsa02_climatologies]
type = s3
provider = Ceph
endpoint = os.unil.cloud.switch.ch
access_key_id = anonymous
secret_access_key = anonymous

[chelsa01_trace_centennial]
type = s3
provider = Ceph
endpoint = os.zhdk.cloud.switch.ch
access_key_id = anonymous
secret_access_key = anonymous
```

## Usage

### 1. Prepare File Lists (Optional)

Before downloading, you can generate file lists for monthly data:

```bash
# Present-day monthly lists
chelsa-download prepare-lists --kind present_monthly

# TraCE21k monthly lists
chelsa-download prepare-lists --kind trace_monthly
```

**Note**: The tool comes with bundled lists in `default_lists/`, so this step is usually not necessary unless you want to update the lists.

### 2. Download Present-Day Monthly Data

#### Download all variables and all months:
```bash
chelsa-download download-present-monthly
```

#### Download specific variables:
```bash
chelsa-download download-present-monthly --var pr --var tasmin
```

#### Download specific months:
```bash
chelsa-download download-present-monthly --month 1 --month 7  # January and July
chelsa-download download-present-monthly --month-range 6-8    # June through August
```

#### Combine filters:
```bash
chelsa-download download-present-monthly --var tasmax --month-range 1-3 --limit 5
```

### 3. Download TraCE21k Monthly Data

#### Download all variables, all time slices, all months:
```bash
chelsa-download download-trace-monthly
```

#### Download specific time slices:
```bash
chelsa-download download-trace-monthly --time-slice -200 --time-slice 0
chelsa-download download-trace-monthly --time-range -200--100  # From -200 to -100 BP
chelsa-download download-trace-monthly --time-range -200-0 --time-interval 10  # Every 10th time slice
```

#### Download specific months:
```bash
chelsa-download download-trace-monthly --month 1 --month 7
chelsa-download download-trace-monthly --month-range 12-2  # December through February
```

#### Combine all filters:
```bash
chelsa-download download-trace-monthly \
  --var pr \
  --time-range -100-0 \
  --time-interval 5 \
  --month-range 6-8 \
  --limit 10
```

### 4. Common Options

All download commands support these options:

- `--max-workers N`: Set number of parallel downloads (default: 6)
- `--windowed` / `--no-windowed`: Use HTTP range requests (default: yes)
- `--unit-normalize` / `--no-unit-normalize`: Apply unit conversions (default: yes)
- `--force`: Overwrite existing files
- `--limit N`: Process only first N files (useful for testing)

## Output Structure

### Present-Day Monthly
```
outputs/present_monthly/
├── pr/
│   ├── CHELSA_pr_01_1981-2010_V.2.1_AOI.tif
│   ├── CHELSA_pr_02_1981-2010_V.2.1_AOI.tif
│   └── ...
├── tasmin/
│   ├── CHELSA_tasmin_01_1981-2010_V.2.1_AOI.tif
│   └── ...
└── tasmax/
    ├── CHELSA_tasmax_01_1981-2010_V.2.1_AOI.tif
    └── ...
```

### TraCE21k Monthly
```
outputs/trace_monthly/
├── pr/
│   ├── CHELSA_TraCE21k_pr_01_-200_V.1.0_AOI.tif
│   ├── CHELSA_TraCE21k_pr_01_-199_V.1.0_AOI.tif
│   └── ...
├── tasmin/
│   └── ...
└── tasmax/
    └── ...
```

## Examples

### Example 1: Download Summer Precipitation for Present Day
```bash
chelsa-download download-present-monthly \
  --var pr \
  --month-range 6-8
```

This downloads June, July, and August precipitation data for the 1981-2010 period.

### Example 2: Download Winter Temperatures for Last Glacial Maximum
```bash
chelsa-download download-trace-monthly \
  --var tasmin --var tasmax \
  --time-slice -200 \
  --month-range 12-2
```

This downloads minimum and maximum temperatures for December, January, and February at -200 years BP.

### Example 3: Sample Monthly Data Every 10 Years
```bash
chelsa-download download-trace-monthly \
  --time-range -200-0 \
  --time-interval 10 \
  --limit 50
```

This downloads every 10th time slice from -200 to 0 BP, limited to 50 files for testing.

## Performance Tips

1. **Use windowed mode** (default): Downloads only the pixels within your AOI, significantly faster
2. **Adjust workers**: Use `--max-workers 8-12` on fast connections, reduce to 2-4 if getting errors
3. **Test first**: Use `--limit 5` to test your filters before downloading everything
4. **Month filtering**: Download only the months you need to save time and disk space
5. **Time slicing**: Use `--time-interval` to sample time slices rather than downloading all

## Troubleshooting

### "Request failed with response_code=0" warnings
These are rate limiting warnings from the server. Reduce `--max-workers` to 2-4.

### Missing files after download
Check that you have the correct file lists. Run `prepare-lists --kind present_monthly` or `--kind trace_monthly` to regenerate.

### Unit conversion issues
If you need raw values for debugging, use `--no-unit-normalize`. Otherwise, temperatures will be in °C and precipitation in mm.

### Memory issues
Reduce `--max-workers` or use `--limit` to process fewer files at once.

## Technical Details

### File Naming Conventions

**Present-day**: `CHELSA_{var}_{month}_1981-2010_V.2.1.tif`
- `{var}`: pr, tasmin, or tasmax
- `{month}`: 01-12 (zero-padded)

**TraCE21k**: `CHELSA_TraCE21k_{var}_{month}_{timeslice}_V.1.0.tif`
- `{var}`: pr, tasmin, or tasmax
- `{month}`: 01-12 (zero-padded)
- `{timeslice}`: -200 to 0020 (zero-padded to 4 chars, negative with minus sign)

### Regex Patterns

Present monthly: `CHELSA_(?P<var>pr|tas(?:min|max))_(?P<month>\d{2})_1981-2010_V\.2\.1\.tif`

Trace monthly: `CHELSA_TraCE21k_(?P<var>pr|tas(?:min|max))_(?P<month>\d{2})_(?P<time>-?\d{4})_V\.1\.0\.tif`

## Changes from Previous Version

### New Commands
- `download-present-monthly`: Download present-day monthly climatologies
- `download-trace-monthly`: Download TraCE21k monthly centennial data

### Extended Commands
- `prepare-lists`: Now supports `--kind present_monthly` and `--kind trace_monthly`

### New Configuration Sections
- `[present_monthly]`: Configuration for present-day monthly data
- `[trace_monthly]`: Configuration for TraCE21k monthly data

### New rclone Remotes
- `chelsa02_climatologies`: For present-day monthly data
- `chelsa01_trace_centennial`: For TraCE21k monthly data

### New Filtering Options
- `--month`, `--month-range`: Filter by month (1-12)
- `--time-slice`, `--time-range`, `--time-interval`: Filter by time (trace monthly only)

## References

- [CHELSA v2.1 Documentation](https://chelsa-climate.org/)
- [TraCE21k Project](https://www.earthsystemgrid.org/project/trace.html)
- [rclone Documentation](https://rclone.org/docs/)
