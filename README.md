# PVPro

A Python tool for automated photo and video organization and archiving with Beijing timezone support.

## Features

- **Smart Metadata Extraction**: Extracts EXIF data from photos and FFmpeg metadata from videos
- **Beijing Timezone Conversion**: Converts various timestamp formats to Beijing timezone (YYYYmmddHHMMSS)
  - Supports EXIF format: `YYYY:MM:DD HH:MM:SS`
  - Supports Chinese AM/PM format: `2018:03:04 10:35:51上午`
  - Supports Unix timestamps (seconds/milliseconds)
  - Supports ISO formats and natural language
- **Intelligent File Renaming**: Standardizes filenames to `{timestamp}_{camera-model}_{original-name}` format
- **Automatic Archiving**: Organizes files by month (YYYYMM) and type (photos='p', videos='v')
- **Duplicate Handling**: Detects and manages duplicate files with configurable strategies
- **Special File Handling**: 
  - Files with unknown camera models go to `snapshot/` directory
  - Duplicate files are managed in `duplicates/` directory
- **Comprehensive Statistics**: Provides monthly statistics with before/after comparisons
- **Progress Tracking**: Detailed logging and real-time progress bars

## Quick Start

### Installation

```bash
# Clone and install dependencies
git clone <repository-url>
cd pvpro
pip install -e .
```

### Basic Usage

```python
from app.core import Pivor, compare_stats

# Initialize (uses PV_ROOT environment variable)
pv = Pivor()

# Get current statistics
stats_before = pv.stats()
print(stats_before)

# Process files from a directory
pv.fit(work_dir="/path/to/photos", handle_duplicate=True)

# Get statistics after processing
stats_after = pv.stats()

# Compare before/after statistics
changes = compare_stats(stats_before, stats_after)
print(changes.to_markdown())

# Preview renaming without actual changes
preview = pv.preview("/path/to/photos")
for original, renamed in preview.items():
    print(f"{original.name} -> {renamed.name}")
```

### Environment Setup

Set the `PV_ROOT` environment variable:

```bash
export PV_ROOT="/path/to/photo/archive"
```

## Directory Structure

PVPro creates this organized structure:

```
PV_ROOT/
├── archive/          # Main archive
│   └── YYYYMM/       # Monthly folders (e.g., 202412)
│       ├── p/        # Photos (JPG, PNG, CR2, ARW)
│       └── v/        # Videos (MOV, MP4, AVI, MKV)
├── duplicates/       # Duplicate files
├── snapshot/         # Files with unknown camera models
├── __process/        # Default processing directory
└── .logs/           # Detailed operation logs
```

## Core Methods

### `Pivor.stats()`
Returns a DataFrame with monthly file counts:
- Columns: `month`, `p` (photos), `v` (videos), `total`
- Last rows show `dup` (duplicates) and `snap` (snapshots)

### `Pivor.preview(work_dir)`
Preview how files will be renamed without making changes.
Returns: `Dict[Path, Path]` mapping original to new filenames.

### `Pivor.fit(work_dir=None, handle_duplicate=True)`
Process and archive files:
- `work_dir`: Directory to process (defaults to `__process/`)
- `handle_duplicate`: Whether to manage duplicate files

### `Pivor.recover()`
Recover duplicate files from the duplicates directory.

### `Pivor.rename(file, mv=False)`
Rename a single file according to the naming convention.
Set `mv=True` to actually rename the file.

### `compare_stats(stats_before, stats_after)`
Compare statistics before and after processing, showing changes with arrow indicators.

## Supported File Formats

**Images**: `.jpg`, `.jpeg`, `.png`, `.cr2`, `.arw`
**Videos**: `.mov`, `.mp4`, `.avi`, `.mkv`

## File Naming Convention

Files are renamed to: `{timestamp}_{camera-model}_{original-name}{extension}`

Examples:
- `20241213142830_iPhone-15_IMG-2859.MOV`
- `20241005125329_iPhone-15_IMG-2803.MOV`

## Advanced Features

### Metadata Fallback
If EXIF/metadata is unavailable, falls back to file modification time.

### Chinese Time Format Support
Special handling for Chinese AM/PM formats like:
- `2018:03:04 10:35:51上午`
- `2018:05:25 21:23:28下午`

### Detailed Logging
All operations are logged to `.logs/` with rotation and UTF-8 encoding.

## License

MIT License