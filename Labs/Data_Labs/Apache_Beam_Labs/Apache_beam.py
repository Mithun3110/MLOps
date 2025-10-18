import apache_beam as beam
import csv, io
from datetime import datetime

inputs_pattern = 'data/*.csv'
outputs_prefix = 'outputs/station_daily'

# --- helpers ---
def parse_csv_line(line):
    # csv.reader handles commas inside quotes safely
    # return dict using header captured separately
    return next(csv.reader([line]))

def to_dict(header):
    # returns a function that maps list->dict using captured header
    def _mapper(row):
        return {h: row[i] if i < len(row) else "" for i, h in enumerate(header)}
    return _mapper

def parse_datetime(s):
    # accept e.g. "2020-07-01 08:15:42" or ISO "2020-07-01T08:15:42"
    s = s.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def safe_duration_seconds(start_str, end_str):
    s, e = parse_datetime(start_str), parse_datetime(end_str)
    if not s or not e or e < s:
        return None
    return int((e - s).total_seconds())

def keep_valid(row):
    # basic quality filter
    return (
        row.get('start_time') and row.get('end_time') and row.get('start_station')
        and safe_duration_seconds(row['start_time'], row['end_time']) is not None
    )

def to_keyed_metrics(row):
    dur = safe_duration_seconds(row['start_time'], row['end_time'])
    date_key = row['start_time'].split("T")[0].split(" ")[0]  # "YYYY-MM-DD"
    key = (date_key, row['start_station'])
    # value = (sum_duration, count)
    return (key, (dur, 1))

def sum_pairs(a, b):
    return (a[0] + b[0], a[1] + b[1])

def format_output(kv):
    (date, station), (sum_dur, n) = kv
    avg = round(sum_dur / n, 2) if n else 0.0
    return f"{date}\t{station}\ttrips={n}\tavg_duration_s={avg}"

with beam.Pipeline() as p:
    lines = p | 'ReadCSV' >> beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)

    # If headers vary per file, read first line separately. Otherwise, pass a static header list:
    header = ['ride_id','start_time','end_time','start_station','start_lat','start_lng','end_station','end_lat','end_lng']  # adjust to your schema

    results = (
        lines
        | 'CSV->List' >> beam.Map(parse_csv_line)
        | 'List->Dict' >> beam.Map(to_dict(header))
        | 'KeepValid' >> beam.Filter(keep_valid)
        | 'ToKeyed' >> beam.Map(to_keyed_metrics)
        | 'Combine' >> beam.CombinePerKey(
                lambda vals: (
                    sum(d for d, _ in vals),     # total duration
                    sum(c for _, c in vals),     # total count
                )
            )

        | 'Format' >> beam.Map(format_output)
    )

    _ = results | 'WriteText' >> beam.io.WriteToText(outputs_prefix)

# Example to peek results on Unix shells:
# head -n 50 outputs/station_daily-00000-of-*
