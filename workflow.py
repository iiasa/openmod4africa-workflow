from pathlib import Path
import pyam
from nomenclature import DataStructureDefinition, RegionProcessor, process


here = Path(__file__).absolute().parent


def main(df: pyam.IamDataFrame) -> pyam.IamDataFrame:
    """Project/instance-specific workflow for scenario processing"""

    dimensions = ["region", "variable"]
    if "subannual" in df.dimensions or df.time_col == "time":
        dimensions = dimensions + ["subannual"]

    # initialize the codelists and region-processing
    dsd = DataStructureDefinition(here / "definitions", dimensions=dimensions)
    processor = RegionProcessor.from_directory(path=here / "mappings", dsd=dsd)

    # run the validation and region-processing
    df = process(df, dsd, processor=processor)

    # convert to subannual format if data provided in datetime format
    if df.time_col == "time":
        logger.info('Re-casting from "time" column to categorical "subannual" format')
        df = df.swap_time_for_year(subannual=OE_SUBANNUAL_FORMAT)

    # check that any datetime-like items in "subannual" are valid datetime and UTC+01:00
    if "subannual" in df.dimensions:
        _datetime = [s for s in df.subannual if s not in dsd.subannual]

        for d in _datetime:
            try:
                _dt = datetime.strptime(f"2020-{d}", "%Y-%m-%d %H:%M%z")
            except ValueError:
                try:
                    datetime.strptime(f"2020-{d}", "%Y-%m-%d %H:%M")
                except ValueError:
                    raise ValueError(f"Invalid subannual timeslice: {d}")

                raise ValueError(f"Missing timezone: {d}")

            # casting to datetime with timezone was successful
            if not (_dt.tzname() == EXP_TZ or _dt.utcoffset() == EXP_TIME_OFFSET):
                raise ValueError(f"Invalid timezone: {d}")

    return df
