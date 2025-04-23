import os
import boto3
import pandas as pd
from typing import Optional
from datetime import datetime


def compile_csvs(
    bucket: str,
    prefix: str,
    date_start: str,
    date_end: str,
    output_path: Optional[str] = None,
    keep_files: Optional[bool] = False,
) -> pd.DataFrame:
    """
    Compiles all the csvs that match the prefix and date range.

    Args:
        bucket: The bucket to download the csvs from.
        prefix: The prefix of the csvs to download.
        date_start: The start date of the csvs to download in YYYY-MM-DD format.
        date_end: The end date of the csvs to download in YYYY-MM-DD format.
        output_path: The path to save the compiled csv to.  If not provided, no CSV will be created.
        keep_files: If True, the individual csvs will not be deleted after compilation.
    """
    # Convert string dates to datetime objects
    start_date = datetime.strptime(date_start, "%Y-%m-%d")
    end_date = datetime.strptime(date_end, "%Y-%m-%d")

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response["Contents"]:
        if start_date <= obj["LastModified"].replace(tzinfo=None) <= end_date:
            s3.download_file(bucket, obj["Key"], f"downloaded_csvs/{obj['Key']}")

    # read all the csvs into a list
    csv_files = [f for f in os.listdir("downloaded_csvs") if f.endswith(".csv")]
    df_list = [pd.read_csv(f"downloaded_csvs/{f}") for f in csv_files]
    df = pd.concat(df_list)

    if output_path:
        df.to_csv(output_path, index=False)

    if not keep_files:
        for file in csv_files:
            os.remove(f"downloaded_csvs/{file}")

    return df


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    compile_csvs(
        bucket="zonos-data-warehouse-stage",
        prefix="cash_fact-data/",
        date_start="2025-03-06",
        date_end="2025-04-25",
        output_path="cash_fact_data.csv",
        keep_files=False,
    )
