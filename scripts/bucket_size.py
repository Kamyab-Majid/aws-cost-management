from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd


def get_bucket_size(
    bucket_name,
    storage_type="StandardStorage",
    session=None,
    profile_name="sand",
    region_name="us-east-1",
):
    """
    Retrieves the size of a bucket in bytes.

    Args:
        bucket_name (str): The name of the bucket.
        storage_type (str, optional): The storage type of the bucket. Defaults to "StandardStorage".
        session (boto3.Session, optional): The session object. Defaults to None.

    Returns:
        int: The size of the bucket in bytes.
    """
    if session is None:
        session = boto3.Session(profile_name=profile_name, region_name=region_name)

    # Create CloudWatch client
    cloudwatch = session.client("cloudwatch")

    # Define the metric query
    metric_data_query = {
        "Id": "m1",
        "MetricStat": {
            "Metric": {
                "Namespace": "AWS/S3",
                "MetricName": "BucketSizeBytes",
                "Dimensions": [
                    {"Name": "BucketName", "Value": bucket_name},
                    {"Name": "StorageType", "Value": storage_type},
                ],
            },
            "Period": 86400,  # 1 day (in seconds)
            "Stat": "Average",
            "Unit": "Bytes",
        },
        "ReturnData": True,
    }

    # Set the start and end time for the metric query
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=5)  # Retrieve data for the last 7 days

    # Perform the metric query
    response = cloudwatch.get_metric_data(
        MetricDataQueries=[metric_data_query], StartTime=start_time, EndTime=end_time
    )

    # Extract and return the bucket size data
    if "MetricDataResults" in response:
        for result in response["MetricDataResults"]:
            for i in range(len(result["Timestamps"])):
                value = result["Values"][i]
                return value

    else:
        raise Exception("No metric data results found.")


# Replace 'your_bucket_name' with the actual S3 bucket name
# Initialize the S3 client
session = boto3.Session(profile_name="sand", region_name="us-east-1")
s3 = session.client("s3")
results_df = pd.DataFrame(columns=["Bucket Name", "Size (TB)"])
# List all S3 buckets
response = s3.list_buckets()

# Iterate through each bucket
for bucket in response["Buckets"]:
    bucket_name = bucket["Name"]

    # Call the function to get bucket size
    size_in_bytes = get_bucket_size(bucket_name, storage_type="StandardStorage")
    print(size_in_bytes)
    # Check if the size is larger than 1 TB (1 TB = 1,099,511,627,776 bytes)
    if size_in_bytes and size_in_bytes > 1099511627776:
        size_in_tb = size_in_bytes / 1099511627776
        # Append data to the DataFrame
        results_df = pd.concat(
            [
                results_df,
                pd.DataFrame(
                    {"Bucket Name": [bucket_name], "Size Standard (TB)": [size_in_tb]}
                ),
            ],
            ignore_index=True,
        )
        print(f"Bucket '{bucket_name}' size is larger than 1 TB: {size_in_tb} TB")
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html
    size_in_bytes = get_bucket_size(bucket_name, storage_type="StandardIAStorage")
    # print(size_in_bytes)
    if size_in_bytes and size_in_bytes > 1099511627776:
        size_in_tb = size_in_bytes / 1099511627776
        # Append data to the DataFrame
        results_df = pd.concat(
            [
                results_df,
                pd.DataFrame(
                    {"Bucket Name": [bucket_name], "Size IA (TB)": [size_in_tb]}
                ),
            ],
            ignore_index=True,
        )
        # print(f"Bucket '{bucket_name}' size is larger than 1 TB: {size_in_tb} TB")
csv_file_path = "bucket_sizes.csv"
results_df.to_csv(csv_file_path, index=False)

print(f"Results saved to {csv_file_path}")
