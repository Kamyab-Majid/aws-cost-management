from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

session = boto3.Session(profile_name="prod", region_name="us-east-1")


def get_bucket_object_count(bucket_name, storage_type="AllStorageTypes"):
    # Create CloudWatch client
    cloudwatch = session.client("cloudwatch")

    # Define the metric query for the number of objects
    metric_data_query = {
        "Id": "m1",
        "MetricStat": {
            "Metric": {
                "Namespace": "AWS/S3",
                "MetricName": "NumberOfObjects",
                "Dimensions": [
                    {"Name": "BucketName", "Value": bucket_name},
                    {"Name": "StorageType", "Value": storage_type},
                ],
            },
            "Period": 86400,  # 1 day (in seconds)
            "Stat": "Average",
            "Unit": "Count",
        },
        "ReturnData": True,
    }

    # Set the start and end time for the metric query
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=1)  # Retrieve data for the last 1 day

    # Perform the metric query
    response = cloudwatch.get_metric_data(
        MetricDataQueries=[metric_data_query], StartTime=start_time, EndTime=end_time
    )

    # Extract and return the number of objects
    if "MetricDataResults" in response:
        for result in response["MetricDataResults"]:
            return result["Values"][0] if result["Values"] else 0
    else:
        print(f"No metric data results found for {bucket_name} - {storage_type}")
        return 0


# Initialize the S3 client
s3 = session.client("s3")
results_df = pd.DataFrame(columns=["Bucket Name", "Object Count", "Storage Type"])

# List all S3 buckets
response = s3.list_buckets()

# Iterate through each bucket
for bucket in response["Buckets"]:
    bucket_name = bucket["Name"]

    # Call the function to get object count for AllStorageTypes
    object_count = get_bucket_object_count(bucket_name, storage_type="AllStorageTypes")

    # Append data to the DataFrame
    results_df = pd.concat(
        [
            results_df,
            pd.DataFrame({"Bucket Name": [bucket_name], "Object Count": [object_count], "Storage Type": ["AllStorageTypes"]}),
        ],
        ignore_index=True,
    )
    print(f"Bucket '{bucket_name}' has {object_count} objects (AllStorageTypes)")

# Save the results to a CSV file
csv_file_path = "bucket_object_counts.csv"
results_df.to_csv(csv_file_path, index=False)

print(f"Results saved to {csv_file_path}")
