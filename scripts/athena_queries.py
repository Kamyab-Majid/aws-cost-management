from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

# Set your AWS credentials and region
session = boto3.Session(profile_name="prod", region_name="us-east-1")

# Set the Athena workgroup and time range for queries
athena_workgroup = "primary"
start_date = datetime.now(timezone.utc) - timedelta(days=30)

# Initialize Athena client
athena_client = session.client("athena")


# Define a function to list Athena queries in a specified time range
def list_queries(workgroup, start_time, next_token=None):
    if next_token:
        response = athena_client.list_query_executions(
            WorkGroup=workgroup,
            MaxResults=50,  # Set to 100000 to retrieve more queries
            NextToken=next_token,
        )
    else:
        response = athena_client.list_query_executions(
            WorkGroup=workgroup,
            MaxResults=50,  # Set to 100000 to retrieve more queries
        )
    queries = response.get("QueryExecutionIds", [])

    # Filter queries based on the start time
    filtered_queries = [
        query_id
        for query_id in queries
        if athena_client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]["SubmissionDateTime"]
        >= start_time
    ]

    return filtered_queries, response["NextToken"]


# Initialize an empty DataFrame
df = pd.DataFrame(
    columns=["Query ID", "Status", "Execution Time (ms)", "Data Scanned (bytes)"]
)
next_token = None
for i in range(20000):
    # Get the list of queries in the last month
    queries, next_token = list_queries(
        athena_workgroup, start_date, next_token=next_token
    )
    query_list = []
    # Append details of each query to the DataFrame
    for query_id in queries:
        query_details = athena_client.get_query_execution(QueryExecutionId=query_id)

        try:
            if (
                query_details["QueryExecution"]["Statistics"]["DataScannedInBytes"]
                < 578224112
            ):
                continue
            query_list.append(
                {
                    "token": next_token,
                    "Query ID": query_id,
                    "submission_date": query_details["QueryExecution"]["Status"][
                        "SubmissionDateTime"
                    ],
                    "Workgroup": query_details["QueryExecution"]["WorkGroup"],
                    "Status": query_details["QueryExecution"]["Status"]["State"],
                    # "Query": query_details["QueryExecution"]["Query"],
                    "ResultReuseConfiguration": query_details["QueryExecution"][
                        "ResultReuseConfiguration"
                    ]["ResultReuseByAgeConfiguration"]["Enabled"],
                    "Execution Time (ms)": query_details["QueryExecution"][
                        "Statistics"
                    ]["EngineExecutionTimeInMillis"],
                    "Data Scanned (bytes)": query_details["QueryExecution"][
                        "Statistics"
                    ]["DataScannedInBytes"],
                }
            )
            with open(f"queries/{query_id}", "w") as f:
                f.write(query_details["QueryExecution"]["Query"])

        except Exception as error:
            print(query_id)
            print(error)
    print(query_details["QueryExecution"]["Status"][
                        "SubmissionDateTime"])
    df2 = pd.DataFrame(query_list)
    # with pd.ExcelWriter("output.xlsx", mode="a") as writer:
    #     df2.to_excel(writer, sheet_name="Sheet_name_3")
    df2.to_csv("athena_result.csv", mode="a", index=False, header=False)
