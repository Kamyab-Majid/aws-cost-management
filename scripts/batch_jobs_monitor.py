import csv
from datetime import datetime, timezone

import boto3
import pandas as pd


def get_batch_job_details(client, job_id):
    response = client.describe_jobs(jobs=[job_id])
    return response


def get_container_environment_variables(client, container_instance_arn):
    response = client.describe_container_instances(
        cluster="pipeline-batch-env-v2_Batch_78ba5903-b444-3205-b670-a262825169f9",
        containerInstances=[container_instance_arn],
    )
    container_instance = response["containerInstances"][0]

    environment_variables = {}
    for variable in container_instance["attributes"]:
        if variable["name"] == "ecs.agent.task-environment":
            environment_variables = {
                item["name"]: item["value"] for item in variable["value"]
            }

    return environment_variables


def main():
    session = boto3.Session(profile_name="prod", region_name="us-east-1")
    aws_batch_client = session.client("batch")
    # Get a list of completed jobs
    nextToken = ""
    job_ids = []
    while True:
        response = aws_batch_client.list_jobs(
            jobQueue="pipeline-batch-queue", jobStatus="SUCCEEDED", nextToken=nextToken
        )
        nextToken = response["nextToken"]
        job_ids.extend([job["jobId"] for job in response["jobSummaryList"]])
        if datetime.fromtimestamp(
            response["jobSummaryList"][0]["startedAt"] / 1000
        ) > datetime(2024, 1, 12, 3, 42, 21, 787000):
            print("going next")
            break
    job_details_list = []

    for job_id in job_ids:
        job_details_response = get_batch_job_details(aws_batch_client, job_id)
        if not job_details_response["jobs"]:
            print("not found")
            print(job_details_response["jobs"])
            continue
        job_details = job_details_response["jobs"][0]
        start_time = job_details["startedAt"]
        end_time = job_details["stoppedAt"]
        duration = end_time - start_time
        job_env_list = job_details_response["jobs"][0]["container"]["environment"]
        job_env_dict = {item["name"]: item["value"] for item in job_env_list}

        job_details_list.append(
            {
                "JobId": job_id,
                "StartDate": datetime.fromtimestamp(start_time / 1000),
                "Duration": duration / 1000,
                **job_env_dict
                # "environment_variables": environment_variables,
            }
        )
    df = pd.DataFrame(job_details_list)

    # Specify the CSV file path
    csv_file_path = "batch_job_details_pandas.csv"

    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)


if __name__ == "__main__":
    main()
