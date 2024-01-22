import csv
from datetime import datetime, timedelta

import boto3


def get_last_month_dates():
    today = datetime.utcnow()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)

    return first_day_of_current_month, today


def get_glue_job_stats(start_date, end_date):
    session = boto3.Session(profile_name="prod", region_name="us-east-1")
    glue_client = session.client("glue")

    response = glue_client.get_jobs()

    job_stats = []

    for job in response["Jobs"]:
        job_name = job["Name"]

        # Get job run statistics
        job_runs = glue_client.get_job_runs(JobName=job_name, MaxResults=200)

        for run in job_runs["JobRuns"]:
            run_start_time = run["StartedOn"]
            if (
                start_date.date() <= run_start_time.date()
                and run["JobRunState"] == "SUCCEEDED"
            ):
                
                run_end_time = run["CompletedOn"]

                # Check if the run is within the specified date range

                # Calculate DPU hours (you can customize this based on your needs)
                if "DPUSeconds" in run:
                    dpu_seconds = run["DPUSeconds"]
                elif "AllocatedCapacity" in run:
                    dpu_seconds = run["ExecutionTime"] * run["AllocatedCapacity"]
                else:
                    dpu_seconds = run["ExecutionTime"] * run["MaxCapacity"]
                ExecutionClass = (
                    run["ExecutionClass"] if "ExecutionClass" in run else "g1"
                )

                job_stats.append(
                    {
                        "JobName": job_name,
                        "RunId": run["Id"],
                        "StartTime": run_start_time,
                        "EndTime": run_end_time,
                        "DPUHours": dpu_seconds,
                        "NumberOfWorkers": run["NumberOfWorkers"],
                        "WorkerType": run["WorkerType"],
                        "ExecutionClass": ExecutionClass,
                        # Add other stats as needed
                    }
                )
                # print(run["WorkerType"])

    return job_stats


def write_to_csv(job_stats, csv_filename):
    fields = list(job_stats[0].keys())

    with open(csv_filename, mode="w", newline="") as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=fields)

        csv_writer.writeheader()
        csv_writer.writerows(job_stats)


if __name__ == "__main__":
    first_day_of_current_month, today = get_last_month_dates()

    job_stats = get_glue_job_stats(first_day_of_current_month, today)

    csv_filename = "glue_job_stats_last_month.csv"
    write_to_csv(job_stats, csv_filename)

    print(f"Job statistics have been written to {csv_filename}.")
