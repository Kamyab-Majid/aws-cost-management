import json
from datetime import datetime

import boto3
import pandas as pd
from pandas.errors import EmptyDataError


def get_kms_decrypted_events():
    try:
        df = pd.read_csv("cloudtrail_events.csv")
    except (EmptyDataError , FileNotFoundError):
        df = pd.DataFrame()
        print("empty file probably first time running the query")
    # Initialize a CloudTrail client
    session = boto3.Session(profile_name="prod", region_name="us-east-1")
    cloudtrail_client = session.client("cloudtrail")

    # Set the name of your CloudTrail trail
    # trail_name = "your-trail-name"

    # Set the start and end time for the event lookup
    start_time = datetime(2023, 1, 1)
    end_time = datetime(2023, 12, 31)

    # Set the attribute for filtering the events
    lookup_attributes = [
        {"AttributeKey": "EventName", "AttributeValue": "Decrypt"},
        {"AttributeKey": "EventSource", "AttributeValue": "kms.amazonaws.com"},
    ]

    # Get the events based on the lookup attributes

    # Create a list to store event details
    event_details_list = []
    response = cloudtrail_client.lookup_events(
        LookupAttributes=lookup_attributes,
        StartTime=start_time,
        EndTime=end_time,
        MaxResults=216000,  # Adjust the number of events you want to retrieve
        NextToken="CoxYY8ohAOIIfoqNdDR0N+qMvpTUfLiXsOFbxDcq01wWFFRqjwVZ2Z19/6faaDtk",
    )
    next_token = response["NextToken"]
    for i in range(100000):
        for event in response["Events"]:
            # Convert the datetime object to timezone-unaware
            event["EventTime"] = event["EventTime"].replace(tzinfo=None)
            cloudtrail_event_dict = json.loads(event.get("CloudTrailEvent", "{}"))
            # Create a dictionary to store details for each event
            event_details = {**event, **cloudtrail_event_dict}
            # print(event_details["Username"])
            # The line `if "@" not in event_details["Username"] or not event_details["Username"]:` is
            # checking if the value of the "Username" key in the `event_details` dictionary does not
            # contain the "@" symbol or if it is an empty string.
            if event_details["Username"] in df.index:
                df.loc[event_details["Username"], "decrypt_count"] += 1
            else:
                df.loc[event_details["Username"], "decrypt_count"] = 1
            if event_details["userIdentity"]["principalId"] in df.index:
                df.loc[
                    event_details["userIdentity"]["principalId"], "decrypt_count"
                ] += 1
            else:
                df.loc[
                    event_details["userIdentity"]["principalId"], "decrypt_count"
                ] = 1

            # Append the event details dictionary to the list
            # event_details_list.append(event_details)
            # if event_details["Username"] in df.index:
            #     df.loc[event_details["Username"], "decrypt_count"] += 1
            # else:
            #     df.loc[event_details["Username"], "decrypt_count"] = 1

        # Create a Pandas DataFrame from the list of event details
        # df = pd.DataFrame(event_details_list)

        # Save the DataFrame to an Excel file
        csv_filename = "cloudtrail_events.csv"
        df.to_csv(csv_filename, index=True, header=False)
        next_token = response["NextToken"]
        df["token,token"] = next_token
        df["token,EventTime"] = event["EventTime"].replace(tzinfo=None)
        print(f"CloudTrail events saved to {csv_filename}")
        response = cloudtrail_client.lookup_events(
            LookupAttributes=lookup_attributes,
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=216000,  # Adjust the number of events you want to retrieve
            NextToken=next_token,
        )


if __name__ == "__main__":
    get_kms_decrypted_events()
