import boto3
from datetime import datetime, timedelta
import json
# Initialize Boto3 clients for CloudTrail and EC2
session = boto3.Session(profile_name="sand", region_name="us-east-1")

cloudtrail_client = session.client('cloudtrail')
ec2_client = session.client('ec2')

# Get a list of all EC2 instances
ec2_instances = ec2_client.describe_instances()
all_ec2_instance_ids = set([instance['InstanceId'] for reservation in ec2_instances['Reservations'] for instance in reservation['Instances']])
events_exist_response = {}
no_response_ec2 = {}
# Define the time range for the last month
end_time = datetime.now()
start_time = end_time - timedelta(days=90)

# Set up your CloudTrail event query
for ec2_id in all_ec2_instance_ids:
    response = cloudtrail_client.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'ResourceName',  # Look for events with matching resource name
                'AttributeValue': ec2_id  # List of all EC2 instance IDs
            }
        ],
        StartTime=start_time,
        EndTime=end_time
    )
    if response['Events']:
        # Events exist for this EC2 instance
        events_exist_response[ec2_id] = response
    else:
        # No events found for this EC2 instance
        no_response_ec2[ec2_id] = "No events found"

# Save responses in JSON files
with open("events_exist_response.json", "w") as events_file:
    json.dump(events_exist_response, events_file, indent=4)

with open("no_response_ec2.json", "w") as no_response_file:
    json.dump(no_response_ec2, no_response_file, indent=4)
# Extract EC2 instance IDs from the CloudTrail events
# used_ec2_instance_ids = set()
# for event in response['Events']:
#     for event_detail in event['CloudTrailEvent']:
#         if 'EC2' in event_detail and 'instanceId' in event_detail['EC2']:
#             used_ec2_instance_ids.add(event_detail['EC2']['instanceId'])

# # Find EC2 instances that haven't been used in the last month
# unused_ec2_instances = all_ec2_instance_ids - used_ec2_instance_ids

# print("Unused EC2 instances:")
# for instance_id in unused_ec2_instances:
#     print(instance_id)
