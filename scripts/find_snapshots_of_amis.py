import json

import boto3


def get_collibra_amis():
    # Set your AWS credentials and region
    session = boto3.Session(profile_name="prod", region_name="us-east-1")

    ec2 = session.client("ec2")

    # Get a list of images (AMIs)
    # Get a list of images (AMIs)
    response = ec2.describe_images(Filters=[{"Name": "name", "Values": ["*collibra*"]}])

    # Define JSON file path
    json_file_path = "collibra_amis_snapshots.json"

    # Prepare data for JSON
    result_data = []

    for image in response["Images"]:
        image_info = {
            "ImageId": image["ImageId"],
            "Name": image["Name"],
            "Snapshots": [],
        }

        for block_device_mapping in image["BlockDeviceMappings"]:
            try:
                snapshot_id = block_device_mapping["Ebs"]["SnapshotId"]
                snapshot_info = ec2.describe_snapshots(SnapshotIds=[snapshot_id])

                snapshot_data = {
                    "SnapshotId": snapshot_id,
                    "Description": snapshot_info["Snapshots"][0]["Description"],
                    "VolumeId": snapshot_info["Snapshots"][0]["VolumeId"],
                    "StartTime": str(snapshot_info["Snapshots"][0]["StartTime"]),
                }

                image_info["Snapshots"].append(snapshot_data)
            except Exception as e:
                print(image["BlockDeviceMappings"], e)
        result_data.append(image_info)

    # Write AMI and snapshot information to JSON
    with open(json_file_path, "w") as json_file:
        json.dump(result_data, json_file, indent=2)


if __name__ == "__main__":
    get_collibra_amis()
