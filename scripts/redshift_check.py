import boto3


def lambda_handler(event, context):
    # Replace 'your-cluster-identifier' with your actual Redshift cluster identifier
    redshift_cluster_identifier = "redshift-cluster-1"
    session = boto3.Session(profile_name="sand", region_name="us-east-1")

    # Replace 'your-sns-topic-arn' with your actual SNS topic ARN
    sns_topic_arn = (
        "arn:aws:sns:us-east-1:399450685648:redshift-schedule-check-test-majid"
    )

    # Create Redshift client
    redshift_client = session.client("redshift")

    # Check Redshift cluster status
    try:
        response = redshift_client.describe_clusters(
            ClusterIdentifier=redshift_cluster_identifier
        )
        cluster_status = response["Clusters"][0]["ClusterStatus"]

        if cluster_status != "available":
            # Redshift cluster is not running, send SNS message
            sns_client = boto3.client("sns")
            message = f"Redshift cluster {redshift_cluster_identifier} is not running. Current status: {cluster_status}"
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=message,
                Subject="redshift not running",
            )

            return {"statusCode": 200, "body": f"Notification sent: {message}"}
        elif cluster_status == "available":
            sns_client = boto3.client("sns")
            message = f"Redshift cluster {redshift_cluster_identifier} is not running. Current status: {cluster_status}"
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=message,
                Subject="redshift not running",
            )
            return {
                "statusCode": 200,
                "body": f"Redshift cluster {redshift_cluster_identifier} is running. Current status: {cluster_status}",
            }
    except Exception as e:
        # Handle exceptions
        return {
            "statusCode": 500,
            "body": f"Error checking Redshift cluster status: {str(e)}",
        }

