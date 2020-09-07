import boto3

from common.settings import settings

CLOUDWATCH_REGION_NAME = "us-east-2"

cloudwatch = boto3.client("cloudwatch", region_name=CLOUDWATCH_REGION_NAME)


def cloudwatch_put_metric(metric_data):
    cloudwatch.put_metric_data(
        MetricData=metric_data, Namespace=f"toes/actblue-{settings.stage}"
    )
