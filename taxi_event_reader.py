import boto3
from boto3 import session

region = "us-east-1"
bucket_name = "aws-bigdata-blog"
object_prefix = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"

session_ = session.Session()

s3_resource = session_.resource("s3", region_name=region)
bucket = s3_resource.Bucket(bucket_name)

# for obj in bucket.objects.filter(Prefix=object_prefix):
#     print(obj.key)
    

for obj in bucket.objects.filter(Prefix=object_prefix):
    target = obj.key.split("/")[-1]
    if target:  # avoid downloading empty prefix
        print(f"Downloading {obj.key} to {target}")
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")
        print(f"File size: {obj.size / (1024 * 1024):.2f} MB")
