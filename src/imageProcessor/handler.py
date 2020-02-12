import boto3
import botocore
import re
from PIL import Image
import os

def handler(message, context):
    print(message)

    record = message['Records'][0]
    if (record['eventName'] != 'ObjectCreated:Put'):
        print("Event is %s, ignoring" % record['eventName'])
        return {}

    sourceBucket = record['s3']['bucket']['name']
    imageName = record['s3']['object']['key']

    # Only operate on JPG files
    if (not re.match(r'.*\.jpg$', imageName)):
        return {}

    s3 = boto3.resource('s3')

    imagePath = "/tmp/%s" % imageName
    print("Retrieving image %s from ObjectStore 'Uploaded Images'" % imageName)
    try:
        s3.Bucket(sourceBucket).download_file(imageName, imagePath)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object %s does not exist in bucket %s", (sourceBucket, imageName))
        else:
            raise


    print("Creating thumbnail from image")
    thumbnailImage = "200x200-%s" % imageName
    thumbnailPath = "/tmp/%s" % thumbnailImage
    size = 200, 200

    im = Image.open(imagePath)
    im.thumbnail(size)
    im.save(thumbnailPath)


    print("Storing thumbnail %s to ObjectStore 'Processed Images'" % thumbnailPath)
    targetBucket = os.environ['BUCKET_NAME']
    try:
        s3.Bucket(targetBucket).upload_file(thumbnailPath, thumbnailImage)
    except botocore.exceptions.ClientError as e:
        print("Error uploading %s to bucket %s" % (thumbnailImage, targetBucket))
        raise

    return {}
