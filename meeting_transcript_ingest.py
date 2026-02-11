import boto3
import os
import urllib.parse
import time

transcribe = boto3.client('transcribe')

def lambda_handler(event, context):
    # Environment variables
    s3_bucket = os.environ['S3_BUCKET']
    output_prefix = os.environ.get("OUTPUT_PREFIX", "output")

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        # Create a unique transcription job name
        job_name = key.replace('/', '_').replace('.', '_') + '_' + str(int(time.time()))

        job_uri = f's3://{bucket}/{key}'
        output_key = f"{output_prefix}/transcripts/{job_name}.json"

        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat=key.split('.')[-1],  # assumes file extension is audio format
            LanguageCode='en-US',
            OutputBucketName=s3_bucket,
            OutputKey=output_key
        )

        print(f"Started transcription job: {job_name}")
        print(f"Input audio: s3://{bucket}/{key}")
        print(f"Output transcript will be saved to: s3://{s3_bucket}/{output_key}")
