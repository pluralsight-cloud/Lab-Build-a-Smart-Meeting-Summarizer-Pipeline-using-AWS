import boto3
import os
import urllib.parse
import time

transcribe = boto3.client('transcribe')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        job_name = key.replace('/', '_').replace('.', '_') + '_' + str(int(time.time()))

        job_uri = f's3://{bucket}/{key}'
        output_bucket = os.environ['OUTPUT_BUCKET']
        output_prefix = os.environ.get("OUTPUT_PREFIX", "output")

        output_key = f"{output_prefix}/transcripts/{job_name}.json"

        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat=key.split('.')[-1],  # assumes file extension is audio format
            LanguageCode='en-US',
            OutputBucketName=output_bucket,
            OutputKey=output_key
        )

        print(f"Started transcription job: {job_name}")

    return {
        'statusCode': 200,
        'body': f'Transcription job {job_name} started.'
    }
