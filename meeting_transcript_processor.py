import boto3
import json
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Environment variables
    output_bucket = os.environ['OUTPUT_BUCKET']
    output_prefix = os.environ.get("OUTPUT_PREFIX", "output")

    # Get the transcription job name from the EventBridge event
    job_name = event['detail']['TranscriptionJobName']

    # Match the same output key pattern used by the first Lambda
    transcript_key = f"{output_prefix}/transcripts/{job_name}.json"

    print(f"Fetching transcript from s3://{output_bucket}/{transcript_key}")

    # Fetch the transcript from S3
    response = s3.get_object(
        Bucket=output_bucket,
        Key=transcript_key
    )

    transcript_data = json.loads(response['Body'].read())

    print(f"Transcript retrieved for job: {job_name}")
    print(json.dumps(transcript_data, indent=2))

    # Future processing goes here:
    # - summarization
    # - action item extraction
    # - sentiment analysis
    # - persistence (DynamoDB, OpenSearch, etc.)

    return {
        'statusCode': 200,
        'body': f'Transcript processed for job {job_name}.'
    }
