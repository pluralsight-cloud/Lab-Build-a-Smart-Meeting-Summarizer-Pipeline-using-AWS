import boto3
import os
import json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Second Lambda:
    Triggered by EventBridge when Transcribe job is COMPLETED.
    Fetches transcript from S3 and performs lightweight analysis.
    """

    # -----------------------------
    # Environment variables
    # -----------------------------
    s3_bucket = os.environ['S3_BUCKET']
    input_prefix = os.environ.get("INPUT_PREFIX", "output/transcripts")
    output_prefix = os.environ.get("OUTPUT_PREFIX", "output/analysis")

    # -----------------------------
    # Get job name from Transcribe event
    # -----------------------------
    try:
        job_name = event['detail']['TranscriptionJobName']
    except KeyError:
        print("Invalid event structure. Expected Transcribe event.")
        print(json.dumps(event))
        return {
            "statusCode": 400,
            "body": "Invalid event structure"
        }

    # -----------------------------
    # Fetch transcript from S3
    # -----------------------------
    transcript_key = f"{input_prefix}/{job_name}.json"
    transcript_s3_path = f"s3://{s3_bucket}/{transcript_key}"
    print(f"Fetching transcript from: {transcript_s3_path}")

    try:
        response = s3.get_object(Bucket=s3_bucket, Key=transcript_key)
        transcript_json = json.loads(response['Body'].read())
    except Exception as e:
        print(f"Error fetching transcript from {transcript_s3_path}: {str(e)}")
        return {
            "statusCode": 500,
            "body": "Failed to fetch transcript"
        }

    # -----------------------------
    # Extract transcript text
    # -----------------------------
    transcript_text = (
        transcript_json
        .get('results', {})
        .get('transcripts', [{}])[0]
        .get('transcript', '')
    )

    # -----------------------------
    # Lightweight analysis (Option B)
    # -----------------------------
    words = transcript_text.split()
    word_count = len(words)

    # Number of questions in transcript
    num_questions = transcript_text.count('?')

    # Number of times "AI" appears
    num_ai_mentions = transcript_text.lower().count('ai')

    # Average word length
    avg_word_len = sum(len(w) for w in words) / word_count if word_count else 0

    # -----------------------------
    # Construct analysis results
    # -----------------------------
    analysis_results = {
        "job_name": job_name,
        "word_count": word_count,
        "num_questions": num_questions,
        "num_ai_mentions": num_ai_mentions,
        "avg_word_length": round(avg_word_len, 2),
        "original_transcript_snippet": transcript_text[:500]  # first 500 chars
    }

    # -----------------------------
    # Save analysis to S3
    # -----------------------------
    analysis_key = f"{output_prefix}/{job_name}_analysis.json"
    analysis_s3_path = f"s3://{s3_bucket}/{analysis_key}"

    print(f"Output analysis will be saved to: {analysis_s3_path}")

    try:
        s3.put_object(
            Bucket=s3_bucket,
            Key=analysis_key,
            Body=json.dumps(analysis_results, indent=2),
            ContentType="application/json"
        )
    except Exception as e:
        print(f"Error saving analysis to {analysis_s3_path}: {str(e)}")
        return {
            "statusCode": 500,
            "body": "Failed to save analysis"
        }

    print(f"Successfully processed transcript.")
    print(f"Analysis saved to: {analysis_s3_path}")

    return {
        "statusCode": 200,
        "body": f"Processed transcript and saved analysis to {analysis_s3_path}"
    }
