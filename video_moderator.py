import boto3
import time
import csv
import os
import tempfile
from datetime import datetime
from urllib.parse import urlparse, unquote

class VideoModerator:
    
    def __init__(self, video_key, s3_bucket,
                 access_key,
                 secret_access_key,
                 region_name = 'us-east-1',
                 threshold = 25):
        
        self.aws_access_key_id = access_key
        self.aws_secret_access_key = secret_access_key
        self.region_name = region_name
        
        self.s3_bucket = s3_bucket
        self.video_key = video_key
        self.moderation_threshold = threshold

        self.s3_client = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, 
                                      aws_secret_access_key=self.aws_secret_access_key, 
                                      region_name=self.region_name)
        
        self.rekognition_client = boto3.client('rekognition', aws_access_key_id=self.aws_access_key_id, 
                                          aws_secret_access_key=self.aws_secret_access_key, 
                                          region_name=self.region_name)

        self.output_csv_key = 'output/video_moderation_output.csv'
        self.csv_file = '/tmp/video_moderation_output.csv'
        
    def video_analysis(self):
        
        print(f"Analyzing {self.video_key}")

        # Start content moderation job
        response = self.rekognition_client.start_content_moderation(
            Video={
                'S3Object': {
                    'Bucket': self.s3_bucket,
                    'Name': self.video_key,
                }
            },
            MinConfidence=self.moderation_threshold,
        )

        # Get the JobId from the response
        job_id = response['JobId']
        print(f'Content moderation job started. JobId: {job_id}')

        # Wait for the job to complete
        while True:
            response = self.rekognition_client.get_content_moderation(JobId=job_id)
            job_status = response['JobStatus']

            print(f'Job status: {job_status}')

            if job_status in ['SUCCEEDED', 'FAILED']:
                break

            time.sleep(100)  # Wait for 100 seconds before checking the status again

        if job_status == 'SUCCEEDED':
            # Get the moderation labels
            moderation_labels = response.get('ModerationLabels', [])
            if moderation_labels:
                moderation_labels = [(label['ModerationLabel']['Name'],label['ModerationLabel']['Confidence'])  for 
                                     label in response.get('ModerationLabels', [])]
                response_status = "Objectionable items recognized! Content Not Approved!"
            else:
                moderation_labels = []
                response_status = "Content Approved!"
        else:
            moderation_labels = {}
            response_status = "Previous Job Failed - No Response Avaialable"

        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [self.video_key, job_id, job_status, response_status, moderation_labels, created_at]
        
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=self.output_csv_key)
            file_exists = True
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                file_exists = False
            else:
                raise e

        if file_exists:
            self.s3_client.download_file(self.s3_bucket, self.output_csv_key, self.csv_file)
        else:
            # If the file does not exist, create a new file with headers
            with open(self.csv_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['video_key', 'job_id', 'job_status', 'response_status', 
                                 'moderation_labels', 'created_at'])

        with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as temp_file:
            writer = csv.writer(temp_file)
            writer.writerow(row)

        with open(self.csv_file, 'a', newline='') as file, open(temp_file.name, 'r') as temp_file:
            file.write(temp_file.read())

        os.remove(temp_file.name)

        # Upload the updated CSV file to S3
        self.s3_client.upload_file(self.csv_file, self.s3_bucket, self.output_csv_key)
        print(f"Successfully analysed {self.video_key} and updated the output file!")


def extract_bucket_key(s3_url):
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc.split('.')[0]
    object_key = unquote(parsed_url.path.lstrip('/')).replace('+', ' ')
    return bucket_name, object_key


if __name__ == "__main__":
    
    # this will be passed through lambda function.
    video_key = "https://imo-videoanalysis-us-east-krishna.s3.amazonaws.com/Dont+Buy+An+iPhone+15+Pro+in+India!+shorts.mp4"
    s3_bucket, video_key = extract_bucket_key(video_key)
    
    access_key = ""
    secret_access_key = ""
    region_name = "us-east-1"

    video_moderator = VideoModerator(video_key = video_key, 
                                     s3_bucket = s3_bucket,
                                     access_key = access_key,
                                     secret_access_key = secret_access_key,
                                     region_name = region_name
                                    )
    video_moderator.video_analysis()
    