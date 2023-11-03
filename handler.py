import boto3
import uuid
import os
import face_recognition
import pickle
from urllib.parse import unquote_plus
from boto3.dynamodb.conditions import Attr
import shutil
from decimal import *
import pandas as pd

INPUT_BUCKET = "inputbucket01"
OUTPUT_BUCKET = "classificationresultsnucket"
ENCODING_FILE_KEY = "encoding"
TABLE = "student_table"
print('Loading function')

s3_client = boto3.client('s3')
dynamo_client = boto3.resource('dynamodb')


# Function to query dynamo db and get academic information in JSON format
def get_info_from_dynamo(query):  # takes a query parameter
    table = dynamo_client.Table(TABLE)
    try:
        response = table.scan(FilterExpression=Attr('name').eq(
            query))  # scans the table where the 'name' attribute is equal to the provided query
    except Exception as e:
        print("DynamoDB query failed ", e)
    return response


# Function to form a CSV file and upload it to s3 output bucket
def create_csv_file(student_info, file_name):
    print(student_info)
    csv_file_path = '/tmp/{}.csv'.format(file_name)
    try:
        student_data = {key: [value] for key, value in student_info['Items'][0].items() if
                        key in ['name', 'major', 'year']}
        dataframe = pd.DataFrame(student_data)
        dataframe = dataframe[['name', 'major', 'year']]
        dataframe = dataframe.reset_index(drop=True)
        dataframe.to_csv(csv_file_path, index=False)
    except Exception as e:
        print('Cannot read student information from the table ', e)
    return csv_file_path


# Function to remove the file extension '.mp4' from the file name
def strip_file_name(fileName):
    stripped_name = fileName.rsplit('.', 1)
    return stripped_name[0]


# Function to upload the csv file generated above to s3
def upload_to_s3(upload_path, key):
    key = key + '.csv'
    s3_client.upload_file(upload_path, '{}'.format(OUTPUT_BUCKET), key)


# Function to read the 'encoding' file
def open_encoding(filename):
    file = open(filename, "rb")
    data = pickle.load(file)
    file.close()
    return data


# Function to extract frames from the video
# input_file_path: The path to the input video file.
# upload_path: The path where the extracted frames will be stored.
def extract_frame_from_video(input_file_path, upload_path, file_name):
    output_dir = os.path.join(upload_path, file_name)
    # remove the existing output directory (if exists) and create a new one
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        os.mkdir(output_dir)
    else:
        os.makedirs(output_dir)
    try:
        # extract frames from the input video and save them as JPEG images in the output directory
        os.system("ffmpeg -i " + str(input_file_path) + " -r 1 " + str(output_dir + '/') + "image-%3d.jpeg")
    except Exception as e:
        print('Frame extraction failed ', e)


# Function to match the query image with the encodings, and perform face recognition on the query image
def recognize_face(query_image_path):
    query_picture = face_recognition.load_image_file(query_image_path)
    # The face_encodings function returns a list of face encodings, but we only consider the first one ([0]).
    query_image_encoding = face_recognition.face_encodings(query_picture)[0]
    encodings = open_encoding("/home/app/encoding")
    try:
        obj_names = encodings['name']
        obj_encoding = encodings['encoding']
    except Exception as e:
        print('Could not find encoding data ', e)

    for name, encoding in zip(obj_names, obj_encoding):
        # use the compare_faces function from the face_recognition library to compare the encoding of the known face
        # with the encoding of the query image
        result = face_recognition.compare_faces([encoding], query_image_encoding)
        if result[0] == True:
            return name
        else:
            continue


# Lambda function handler
def face_recognition_handler(event, context):
    print("event", event)
    print("context", context)
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_name = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), file_name)
        file_name = strip_file_name(file_name)
        upload_path = '/tmp/{}'.format(OUTPUT_BUCKET)
        s3_client.download_file(bucket, key, download_path)
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                key, bucket))
        raise e

    extract_frame_from_video(download_path, upload_path, file_name)

    extracted_image_path = os.path.join(upload_path, file_name)
    image_frames = os.listdir(extracted_image_path)

    subject_name = recognize_face(os.path.join(extracted_image_path, image_frames[0]))
    student_info = get_info_from_dynamo(subject_name)
    output_file_path = create_csv_file(student_info, file_name)

    try:
        response = upload_to_s3(output_file_path, file_name)
    except Exception as e:
        print('Could not push CSV file to s3 ', e)

    return {
        'message': "Done!"
    }
