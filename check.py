import os
import sys

from boto3 import client as boto3_client
from dotenv import dotenv_values
import pandas as pd

input_bucket = "cloudspades-input-bucket"
output_bucket = "cloudspades-output-bucket"
CONFIG = dotenv_values()

with open("mapping", "r") as fp:
    data = fp.read()

mapping = {

}

s3_client = boto3_client('s3', aws_secret_access_key=CONFIG.get('AWS_SECRET_ACCESS_KEY'),
                         aws_access_key_id=CONFIG.get('AWS_ACCESS_KEY_ID'),
                         region_name=CONFIG.get('AWS_DEFAULT_REGION'))

for line in data.split("\n"):
    file_name = line.strip().split(":")[0].rsplit('.', 1)[0]
    mapping[file_name] = {
        "major": line.strip().split(":")[1].split(",")[0],
        "year": line.strip().split(":")[1].split(",")[1]
    }


def test_video(test_dir):
    correct = 0
    wrong = 0
    for filename in os.listdir(test_dir):
        if filename.endswith(".mp4") or filename.endswith(".MP4"):
            f = filename.rsplit('.', 1)[0]
            try:
                s3_client.download_file(output_bucket, f, "/tmp/file.csv")
                df = pd.read_csv("/tmp/file.csv")
                if mapping[f]["major"] == df.iloc[0]["major"] and mapping[f]["year"] == df.iloc[0]["year"]:
                    correct += 1
                else:
                    wrong += 1
                    print(df)
                    print(mapping[f]["major"], mapping[f]["major"])
                    break
                print("Correct: %s, Wrong: %s" % (correct, wrong))
            except Exception as e:
                print("%s not found" % file_name)
                continue


test_video(sys.argv[1])
