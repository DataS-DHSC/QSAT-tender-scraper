import datetime
import json
import logging
import os
import xml.etree.ElementTree as ET  # nosec
from pathlib import Path

import awswrangler as wr
import defusedxml
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

import src.xml_functions as xml_fn

# %%


load_dotenv()
CONN_STR = os.environ["CONN_STRING"]
container = "stiappautdevuks001 "
bsc = BlobServiceClient.from_connection_string(CONN_STR)
fn = "tender-test"
fbn = f"{fn}/"
fbc = bsc.get_blob_client(container=container, blob=fbn)
if not fbc.exists():
    fbc.upload_blob("", overwrite=False)
f = "test.csv"
bc = bsc.get_blob_client(container=container, blob=f"{fn}/{f}")
d = pd.DataFrame({"var1": ["val1", "val2"], "var2": ["val3", "val4"]})
d.to_csv(f, index=False)
with open(f, "rb") as info:
    bc.upload_blob(info)


# %%
def handler():
    defusedxml.defuse_stdlib()

    harvest_url = "https://www.find-tender.service.gov.uk/harvester/notices/json"
    response = requests.get(harvest_url, timeout=5)
    bad_urls = []

    XML_DATA_FOLDER = Path(os.environ["XML_DATA_FOLDER"])  # nosec

    if datetime.date.today().day == 8:
        end_date = datetime.date.today() - datetime.timedelta(1)
        start_date = datetime.datetime(end_date.year, end_date.month, 1)

        filtered_response = [
            x
            for x in response.json()
            if datetime.datetime.strptime(x["issued"], "%Y-%m") >= start_date
            and datetime.datetime.strptime(x["issued"], "%Y-%m")
            < end_date + datetime.timedelta(minutes=1)
        ]

        for month_data in filtered_response:
            # logging.info('------ Downloading data from %s ------ ' % (month_data))
            for day in month_data["distribution"]:
                print(f"------ Downloading {day} ------ ")
                bad_urls = xml_fn.download_zip(
                    XML_DATA_FOLDER, day["downloadURL"], bad_urls
                )

        output = pd.DataFrame()
        for xml_file in XML_DATA_FOLDER.glob("*.xml"):
            tree = ET.iterparse(xml_file)  # nosec
            root = xml_fn.remove_namespace(tree)
            if root.find(".//CPV_MAIN/CPV_CODE") is not None:
                cpv_code = root.find(".//CPV_MAIN/CPV_CODE").get("CODE")
                if cpv_code[0:3] == "851":
                    file_data = xml_fn.extract_data(
                        root, xml_file_path=xml_file, cpv_code=cpv_code
                    )
                    output = pd.concat([output, file_data])
                    output.reset_index(inplace=True, drop=True)

        logging.info("------ XML extraction finished ------")

        month = start_date.strftime("%B")
        wr.s3.to_csv(df=output, path=f"s3://qsat-tender-data/tender_data_{month}.csv")
        #
        # client = boto3.client("sns")

        # snsArn = "arn:aws:sns:eu-west-2:335923355498:PreReleaseStats"

        # url = boto3.client("s3").generate_presigned_url(
        #     ClientMethod="get_object",
        #     Params={"Bucket": "qsat-tender-data", "Key": f"tender_data_{month}.csv"},
        #     ExpiresIn=86400,
        # )
        # message = f"A New Tender Dataset is Available at {url}"

        # _ = client.publish(  # pylint: disable=unused-variable
        #     TopicArn=snsArn,
        #     Message=message,
        #     Subject="A New Prerelease Stats Dataset Is Available [ACCESS TO FILE ENDS AFTER 24 HOURS]", # noqa: E501
        # )

        return {"statusCode": 200, "body": json.dumps("Hello from Lambda")}


# %%
