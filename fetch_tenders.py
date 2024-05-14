import datetime
import json
import logging
import os
import xml.etree.ElementTree as ET  # nosec
from pathlib import Path

import awswrangler as wr
import boto3
import defusedxml
import dotenv
import pandas as pd
import requests

import xml_functions as xml_fn

dotenv.load_dotenv()


# %%
def handler(event, context):  # pylint: disable=unused-argument
    defusedxml.defuse_stdlib()

    harvest_url = "https://www.find-tender.service.gov.uk/harvester/notices/json"
    response = requests.get(harvest_url, timeout=5)
    bad_urls = []

    XML_DATA_FOLDER = Path(os.environ["XML_DATA_FOLDER"])  # nosec

    end_date = datetime.datetime.today() - datetime.timedelta(1)
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

    url = boto3.client("s3", region_name="eu-west-1").generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": "qsat-tender-data", "Key": f"tender_data_{month}.csv"},
        ExpiresIn=604799,
    )
    message = f"A New Tender Dataset is Available at {url}"

    _ = boto3.client(
        "sns", region_name="eu-west-1"
    ).publish(  # pylint: disable=unused-variable
        TopicArn="arn:aws:sns:eu-west-1:335923355498:qsat-tender-scraper",
        Message=message,
        Subject="A New Tender Dataset Is Available [ACCESS TO FILE ENDS AFTER 24 HOURS]",  # noqa: E501
    )

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda")}
