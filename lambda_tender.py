import datetime
import json
import logging
import xml.etree.ElementTree as ET  # nosec
from pathlib import Path

import awswrangler as wr
import defusedxml
import pandas as pd
import requests

import src.xml_functions as xml_fn

# %%
defusedxml.defuse_stdlib()


def handler():
    harvest_url = "https://www.find-tender.service.gov.uk/harvester/notices/json"
    response = requests.get(harvest_url, timeout=5)
    bad_urls = []

    xml_data_folder = Path("/tmp")  # nosec

    if datetime.date.today().day == 7:
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
                    xml_data_folder, day["downloadURL"], bad_urls
                )

        output = pd.DataFrame()
        for xml_file in xml_data_folder.glob("*.xml"):
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
