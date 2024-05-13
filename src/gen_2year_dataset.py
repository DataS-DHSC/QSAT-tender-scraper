# %%
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

import src.xml_functions as xml_fn

base_dir = Path(os.getcwd())  # .parents[0]
xml_data_folder = base_dir / "input/data/zip_data"

logging.basicConfig(
    # filename=base_dir / "outputs/tweets/tweet_tagging.log",
    encoding="utf-8",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(base_dir / "output/tender_scraper.log"),
        logging.StreamHandler(),
    ],
)

start_date = datetime.strptime("2022-01", "%Y-%m")
end_date = datetime.strptime("2024-04", "%Y-%m")

logging.info(
    "-------- -Starting run bewteen %s and %s ---------" % (start_date, end_date)
)


# %% get harvest url


harvest_url = "https://www.find-tender.service.gov.uk/harvester/notices/json"
response = requests.get(harvest_url, timeout=5)
bad_urls = []
#
filtered_response = [
    x
    for x in response.json()
    if datetime.strptime(x["issued"], "%Y-%m") >= start_date
    and datetime.strptime(x["issued"], "%Y-%m") < end_date + timedelta(minutes=1)
]

for month_data in filtered_response:
    logging.info("------ Downloading data from %s ------ " % (month_data))
    for day in month_data["distribution"]:
        print(f"------ Downloading {day} ------ ")
        bad_urls = xml_fn.download_zip(xml_data_folder, day["downloadURL"], bad_urls)
logging.info("------ All zip files processed ------")

print(bad_urls)
# %%
logging.info("------ Starting XML extraction  ------")

all_data = pd.DataFrame()
for xml_file in xml_data_folder.glob("*.xml"):
    tree = ET.iterparse(xml_file)
    root = xml_fn.remove_namespace(tree)
    if root.find(".//CPV_MAIN/CPV_CODE") is not None:
        cpv_code = root.find(".//CPV_MAIN/CPV_CODE").get("CODE")
        if cpv_code[0:3] == "851":
            file_data = xml_fn.extract_data(
                root, xml_file_path=xml_file, cpv_code=cpv_code
            )
            all_data = pd.concat([all_data, file_data])
            all_data.reset_index(inplace=True, drop=True)

logging.info("------ XML extraction finished ------")
all_data.to_csv(base_dir / "output/tender_data_jan22_apr24_v3.csv", index=False)
#
# %%
