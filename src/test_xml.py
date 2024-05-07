# %%
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# %%


def get_val(elem, query):
    if elem.find(query) is not None:
        val = elem.find(query).text
    else:
        val = np.nan
    return val


def get_total_value(elem):
    if elem.find(".//VALUES/VALUE") is not None:
        total_value = elem.find(".//VALUES/VALUE").text
    elif elem.find(".//VALUES/VAL_TOTAL") is not None:
        total_value = elem.find(".//VALUES/VAL_TOTAL").text
    else:
        total_value = "no_info"
    return total_value


def get_is_sme(elem):
    if elem.find(".//SME") is not None:
        is_sme = True
    elif elem.find(".//NO_SME") is not None:
        is_sme = False
    else:
        is_sme = np.nan
    return is_sme


def get_is_awarded_group(elem):
    if elem.find(".//CONTRACTORS/AWARDED_TO_GROUP") is not None:
        awarded_to_group = True
    elif elem.find(".//CONTRACTORS/NO_AWARDED_TO_GROUP") is not None:
        awarded_to_group = False
    else:
        awarded_to_group = np.nan
    return awarded_to_group


def get_description(elem):
    if len(elem.findall(".//SHORT_DESCR/P")) > 0:
        text = ""
        for p in elem.findall(".//SHORT_DESCR/P"):
            if p.text:
                text += p.text
    return text


def get_is_awarded(elem):
    if elem.find(".//AWARD_CONTRACT/AWARDED_CONTRACT") is not None:
        is_awarded = True
    else:
        is_awarded = "no_info"
    return is_awarded


def get_n_contracts(elem):
    if elem.findall(".//AWARD_CONTRACT"):
        n_contracts = len(elem.findall(".//AWARD_CONTRACT"))
    else:
        n_contracts = np.nan
    return n_contracts


def get_values(elem):
    if elem.find(".//VALUES/VAL_TOTAL") is not None:
        total_value = elem.find(".//VALUES/VAL_TOTAL").text
    elif elem.find(".//VALUES/VALUE") is not None:
        total_value = elem.find(".//VALUES/VALUE").text
    else:
        total_value = np.nan
    if elem.find(".//VAL_RANGE_TOTAL") is not None:
        low_value = elem.find(".//VAL_RANGE_TOTAL//LOW").text
        high_value = elem.find(".//VAL_RANGE_TOTAL//HIGH").text
    else:
        low_value = np.nan
        high_value = np.nan

    return total_value, low_value, high_value


def get_date_pub(elem):
    if elem.find(".//CODED_DATA_SECTION/REF_OJS/DATE_PUB") is not None:
        date = datetime.strptime(
            elem.find(".//CODED_DATA_SECTION/REF_OJS/DATE_PUB").text, "%Y%m%d"
        )
        date_formatted = date.strftime("%d/%m/%Y")
    else:
        date_formatted = np.nan
    return date_formatted


# date


# %%
def remove_namespace(input):
    for _, el in input:
        _, _, el.tag = el.tag.rpartition("}")
    root = input.root
    return root


base_dir = Path(os.getcwd()).parents[0]
xml_files = []
xml_fodler = base_dir / "input/data/zip_data/"

bad_files = {}
c = 0
data = pd.DataFrame()

for xml_file_path in xml_fodler.glob("*.xml"):
    tree = ET.iterparse(xml_file_path)
    root = remove_namespace(tree)
    if root.find(".//CPV_MAIN/CPV_CODE") is not None:
        cpv_code = root.find(".//CPV_MAIN/CPV_CODE").get("CODE")
        if cpv_code[0:3] == "851":

            print("851")
            if root.find(".//AWARD_CONTRACT/AWARDED_CONTRACT") is not None:
                print(root.find(".//AWARD_CONTRACT/AWARDED_CONTRACT"))
                for contract in root.findall(".//AWARD_CONTRACT"):
                    for provider in contract.findall(".//CONTRACTORS/CONTRACTOR"):
                        print(c)
                        data.loc[c, "date_publised"] = get_date_pub(root)
                        data.loc[c, "cpv_code"] = cpv_code
                        data.loc[c, "contract_title"] = get_val(
                            root, ".//OBJECT_CONTRACT/TITLE/P"
                        )
                        #                                     './/TITLE/P')
                        data.loc[c, "link"] = root.find(".//URI_LIST/URI_DOC").text

                        data.loc[c, "comissoner_name"] = get_val(
                            root, ".//ADDRESS_CONTRACTING_BODY/OFFICIALNAME"
                        )

                        data.loc[c, "comissioner_type"] = get_val(
                            root, ".//CODIF_DATA/AA_AUTHORITY_TYPE"
                        )
                        data.loc[c, "procurement"] = get_val(
                            root, ".//CODIF_DATA/PR_PROC"
                        )

                        data.loc[c, "n_bids"] = get_val(
                            contract, ".//NB_TENDERS_RECEIVED"
                        )

                        data.loc[c, "n_sme_bids"] = get_val(
                            contract, ".//NB_TENDERS_RECEIVED_SME"
                        )

                        data.loc[
                            c, ["total_value", "value_range_low", "value_range_high"]
                        ] = get_values(contract)
                        data.loc[c, "provider_name"] = get_val(
                            provider, ".//OFFICIALNAME"
                        )

                        data.loc[c, "provider_type"] = get_val(
                            root, ".//NOTICE_DATA/ORIGINAL_CPV"
                        )
                        data.loc[c, "provider_is_sme"] = get_is_sme(provider)

                        data.loc[c, "awarded_to_group"] = get_is_awarded_group(contract)

                        data.loc[c, "file_name"] = str(xml_file_path).rpartition(
                            "zip_data\\"
                        )[2]
                        c += 1


# %%


# %%
