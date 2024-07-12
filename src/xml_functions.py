import io
import logging
import os
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd
import requests


def extract_data(root, xml_file_path, cpv_code):
    data = pd.DataFrame()
    c = 0
    for contract in root.findall(".//AWARD_CONTRACT"):
        for provider in contract.findall(".//CONTRACTORS/CONTRACTOR"):
            data.loc[c, "date_publised"] = get_date_pub(root)
            data.loc[c, "cpv_code"] = cpv_code
            data.loc[c, "contract_title"] = get_val(root, ".//OBJECT_CONTRACT/TITLE/P")
            data.loc[c, "link"] = root.find(".//URI_LIST/URI_DOC").text
            data.loc[c, "comissoner_name"] = get_val(
                root, ".//ADDRESS_CONTRACTING_BODY/OFFICIALNAME"
            )
            data.loc[c, "comissioner_type"] = get_val(
                root, ".//CODIF_DATA/AA_AUTHORITY_TYPE"
            )
            data.loc[c, "procurement"] = get_val(root, ".//CODIF_DATA/PR_PROC")
            data.loc[c, "procument_details"] = get_proc_details(root)
            data.loc[c, "n_bids"] = get_val(contract, ".//NB_TENDERS_RECEIVED")
            data.loc[c, "n_sme_bids"] = get_val(contract, ".//NB_TENDERS_RECEIVED_SME")
            data.loc[c, ["total_value", "value_range_low", "value_range_high"]] = (
                get_values(contract)
            )
            data.loc[c, "provider_name"] = get_val(provider, ".//OFFICIALNAME")
            data.loc[c, "provider_type"] = get_val(root, ".//NOTICE_DATA/ORIGINAL_CPV")
            data.loc[c, "provider_is_sme"] = get_is_sme(provider)
            if len(root.findall(".//CONTRACTORS/CONTRACTOR")) < 1:
                data.loc[c, "awarded_to_group"] = True
            else:
                data.loc[c, "awarded_to_group"] = False
            data.loc[c, "complementary_info"] = get_free_text(
                root, ".//COMPLEMENTARY_INFO/INFO_ADD/P"
            )
            data.loc[c, "file_name"] = str(xml_file_path).rpartition("zip_data\\")[2]
            c += 1
    return data


def remove_namespace(inputs):
    for _, el in inputs:
        _, _, el.tag = el.tag.rpartition("}")
    root = inputs.root
    return root


def title_to_snakecase(string):
    output = string.lower().replace(" ", "_").replace("-", "")
    return output


def download_zip(output_folder, url, bad_urls):
    try:
        zip_file = zipfile.ZipFile(io.BytesIO(requests.get(url, timeout=60).content))
        for file_info in zip_file.infolist():

            if not file_info.is_dir():
                file_data = zip_file.read(file_info)
                if not os.path.exists(output_folder / file_info.filename):
                    with open(output_folder / file_info.filename, "wb") as file:
                        file.write(file_data)
    except zipfile.BadZipfile:
        logging.error(f"------ PROBLEM WITH {url} bad zipfile ------")
        bad_urls += url
    return bad_urls


#
def get_proc_details(elem):
    free_texts = []
    for short_desc in elem.findall(".//OBJECT_DESCR/SHORT_DESCR"):
        short_desc_text = ""
        for p in short_desc.findall(".//P"):
            if p.text:
                short_desc_text = " ".join([short_desc_text, p.text])
                short_desc_text = short_desc_text.replace("\t", " ")
        free_texts += [short_desc_text]
    if free_texts:
        unique_texts = set(free_texts)
        output = ' '.join(unique_texts)
    else:
        output = np.nan
    return output


def get_val(elem, query):
    if len(elem.findall(query)) > 1:
        print(f"LEN {query}")

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
        is_sme = "no_info"
    return is_sme


# def get_description(elem):
def get_free_text(elem, query):
    text = ""
    if len(elem.findall(query)) > 0:
        # if len(elem.findall(".//SHORT_DESCR/P")) > 0:
        text = ""
        for p in elem.findall(query):
            if p.text:
                text += p.text
    if text:
        output = text
    else:
        output = np.nan
    return output


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
