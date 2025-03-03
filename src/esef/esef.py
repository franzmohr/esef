import json
import os
import polars as pl
import requests

def create_directory_tree(dl_folder):
    """
    Creates the directory tree used by the module's functions.

    Parameters
    ----------
    dl_folder : str
        The path to the root directory of the data lake.
    """
    if not os.path.exists(dl_folder):
        print("Specified folder does not exist.")
    else:
        candidates = [os.path.join(dl_folder, "bronze"), \
                      path_bronze_subfolder_country(dl_folder), \
                      path_silver(dl_folder)]
        for cand_i in candidates:
            if not os.path.exists(cand_i):
                os.mkdir(cand_i)



def path_bronze_subfolder_country(dl_folder):
    """
    Gives the path to the country folder in the bronze layer.

    Parameters
    ----------
    dl_folder : str
        Path to the root directory of the data lake.
    
    Returns
    -------
    str
        Path to the country folder in the bronze layer.
    """
    return os.path.join(dl_folder, "bronze", "filings")


def path_silver(dl_folder):
    """
    Gives the path to the root folder of the silver layer.

    Parameters
    ----------
    dl_folder : str
        Path to the root directory of the data lake.
    
    Returns
    -------
    str
        Path to the root folder of the bronze layer.
    """
    return os.path.join(dl_folder, "silver")


def url_filings():
    """Base URL of filing-related queries from the API."""
    return "https://filings.xbrl.org/api/filings?include=entity&page[size]=200"


def extract_filing_data(filing_page):
    """
    Extracts filing information of an individual page of a JSON API query.

    Parameters
    ----------
    filing_page : requests.Response
        The response of a call to the API after applying the .json() method.
    
    Returns
    -------
    polars.DataFrame
        A table with information on available XBRL report submissions.
    """
    filing_page = filing_page["data"]
    result = []
    for i in range(len(filing_page)):
        json_url = filing_page[i]["attributes"]["json_url"]
        if json_url is not None:
            json_position = json_url.find(".json")
            lang = json_url[json_position-2:json_position]
            identifier = json_url.split("/")[1]
            row = {'country': filing_page[i]["attributes"]["country"],
                   'identifier': identifier,
                   'entity_id': filing_page[i]["relationships"]["entity"]["data"]["id"],
                   'lang': lang,
                   'period_end': filing_page[i]["attributes"]["period_end"],
                   'filing_id': filing_page[i]["id"],
                   'package_url': filing_page[i]["attributes"]["package_url"],
                   'json_url': json_url,
                   'processed': filing_page[i]["attributes"]["processed"]}
            result.append(row)
    return pl.DataFrame(result)


def available_filings(country):
    """
    Obtains a table with information on available XBRL report submissions.

    Parameters
    ----------
    countr : str
        ISO-2 country code.
    
    Returns
    -------
    polars.DataFrame
        A table with information on available XBRL report submissions.
    """
    url = url_filings() + "&filter[country]=" + country
    response = requests.get(url)
    if response.status_code == 200:
        filings = response.json()
        result = []
        result.append(extract_filing_data(filings))
        links = filings["links"]
        current_link = links["self"]
        last_link = links["last"]

        while current_link != last_link:
            response = requests.get(links["next"])
            filings = response.json()
            result.append(extract_filing_data(filings))
            links = filings["links"]
            current_link = links["self"]
        
        result = pl.concat(result).sort(["period_end", "processed"])
        return result
    else:
        print(f"Error: {response.status_code}")


def download_report_package(dl_folder, filings):
    """
    Downloads a copy of the XBRL Report Package for the filing.

    Parameters
    ----------
    dl_folder : str
        Path to the root directory of the data lake.
    filings : polars.DataFrame
        A table of available filings that should be downloaded.
    """
    for i in range(len(filings)):
        if filings["package_url"].is_not_null()[i]:
            url = 'https://filings.xbrl.org' + filings["package_url"][i]
            response = requests.get(url)
            if response.status_code == 200:
                country = filings["country"][i]
                identifier = filings["identifier"][i]
                lang = filings["lang"][i]
                period = filings["period_end"][i]
                subfolders = [os.path.join(path_bronze_subfolder_country(dl_folder), country),
                              os.path.join(path_bronze_subfolder_country(dl_folder), country, identifier), \
                              os.path.join(path_bronze_subfolder_country(dl_folder), country, identifier, lang),\
                              os.path.join(path_bronze_subfolder_country(dl_folder), country, identifier, lang, period)]
                for j in subfolders:
                    if not os.path.exists(j):
                        os.mkdir(j)
                
                final_file = os.path.join(path_bronze_subfolder_country(dl_folder), country, identifier, lang, period, filings["filing_id"][i] + ".zip")
                with open(final_file, mode="wb") as file:
                    file.write(response.content)
            else:
                print(f"Error: {response.status_code}")

