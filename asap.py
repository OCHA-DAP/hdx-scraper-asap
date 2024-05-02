#!/usr/bin/python
"""
ASAP Hotspots:
------------

Reads ASAP - Anomaly Hotspots of Agricultural Production .csv and creates datasets.

"""
import logging
from datetime import datetime, timezone
import zipfile
import pandas as pd
from hdx.data.dataset import Dataset
from slugify import slugify
from hdx.location.country import Country
import os

logger = logging.getLogger(__name__)


def correct_country_names(countries):
    """
    Some countries don't match the HDX library so they need to be fixed before publishing.

    :param countries:
    :return: countries list with corrected names
    """

    corrected_countries = []
    countries_mapping = {"Laos": "Lao People's Democratic Republic",
                         "North Korea": "Democratic People's Republic of Korea",
                         "Central Africa": "Central African Republic",
                         "DR Congo": "Democratic Republic of the Congo",
                         "Equat. Guinea": "Equatorial Guinea"}

    for country in countries:
        if country in countries_mapping.keys():
            corrected_countries.append(countries_mapping[country])
        else:
            corrected_countries.append(country)

    return corrected_countries


class AsapHotspots:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.manual_url = None
        self.dataset_data = {}
        self.errors = errors
        self.created_date = None

    def get_data(self, state):
        base_url = self.configuration["base_url"]
        hotspots_filename = self.configuration["hotspots_filename"]
        dataset_name = self.configuration["dataset_names"]["ASAP-HOTSPOTS-MONTHLY"]
        self.manual_url = self.configuration["manual_link"]

        data_url = f"{base_url}{hotspots_filename}.zip"
        downloaded_zipfile = self.retriever.download_file(data_url)

        with zipfile.ZipFile(downloaded_zipfile, 'r') as hotspots_data_file:
            hotspots_data_file.extractall(path=os.path.dirname(downloaded_zipfile))

        hotspots_file = f"{os.path.dirname(downloaded_zipfile)}{os.sep}{hotspots_filename}.csv"

        # Checking if the downloaded file created date is more recent than the most current dataset
        self.created_date = datetime.fromtimestamp((os.path.getctime(hotspots_file)), tz=timezone.utc)
        if self.created_date > state.get(dataset_name, state["DEFAULT"]):
            hotspots_df = pd.read_csv(hotspots_file, sep=";", escapechar='\\').replace('[“”]', '', regex=True)

            hotspots_df["asap0_name"] = correct_country_names(hotspots_df["asap0_name"])
            hotspots_df["ISO3"] = [Country.get_iso3_country_code(country) for country in hotspots_df["asap0_name"]]
            self.dataset_data[dataset_name] = hotspots_df.apply(lambda x: x.to_dict(), axis=1)

            return [{"name": dataset_name}]
        else:
            return None

    def generate_dataset(self, dataset_name):

        # Setting metadata and configurations
        name = self.configuration["dataset_names"]["ASAP-HOTSPOTS-MONTHLY"]
        title = self.configuration["title"]
        update_frequency = self.configuration["update_frequency"]
        dataset = Dataset({"name": slugify(name), "title": title})
        rows = self.dataset_data[dataset_name]
        dataset.set_maintainer(self.configuration["maintainer_id"])
        dataset.set_organization(self.configuration["organization_id"])
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        dataset["notes"] = self.configuration["notes"]
        filename = f"{dataset_name.lower()}.csv"
        resource_data = {"name": filename,
                         "description": self.configuration["description"]}
        tags = sorted([t for t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        # Setting time period
        start_date = self.configuration["start_date"]
        ongoing = False
        if not start_date:
            logger.error(f"Start date missing for {dataset_name}")
            return None, None
        dataset.set_time_period(start_date, self.created_date, ongoing)

        headers = rows[0].keys()
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        for row in rows:
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        return dataset
