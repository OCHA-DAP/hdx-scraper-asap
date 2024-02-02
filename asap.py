#!/usr/bin/python
"""
ASAP Hotspots:
------------

Reads ASAP - Anomaly Hotspots of Agricultural Production .csv and creates datasets.

"""
import logging
import sys
from datetime import datetime, timezone
import zipfile
import os
import pandas as pd
import csv
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from slugify import slugify

logger = logging.getLogger(__name__)


class AsapHotspots:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.dataset_data = {}
        self.errors = errors

    def get_data(self):
        base_url = self.configuration["base_url"]
        filename = self.configuration["filename"]
        dataset_name = self.configuration["dataset_names"]["ASAP-HOTSPOTS-TEST"]

        data_url = f"{base_url}{filename}.zip"
        downloaded_zipfile = self.retriever.download_file(data_url)

        with zipfile.ZipFile(downloaded_zipfile, 'r') as hotspots_data_file:
            hotspots_data_file.extract(member=f"{filename}.csv",
                                       path=os.path.dirname(downloaded_zipfile))

        hotspots_file = f"{os.path.dirname(downloaded_zipfile)}{os.sep}{filename}.csv"
        hotspots_df = pd.read_csv(hotspots_file, sep=";", escapechar='\\').replace('[“”]', '', regex=True)

        self.dataset_data[dataset_name] = hotspots_df.apply(lambda x: x.to_dict(), axis=1)

        return [{"name": dataset_name}]

    def generate_dataset(self, dataset_name):
        rows = self.dataset_data[dataset_name]

        name = self.configuration["dataset_names"]["ASAP-HOTSPOTS-TEST"]
        title = "ASAP - Anomaly Hotspots of Agricultural Production: ASAP-HOTSPOTS-TEST"
        dataset = Dataset({"name": slugify(name), "title": title})
        dataset.set_maintainer("2f9fd160-2a16-49c0-89d6-0bc3230599bf")
        dataset.set_organization("13b92c81-4df3-4ed6-a743-ec1a4e4889e8")
        update_frequency = "monthly"
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        dataset["notes"] = "Monthly identification of agricultural production hotspot countries and summary narratives for agriculture and food security analysts. The historical hotspot time series is available in .csv format. To visualize the information in a GIS environment, use the field asap0_id and join with the spatial layer gaul0_asap (see section Administrative Boundaries). The latest ASAP hotspot layer can also be downloaded as shapefile directly from the ASAP homepage and is available as WFS/WMS"
        filename = f"{dataset_name.lower()}.csv"
        resourcedata = {
            "name": filename,
            "description": "",
        }

        '''
        if metadata["Tags"]:
            for tag in metadata["Tags"]:
                tags.add(tag["Tag"].lower())
        if metadata["Themes"]:
            for theme in metadata["Themes"]:
                tags.add(theme["Theme"].lower())
        '''
        tags = sorted([t for t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        start_date = "2023-01-01"#metadata["Start Range"]
        end_date = "2024-01-01"#metadata["End Range"]
        ongoing = True
        if end_date:
            ongoing = False
        if not start_date:
            logger.error(f"Start date missing for {dataset_name}")
            return None, None
        dataset.set_time_period(start_date, end_date, ongoing)

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

        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            rows,
            resourcedata,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        return dataset
