#!/usr/bin/python
"""
Unit tests for ASAP Hotspots.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.vocabulary import Vocabulary
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from asap import AsapHotspots


class TestAsapHotspots:
    dataset = {
        "data_update_frequency": "30",
        "dataset_date": "[2023-01-01T00:00:00 TO 2024-01-01T23:59:59]",
        "groups": [{"name": "world"}],
        "maintainer": "2f9fd160-2a16-49c0-89d6-0bc3230599bf",
        "name": "test-asap-hotspots",
        "notes": "Monthly identification of agricultural production hotspot countries and summary narratives for agriculture and food security analysts. The historical hotspot time series is available in .csv format. To visualize the information in a GIS environment, use the field asap0_id and join with the spatial layer gaul0_asap (see section Administrative Boundaries). The latest ASAP hotspot layer can also be downloaded as shapefile directly from the ASAP homepage and is available as WFS/WMS",
        "owner_org": "13b92c81-4df3-4ed6-a743-ec1a4e4889e8",
        "subnational": "0",
        "tags": [
            {'name': 'drought', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
            {'name': 'climate hazards', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
        "title": "ASAP - Anomaly Hotspots of Agricultural Production: ASAP-HOTSPOTS-TEST",
    }
    resource = {
        "description": "",
        "format": "csv",
        "name": "asap_hotspots_test.csv",
        "resource_type": "file.upload",
        "url_type": "upload",
    }

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        tags = (
            "drought",
            "climate hazards"
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    def test_generate_dataset(self, configuration, fixtures):
        with temp_dir(
            "test_asap_hotspots", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                asaphotspots = AsapHotspots(configuration, retriever, folder, ErrorsOnExit())
                dataset_names = asaphotspots.get_data()
                assert dataset_names == [{"name": "test-asap-hotspots"}]

                dataset = asaphotspots.generate_dataset("test-asap-hotspots")
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource
                file = "asap_hotspots_test.csv"
                assert_files_same(join("tests", "fixtures", file), join(folder, file))
