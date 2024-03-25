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
        "name": "asap-hotspots-monthly",
        "title": "Anomaly Hotspots of Agricultural Production",
        "maintainer": "2f9fd160-2a16-49c0-89d6-0bc3230599bf",
        "owner_org": "13b92c81-4df3-4ed6-a743-ec1a4e4889e8",
        "data_update_frequency": "30",
        "subnational": "0",
        "groups": [{"name": "world"}],
        "notes": f"[ASAP](https://agricultural-production-hotspots.ec.europa.eu/index.php) is an online decision support system for early warning about hotspots of agricultural production anomaly (crop and rangeland), developed by the JRC for food security crises prevention and response planning. \n\n The monthly hotspots data set is available below, but you can explore the hotspots on the [ASAP Warning Explorer](https://agricultural-production-hotspots.ec.europa.eu/wexplorer/) and access more contextual data on the [downloads page](https://agricultural-production-hotspots.ec.europa.eu/download.php). To learn more about the hotspots, refer to the [warning classification methodology document](https://agricultural-production-hotspots.ec.europa.eu/files/asap_warning_classification_v_4_0.pdf).",
        "tags": [
            {'name': 'climate hazards', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
            {'name': 'drought', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
            {'name': 'food security', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
        "dataset_date": "[2016-10-01T00:00:00 TO 2024-03-25T23:59:59]",

    }
    resource = {
        "description": "Historical data set with the time series of all ASAP hotspot assessments since October 2016 (to date).",
        "format": "csv",
        "name": "asap-hotspots-monthly.csv",
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
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        UserAgent.set_global("test")
        tags = (
            "climate hazards",
            "drought",
            "food security")
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
            "test_asap_monthly_hotspots", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                asaphotspots = AsapHotspots(configuration, retriever, folder, ErrorsOnExit())
                dataset_names = asaphotspots.get_data(
                    state={"ASAP-HOTSPOTS-MONTHLY": parse_date("2016-10-01"),
                           "DEFAULT": parse_date("2016-10-01")}
                )
                assert dataset_names == [{"name": "asap-hotspots-monthly"}]

                dataset = asaphotspots.generate_dataset("asap-hotspots-monthly")
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource
                file = "asap-hotspots-monthly.csv"
                assert_files_same(join("tests", "fixtures", file), join(folder, file))

