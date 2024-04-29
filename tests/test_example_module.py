# %%
""" """
# general
import os
from pathlib import Path

# project specific
import pandas as pd
import pytest
import yaml

# custom
from src.example_module import download, process

base_dir = Path(__file__).parents[1]
input_dir = base_dir / "input"
with open(input_dir / "configs/example_config.yml", "r", encoding="utf-8") as file:
    example_config = yaml.safe_load(file)


class TestDownload:
    url = example_config["url"]

    # test scrape file links returns an error correctly
    def test_url_type_error(self):
        with pytest.raises(TypeError):
            download.scrape_file_links(12345)


class TestProcess:
    data_path = input_dir / "data"
    summary_expected_columns = [
        "weekday",
        "appointment_date",
        "total_count_of_appointments",
        "attended",
        "did_not_attend",
        "unknown",
    ]
    nims_expected_columns = [
        "date",
        "type",
        "nhs_area_code",
        "ons_code",
        "name",
        "total",
    ]

    @pytest.mark.skipif(
        not os.path.exists(data_path / "summary.xlsx"), reason="file not downloaded"
    )
    def test_summary_loading(self):
        # test loading function returns dataframe
        assert isinstance(process.load_summary_data(self.data_path), pd.DataFrame)
        # test expected columns of summary
        df = process.load_summary_data(self.data_path)
        assert set(df.columns) == set(self.summary_expected_columns)
        # test no null values
        assert not df.isnull().values.any()

    @pytest.mark.skipif(
        not os.path.exists(data_path / "nims.csv"), reason="file not downloaded"
    )
    def test_nims_loading(self):
        # test loading function returns dataframe
        assert isinstance(process.load_nims_df(self.data_path), pd.DataFrame)
        # test expected columns of summary
        df = process.load_nims_df(self.data_path)
        assert set(df.columns) == set(self.nims_expected_columns)
        # test no null values
        assert not df.isnull().values.any()

    @pytest.mark.skipif(
        not os.path.exists(data_path / "nims.csv"), reason="file not downloaded"
    )
    def test_nims_monthly_totals(self):
        nims_df = process.load_nims_df(self.data_path)
        # test loading function returns dataframe
        assert isinstance(process.get_monthly_totals(nims_df), pd.DataFrame)
        # test expected columns of summary
        df = process.get_monthly_totals(nims_df)
        # test no null values
        assert not df.isnull().values.any()


# class TestVisualise():

# class TestUtil():
