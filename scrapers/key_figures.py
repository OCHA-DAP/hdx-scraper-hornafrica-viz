import logging

from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.text import number_format

logger = logging.getLogger(__name__)


class KeyFigures(BaseScraper):
    def __init__(self, datasetinfo, today):
        super().__init__(
            "key_figures",
            datasetinfo,
            {
                "national": (
                    (
                        "RequiredFunding",
                        "Funding",
                        "PercentFunded",
                        "InNeed",
                        "Targeted",
                        "Reached",
                        "Internal Displacement",
                        "Food Insecurity",
                        "SAM",
                        "MAM",
                        "GAM",
                        "Water Insecurity",
                    ),
                    (
                        "#value+funding+required+usd",
                        "#value+funding+total+usd",
                        "#value+funding+pct",
                        "#inneed",
                        "#targeted",
                        "#reached",
                        "#affected+idps",
                        "#affected+food",
                        "#affected+sam",
                        "#affected+mam",
                        "#affected+gam",
                        "#affected+water",
                    ),
                )
            },
        )
        self.today = today

    def run(self) -> None:
        url = self.datasetinfo["url"]
        reader = self.get_reader()
        headers, iterator = reader.get_tabular_rows(
            url, headers=1, dict_form=True, format="csv"
        )
        rows = list(iterator)
        hxltags = self.get_headers("national")[1]
        values = self.get_values("national")
        for ds_row in rows:
            countryiso3 = ds_row["Country ISO"]
            dataset_name = ds_row["Dataset Name"]
            if not dataset_name:
                logger.warning(f"No Key Figures data for {countryiso3}.")
                continue
            dataset = reader.read_dataset(dataset_name)
            if not dataset:
                logger.warning(f"No Key Figures data for {countryiso3}.")
                continue
            resource = dataset.get_resource()
            data = reader.read_hxl_resource(countryiso3, resource, "Key Figures data")
            if data is None:
                continue
            for row in data:
                for i, hxltag in enumerate(hxltags):
                    values[i][countryiso3] = row.get(hxltag)

        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = ""
