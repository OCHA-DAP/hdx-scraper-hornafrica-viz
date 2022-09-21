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
                    ),
                    (
                        "#value+funding+required+usd",
                        "#value+funding+total+usd",
                        "#value+funding+pct",
                        "#inneed",
                        "#targeted",
                        "#reached",
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
        requirements = self.get_values("national")[0]
        funding = self.get_values("national")[1]
        percentage = self.get_values("national")[2]
        affected = self.get_values("national")[3]
        targeted = self.get_values("national")[4]
        reached = self.get_values("national")[5]
        reader = self.get_reader("hdx")
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
                requirements[countryiso3] = number_format(
                    row.get("#value+funding+required"), format="%.0f"
                )
                funding[countryiso3] = number_format(row.get("#value+funding+total"), format="%.0f")
                percentage[countryiso3] = number_format(row.get("#value+funding+pct"), format="%.2f")
                affected[countryiso3] = number_format(
                    row.get("#inneed"), format="%.0f"
                )
                targeted[countryiso3] = number_format(
                    row.get("#targeted"), format="%.0f"
                )
                reached[countryiso3] = number_format(
                    row.get("#reached"), format="%.0f"
                )

        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = ""
