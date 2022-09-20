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
                        "FundingTimeline"
                        "RequiredFunding",
                        "Funding",
                        "PercentFunded",
                        "Affected",
                        "Targeted",
                        "Reached",
                    ),
                    (
                        "#date+funding",
                        "#value+funding+required+usd",
                        "#value+funding+total+usd",
                        "#value+funding+pct",
                        "#affected",
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
        timelines = self.get_values("national")[0]
        requirements = self.get_values("national")[1]
        funding = self.get_values("national")[2]
        percentage = self.get_values("national")[3]
        affected = self.get_values("national")[4]
        targeted = self.get_values("national")[5]
        reached = self.get_values("national")[6]
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
            headers, iterator = reader.get_tabular_rows(
                resource["url"], dict_form=True, format="xlsx"
            )
            for row in iterator:
                timelines[countryiso3] = row["Funding timeline"]
                requirements[countryiso3] = number_format(
                    row["Total Fund Requirement"], format="%.0f"
                )
                funding[countryiso3] = number_format(row["Funded"], format="%.0f")
                percentage[countryiso3] = number_format(row["Funded %"], format="%.2f")
                affected[countryiso3] = number_format(
                    row["Total Affected"], format="%.0f"
                )
                targeted[countryiso3] = number_format(
                    row["Total Targeted"], format="%.0f"
                )
                reached[countryiso3] = number_format(
                    row["Total Reached"], format="%.0f"
                )

        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = ""
