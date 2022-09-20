import logging

from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class PINTargetedReached(BaseScraper):
    def __init__(self, datasetinfo, today, admintwo):
        super().__init__(
            "pin_target_reach",
            datasetinfo,
            {
                "admintwo": (
                    ("PIN", "Targeted", "Reached"),
                    ("#inneed", "#targeted", "#reached"),
                )
            },
        )
        self.today = today
        self.admintwo = admintwo

    def run(self) -> None:
        pin_url = self.datasetinfo["url"]
        reader = self.get_reader()
        headers, iterator = reader.get_tabular_rows(
            pin_url, headers=1, dict_form=True, format="csv"
        )
        rows = list(iterator)
        inneeddict = dict()
        targeteddict = dict()
        reacheddict = dict()
        reader = self.get_reader("hdx")
        for ds_row in rows:
            countryiso3 = ds_row["Country ISO"]
            dataset_name = ds_row["Dataset Name"]
            if not dataset_name:
                logger.warning(f"No PIN Targeted Reached data for {countryiso3}.")
                continue
            dataset = reader.read_dataset(dataset_name)
            if not dataset:
                logger.warning(f"No PIN Targeted Reached data for {countryiso3}.")
                continue
            resource = dataset.get_resource()
            headers, iterator = reader.get_tabular_rows(
                resource["url"], dict_form=True, format="xlsx", sheet=2, headers=[1, 2]
            )
            admin_level = self.admintwo.get_admin_level(countryiso3)
            admcode = f"LOCATION admin{admin_level}Pcode"
            pcodes_found = False
            for row in iterator:
                pcode = row[admcode].strip().upper()
                for key in row:
                    lowerkey = key.lower()
                    if "overall" not in lowerkey:
                        continue
                    value = row[key]
                    if not value:
                        continue
                    if "target" in lowerkey:
                        dict_of_lists_add(targeteddict, f"{countryiso3}:{pcode}", value)
                    elif "reached" in lowerkey:
                        dict_of_lists_add(reacheddict, f"{countryiso3}:{pcode}", value)
                    else:
                        dict_of_lists_add(inneeddict, f"{countryiso3}:{pcode}", value)
                pcodes_found = True
            if not pcodes_found:
                logger.warning(f"No pcodes found for {countryiso3}.")

        def fill_values(input, output):
            for countrypcode in input:
                countryiso3, pcode = countrypcode.split(":")
                if pcode not in self.admintwo.pcodes:
                    logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
                else:
                    output[pcode] = sum(input[countrypcode])

        inneed = self.get_values("admintwo")[0]
        fill_values(inneeddict, inneed)
        targeted = self.get_values("admintwo")[1]
        fill_values(targeteddict, targeted)
        reached = self.get_values("admintwo")[2]
        fill_values(reacheddict, reached)
        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = ""
