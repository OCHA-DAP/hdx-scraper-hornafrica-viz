import logging

import hxl
from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format
from hxl import InputOptions

logger = logging.getLogger(__name__)


class AffectedTargetedReached(BaseScraper):
    def __init__(self, datasetinfo, today, adminone, admintwo):
        super().__init__(
            "affected_targeted_reached",
            datasetinfo,
            {
                "adminone": (
                    ("Total Affected", "Total Targeted", "Total Reached", "Priority"),
                    (
                        "#affected+total",
                        "#targeted+total",
                        "#reached+total",
                        "#priority",
                    ),
                ),
                "admintwo": (
                    ("Total Affected", "Total Targeted", "Total Reached", "Priority"),
                    (
                        "#affected+total",
                        "#targeted+total",
                        "#reached+total",
                        "#priority",
                    ),
                ),
            },
        )
        self.today = today
        self.adminone = adminone
        self.admintwo = admintwo

    def run(self) -> None:
        urls = self.datasetinfo["urls"]
        affecteddict1 = dict()
        targeteddict1 = dict()
        reacheddict1 = dict()
        prioritydict1 = dict()
        affecteddict2 = dict()
        targeteddict2 = dict()
        reacheddict2 = dict()
        prioritydict2 = dict()

        for countryiso3, url in urls.items():
            data = hxl.data(url, InputOptions(allow_local=True)).cache()
            admin_level1 = self.adminone.get_admin_level(countryiso3)
            admin_level2 = self.admintwo.get_admin_level(countryiso3)
            for row in data:
                pcode1 = row.get(f"#adm{admin_level1}+code")
                pcode2 = row.get(f"#adm{admin_level2}+code")

                def add_to_dict(inddict, hxltag, pcode):
                    value = row.get(hxltag)
                    if value is not None:
                        dict_of_lists_add(inddict, f"{countryiso3}:{pcode}", int(value))

                add_to_dict(affecteddict1, "#affected+total", pcode1)
                add_to_dict(targeteddict1, "#targeted+total", pcode1)
                add_to_dict(reacheddict1, "#reached+total", pcode1)
                add_to_dict(prioritydict1, "#priority", pcode1)
                add_to_dict(affecteddict2, "#affected+total", pcode2)
                add_to_dict(targeteddict2, "#targeted+total", pcode2)
                add_to_dict(reacheddict2, "#reached+total", pcode2)
                add_to_dict(prioritydict2, "#priority", pcode2)

        def fill_values(input, output, adminlevel, average=False):
            for countrypcode in input:
                countryiso3, pcode = countrypcode.split(":")
                if pcode not in adminlevel.pcodes:
                    logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
                else:
                    aggregate_value = sum(input[countrypcode])
                    if average:
                        aggregate_value /= len(input[countrypcode])
                    output[pcode] = number_format(aggregate_value, format="%.0f")

        affected = self.get_values("adminone")[0]
        fill_values(affecteddict1, affected, self.adminone)
        targeted = self.get_values("adminone")[1]
        fill_values(targeteddict1, targeted, self.adminone)
        reached = self.get_values("adminone")[2]
        fill_values(reacheddict1, reached, self.adminone)
        priority = self.get_values("adminone")[3]
        fill_values(prioritydict1, priority, self.adminone, average=True)
        affected = self.get_values("admintwo")[0]
        fill_values(affecteddict2, affected, self.admintwo)
        targeted = self.get_values("admintwo")[1]
        fill_values(targeteddict2, targeted, self.admintwo)
        reached = self.get_values("admintwo")[2]
        fill_values(reacheddict2, reached, self.admintwo)
        priority = self.get_values("admintwo")[3]
        fill_values(prioritydict2, priority, self.admintwo, average=True)
        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = ""
