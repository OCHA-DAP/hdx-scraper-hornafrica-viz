import logging
from abc import ABC

from hdx.scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class BaseIPC(BaseScraper, ABC):
    def __init__(self, name, datasetinfo, today, countryiso3s, adminone, admintwo):
        self.phases = ["3", "4", "5"]
        p3plus_header = "FoodInsecurityIPCP3+"
        p3plus_hxltag = "#affected+food+ipc+p3plus+num"
        super().__init__(
            name,
            datasetinfo,
            {
                "national": ((p3plus_header,), (p3plus_hxltag,)),
                "adminone": ((p3plus_header,), (p3plus_hxltag,)),
                "admintwo": ((p3plus_header,), (p3plus_hxltag,)),
            },
        )
        self.today = today
        self.countryiso3s = countryiso3s
        self.adminone = adminone
        self.admintwo = admintwo
