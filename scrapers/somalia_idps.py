import logging

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.sources import Sources

logger = logging.getLogger(__name__)


class SomaliaIDPs(BaseScraper):
    def __init__(self, datasetinfo, admintwo):
        super().__init__(
            "somalia_idps",
            datasetinfo,
            {
                "admintwo": (
                    ("IDPS",),
                    ("#affected+idps+ind",),
                ),
            },
            source_configuration=Sources.create_source_configuration(
                adminlevel=admintwo,
                should_overwrite_sources=True,
            ),
        )
        self.admintwo = admintwo

    def run(self):
        reader = self.get_reader()
        headers, iterator = reader.read(self.datasetinfo)
        idps = {}
        max_yearweek = {}
        for inrow in iterator:
            if "drought" not in inrow["Reason"].lower():
                continue
            district = inrow["Current (Arrival) District"]
            pcode = self.admintwo.fuzzy_pcode("SOM", district)
            if not pcode:
                continue
            yearweek = inrow["Year Week"]
            if yearweek > max_yearweek.get(pcode, 0):
                max_yearweek[pcode] = yearweek
            individuals = inrow["Number of Individuals"]
            idps_yearweek = idps.get(yearweek, {})
            idps_yearweek[pcode] = idps_yearweek.get(pcode, 0) + individuals
            idps[yearweek] = idps_yearweek
        valuedict = self.get_values("admintwo")[0]
        for pcode in sorted(max_yearweek):
            end_yearweek = str(max_yearweek[pcode])
            year = end_yearweek[:4]
            month = end_yearweek[-2:]
            year = int(year) - 1
            start_yearweek = int(f"{year}{month}")
            idps_total = 0
            for yearweek in sorted(idps):
                if yearweek < start_yearweek:
                    continue
                idps_total += idps[yearweek].get(pcode, 0)
            valuedict[pcode] = idps_total
