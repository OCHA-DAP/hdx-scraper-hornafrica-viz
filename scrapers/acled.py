import logging
from itertools import chain

from hdx.location.country import Country
from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)

hxltags = {
    "event_date": "#date+occurred",
    "event_type": "#event+type",
    "sub_event_type": "#event+type+sub",
    "actor1": "#group+name+first",
    "actor2": "#group+name+second",
    "admin1": "#adm1+name",
    "admin2": "#adm2+name",
    "admin3": "#adm3+name",
    "adm2_pcode": "#adm2+code",
    "location": "#loc+name",
    "latitude": "#geo+lat",
    "longitude": "#geo+lon",
    "notes": "#description",
    "fatalities": "#affected+killed",
}


class ACLED(BaseScraper):
    def __init__(
        self,
        datasetinfo,
        today,
        countryiso3s,
        outputs,
        admintwo,
    ):
        # ACLED outputs to its own tab "fatalities" so there are no headers
        super().__init__(
            "acled",
            datasetinfo,
            dict(),
            source_configuration=Sources.create_source_configuration(
                adminlevel=admintwo
            ),
        )
        self.start_date = parse_date(datasetinfo["start_date"])
        self.today = today
        self.countryiso3s = countryiso3s
        self.outputs = outputs
        self.admintwo = admintwo

    def run(self):
        years = range(self.start_date.year, self.today.year + 1)
        iterables = list()
        reader = self.get_reader()
        for year in years:
            for countryiso3 in self.countryiso3s:
                countrycode = Country.get_m49_from_iso3(countryiso3)
                url = self.datasetinfo["url"] % (countrycode, year)
                path = reader.download_file(url)
                downloader = Download()
                headers, iterator = downloader.get_tabular_rows(path, dict_form=True)
                iterables.append(iterator)
        latest_date = default_date
        rows = [list(hxltags.keys()), list(hxltags.values())]
        for inrow in chain.from_iterable(iterables):
            date = parse_date(inrow["event_date"])
            if date < self.start_date:
                continue
            if date > latest_date:
                latest_date = date
            iso3 = Country.get_iso3_from_m49(int(inrow["iso"]))
            admlevel = self.admintwo.get_admin_level(iso3)
            admname = inrow[f"admin{admlevel}"]
            pcode = None
            if admname:
                pcode, _ = self.admintwo.get_pcode(iso3, admname)
            inrow["adm2_pcode"] = pcode
            row = list()
            for header in hxltags:
                row.append(inrow[header])
            rows.append(row)
        tabname = "fatalities"
        for output in self.outputs.values():
            output.update_tab(tabname, rows)
        self.datasetinfo["source_date"] = latest_date

    def add_sources(self):
        self.add_hxltag_source(
            "#date+latest+acled+regional", datasetinfo=self.datasetinfo
        )
        for countryiso3 in self.countryiso3s:
            self.add_hxltag_source(
                f"#date+latest+acled+{countryiso3.lower()}",
                datasetinfo=self.datasetinfo,
            )
