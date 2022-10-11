import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.location.country import Country
from hdx.scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class IPC(BaseScraper):
    def __init__(self, datasetinfo, today, countryiso3s, adminone, admintwo):
        self.phases = ["3", "4", "5"]
        p3plus_header = "FoodInsecurityIPCP3+"
        p3plus_hxltag = "#affected+food+ipc+p3plus+num"
        super().__init__(
            "ipc",
            datasetinfo,
            {
                "adminone": ((p3plus_header,), (p3plus_hxltag,)),
                "admintwo": ((p3plus_header,), (p3plus_hxltag,)),
            },
        )
        self.today = today
        self.countryiso3s = countryiso3s
        self.adminone = adminone
        self.admintwo = admintwo

    def get_period(self, projections, countryiso3):
        if self.admintwo.get_admin_level(countryiso3) > 1:
            projection_number = 0
            projection = projections[projection_number]
            start = datetime.strptime(projection[0:8], "%b %Y").date()
            end = datetime.strptime(projection[11:19], "%b %Y").date() + relativedelta(
                day=31
            )
            return (
                projection_number,
                start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
            )

        today = self.today.date()
        projection_number = None
        for i, projection in enumerate(projections):
            if projection == "":
                continue
            start = datetime.strptime(projection[0:8], "%b %Y").date()
            end = datetime.strptime(projection[11:19], "%b %Y").date() + relativedelta(
                day=31
            )
            if today < end:
                projection_number = i
                break
        if projection_number is None:
            for i, projection in reversed(list(enumerate(projections))):
                if projection != "":
                    projection_number = i
                    break
        return projection_number, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def run(self):
        base_url = self.datasetinfo["url"]
        reader = self.get_reader(self.name)
        countryisos = set()
        json = reader.download_json(f"{base_url}/analyses?type=A")
        for analysis in json:
            countryiso2 = analysis["country"]
            countryiso3 = Country.get_iso3_from_iso2(countryiso2)
            if countryiso3 not in self.countryiso3s:
                continue
            countryisos.add((countryiso3, countryiso2))
        adminone_p3plus = self.get_values("adminone")[0]
        admintwo_p3plus = self.get_values("admintwo")[0]
        projection_mappings = ["", "_projected", "_second_projected"]
        analysis_dates = set()
        for countryiso3, countryiso2 in sorted(countryisos):
            url = f"{base_url}/population?country={countryiso2}"
            country_data = reader.download_json(url)
            if country_data:
                country_data = country_data[0]
            else:
                continue
            analysis_dates.add(country_data["analysis_date"])
            projections = list()
            projections.append(country_data["current_period_dates"])
            projections.append(country_data["projected_period_dates"])
            projections.append(country_data["second_projected_period_dates"])
            projection_number, start, end = self.get_period(projections, countryiso3)
            projection_mapping = projection_mappings[projection_number]
            admin1_areas = country_data.get("groups", country_data.get("areas"))
            if admin1_areas:
                for admin1_area in admin1_areas:
                    pcode, _ = self.adminone.get_pcode(
                        countryiso3, admin1_area["name"], "IPC"
                    )
                    if not pcode:
                        continue
                    sum = 0
                    for phase in self.phases:
                        pop = admin1_area.get(
                            f"phase{phase}_population{projection_mapping}"
                        )
                        if pop:
                            sum += pop
                    cur_sum = adminone_p3plus.get(pcode)
                    if cur_sum:
                        adminone_p3plus[pcode] = cur_sum + sum
                    else:
                        adminone_p3plus[pcode] = sum
                    if self.admintwo.get_admin_level(countryiso3) == 1:
                        if cur_sum:
                            admintwo_p3plus[pcode] = cur_sum + sum
                        else:
                            admintwo_p3plus[pcode] = sum
                    admin2_areas = admin1_area.get("areas")
                    if admin2_areas and self.admintwo.get_admin_level(countryiso3) < 3:
                        for admin2_area in admin2_areas:
                            pcode, _ = self.admintwo.get_pcode(
                                countryiso3, admin2_area["name"], "IPC"
                            )
                            if not pcode:
                                continue
                            sum = 0
                            for phase in self.phases:
                                pop = admin2_area.get(
                                    f"phase{phase}_population{projection_mapping}"
                                )
                                if pop:
                                    sum += pop
                            cur_sum = admintwo_p3plus.get(pcode)
                            if cur_sum:
                                admintwo_p3plus[pcode] = cur_sum + sum
                            else:
                                admintwo_p3plus[pcode] = sum
        reader.read_hdx_metadata(self.datasetinfo)
