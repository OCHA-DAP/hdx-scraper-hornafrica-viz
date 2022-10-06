import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.location.country import Country
from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.sources import create_source_configuration

logger = logging.getLogger(__name__)


class IPC(BaseScraper):
    def __init__(self, datasetinfo, today, countryiso3s, adminone, admintwo):
        self.phases = ["3", "4", "5"]
        self.projections = ["Current", "First Projection", "Second Projection"]
        p3plus_header = "FoodInsecurityIPCP3+"
        self.p3plus_hxltag = "#affected+food+ipc+p3plus+num"
        phase_header = "FoodInsecurityIPCPhase"
        self.phase_hxltag = "#affected+food+ipc+phase+type"
        colheaders = [f"FoodInsecurityIPC{phase}" for phase in self.phases]
        colheaders.append(p3plus_header)
        colheaders.append("FoodInsecurityIPCAnalysedNum")
        colheaders.append("FoodInsecurityIPCAnalysisPeriod")
        colheaders.append("FoodInsecurityIPCAnalysisPeriodStart")
        colheaders.append("FoodInsecurityIPCAnalysisPeriodEnd")
        hxltags = [f"#affected+food+ipc+p{phase}+num" for phase in self.phases]
        hxltags.append(self.p3plus_hxltag)
        hxltags.append("#affected+food+ipc+analysed+num")
        hxltags.append("#date+ipc+period")
        hxltags.append("#date+ipc+start")
        hxltags.append("#date+ipc+end")
        super().__init__(
            "ipc",
            datasetinfo,
            {
                "national": (tuple(colheaders), tuple(hxltags)),
                "adminone": (
                    (
                        p3plus_header,
                        phase_header,
                    ),
                    (
                        self.p3plus_hxltag,
                        self.phase_hxltag,
                    ),
                ),
                "admintwo": (
                    (
                        p3plus_header,
                        phase_header,
                    ),
                    (
                        self.p3plus_hxltag,
                        self.phase_hxltag,
                    ),
                ),
            },
            source_configuration=create_source_configuration(
                adminlevel=(adminone, admintwo)
            ),
        )
        self.today = today
        self.countryiso3s = countryiso3s
        self.adminone = adminone
        self.admintwo = admintwo

    def get_period(self, projections, countryiso3):
        if self.admintwo.get_admin_level(countryiso3) == 2:
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
        national_outputs = self.get_values("national")
        national_populations = {
            phase: national_outputs[i] for i, phase in enumerate(self.phases)
        }
        i = len(self.phases)
        national_populations["P3+"] = national_outputs[i]
        national_analysed = national_outputs[i + 1]
        national_period = national_outputs[i + 2]
        national_start = national_outputs[i + 3]
        national_end = national_outputs[i + 4]
        adminone_populations = self.get_values("adminone")[0]
        admintwo_populations = self.get_values("admintwo")[0]
        adminone_phases = self.get_values("adminone")[1]
        admintwo_phases = self.get_values("admintwo")[1]
        projection_names = ["Current", "First Projection", "Second Projection"]
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
            sum = 0
            projection_mapping = projection_mappings[projection_number]
            for phase in self.phases:
                population = country_data[
                    f"phase{phase}_population{projection_mapping}"
                ]
                national_populations[phase][countryiso3] = population
                sum += population
            national_populations["P3+"][countryiso3] = sum
            national_analysed[countryiso3] = country_data[
                f"estimated_population{projection_mapping}"
            ]
            national_period[countryiso3] = projection_names[projection_number]
            national_start[countryiso3] = start
            national_end[countryiso3] = end
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
                    cur_sum = adminone_populations.get(pcode)
                    if cur_sum:
                        adminone_populations[pcode] = cur_sum + sum
                    else:
                        adminone_populations[pcode] = sum
                    phase_class = None
                    for phase in range(1, 6):
                        pct = admin1_area.get(
                            f"phase{phase}_percentage{projection_mapping}"
                        )
                        if pct and pct >= 0.2:
                            phase_class = phase
                    cur_phase = adminone_phases.get(pcode)
                    if cur_phase:
                        if phase_class and phase_class > cur_phase:
                            adminone_phases[pcode] = phase_class
                    else:
                        adminone_phases[pcode] = phase_class
                    if self.admintwo.get_admin_level(countryiso3) == 1:
                        if cur_sum:
                            admintwo_populations[pcode] = cur_sum + sum
                        else:
                            admintwo_populations[pcode] = sum
                        if cur_phase:
                            if phase_class and phase_class > cur_phase:
                                admintwo_phases[pcode] = phase_class
                        else:
                            admintwo_phases[pcode] = phase_class
                        continue
                    admin2_areas = admin1_area.get("areas")
                    if admin2_areas:
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
                            cur_sum = admintwo_populations.get(pcode)
                            if cur_sum:
                                admintwo_populations[pcode] = cur_sum + sum
                            else:
                                admintwo_populations[pcode] = sum
                            phase_class = None
                            for phase in range(1, 6):
                                pct = admin2_area.get(
                                    f"phase{phase}_percentage{projection_mapping}"
                                )
                                if pct and pct >= 0.2:
                                    phase_class = phase
                            cur_phase = admintwo_phases.get(pcode)
                            if cur_phase:
                                if phase_class and phase_class > cur_phase:
                                    admintwo_phases[pcode] = phase_class
                            else:
                                admintwo_phases[pcode] = phase_class
        reader.read_hdx_metadata(self.datasetinfo)
