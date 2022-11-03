import logging
import re

from dateutil.relativedelta import relativedelta
from hdx.location.country import Country
from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import earliest_index, get_fraction_str, multiple_replace

logger = logging.getLogger(__name__)


class FTSException(Exception):
    pass


class FTS(BaseScraper):
    def __init__(self, datasetinfo, today, outputs, countryiso3s):
        super().__init__(
            "fts",
            datasetinfo,
            {
                "national": (
                    (
                        "RequiredFunding",
                        "Funding",
                        "PercentFunded",
                    ),
                    (
                        "#value+funding+required+usd",
                        "#value+funding+total+usd",
                        "#value+funding+pct",
                    ),
                ),
            },
            source_configuration=Sources.create_source_configuration(
                admin_sources=True
            ),
        )
        self.today = today
        self.outputs = outputs
        self.countryiso3s = countryiso3s

    def download(self, url, reader):
        json = reader.download_json(url)
        status = json["status"]
        if status != "ok":
            raise FTSException(f"{url} gives status {status}")
        return json

    def download_data(self, url, reader):
        return self.download(url, reader)["data"]

    def get_requirements_and_funding_location(
        self, base_url, plan, countryid_iso3mapping, reader
    ):
        allreqs, allfunds = dict(), dict()
        plan_id = plan["id"]
        url = f"{base_url}1/fts/flow/custom-search?planid={plan_id}&groupby=location"
        data = self.download_data(url, reader)
        requirements = data["requirements"]
        totalreq = requirements["totalRevisedReqs"]
        countryreq_is_totalreq = True
        for reqobj in requirements["objects"]:
            countryid = reqobj.get("id")
            if not countryid:
                continue
            countryiso = countryid_iso3mapping.get(str(countryid))
            if not countryiso:
                continue
            if countryiso not in self.countryiso3s:
                continue
            req = reqobj.get("revisedRequirements")
            if req:
                allreqs[countryiso] = req
                if req != totalreq:
                    countryreq_is_totalreq = False
        if countryreq_is_totalreq:
            allreqs = dict()
            logger.info(
                f"{plan_id} has same country requirements as total requirements!"
            )

        fundingobjects = data["report3"]["fundingTotals"]["objects"]
        if len(fundingobjects) != 0:
            objectsbreakdown = fundingobjects[0].get("objectsBreakdown")
            if objectsbreakdown:
                for fundobj in objectsbreakdown:
                    countryid = fundobj.get("id")
                    if not countryid:
                        continue
                    countryiso = countryid_iso3mapping.get(countryid)
                    if not countryiso:
                        continue
                    if countryiso not in self.countryiso3s:
                        continue
                    allfunds[countryiso] = fundobj["totalFunding"]
        return allreqs, allfunds

    def run(self) -> None:
        (
            requirements_values,
            funding_values,
            percent_values,
        ) = self.get_values("national")

        plantype_values = dict()

        def set_values(
            plan_type_value, requirements_value, funding_value, percent_value
        ):
            plantype_values[countryiso] = plan_type_value
            requirements_values[countryiso] = requirements_value
            if allfund:
                funding_values[countryiso] = funding_value
                percent_values[countryiso] = percent_value

        base_url = self.datasetinfo["url"]
        reader = self.get_reader(self.name)
        curdate = self.today - relativedelta(months=1)
        url = f"{base_url}2/fts/flow/plan/overview/progress/{curdate.year}"
        data = self.download_data(url, reader)
        plans = data["plans"]
        for plan in plans:
            allreq = plan["requirements"]["revisedRequirements"]
            funding = plan.get("funding")
            if funding:
                allfund = funding["totalFunding"]
                allpct = get_fraction_str(funding["progress"], 100)
            else:
                allfund = None
                allpct = None
            if plan.get("customLocationCode") == "COVD":
                continue
            plan_type = plan["planType"]["name"].lower()

            countries = plan["countries"]
            countryid_iso3mapping = dict()
            for country in countries:
                countryiso = country["iso3"]
                if countryiso:
                    countryid = country["id"]
                    countryid_iso3mapping[str(countryid)] = countryiso
            if len(countryid_iso3mapping) == 0:
                continue
            if len(countryid_iso3mapping) == 1:
                countryiso = countryid_iso3mapping.popitem()[1]
                if not countryiso or countryiso not in self.countryiso3s:
                    continue
                plantype_value = plantype_values.get(countryiso)
                if plantype_value == "humanitarian response plan":
                    continue
                if allreq:
                    if (
                        plan_type == "humanitarian response plan"
                        or requirements_values.get(countryiso) is None
                        or plantype_value == "regional response plan"
                    ):
                        set_values(plan_type, allreq, allfund, allpct)
            else:
                allreqs, allfunds = self.get_requirements_and_funding_location(
                    base_url, plan, countryid_iso3mapping, reader
                )
                for countryiso in allfunds:
                    plantype_value = plantype_values.get(countryiso)
                    if plantype_value in ("humanitarian response plan", "flash appeal"):
                        continue
                    allfund = allfunds[countryiso]
                    allreq = allreqs.get(countryiso)
                    if allreq:
                        allpct = get_fraction_str(allfund, allreq)
                    else:
                        allpct = None
                    if requirements_values.get(countryiso) is None and allreq:
                        set_values(plan_type, allreq, allfund, allpct)
                for countryiso in allreqs:
                    if countryiso in allfunds:
                        continue
                    plantype_value = plantype_values.get(countryiso)
                    if plantype_value in ("humanitarian response plan", "flash appeal"):
                        continue
                    if requirements_values.get(countryiso) is None and allreq:
                        set_values(plan_type, allreqs[countryiso], None, None)

        self.datasetinfo["source_date"] = self.today

    def add_sources(self) -> None:
        self.datasetinfo["source_date"] = {}
        source_dates = self.datasetinfo["source_date"]
        self.datasetinfo["source"] = {}
        sources = self.datasetinfo["source"]
        self.datasetinfo["source_url"] = {}
        source_urls = self.datasetinfo["source_url"]
        reader = self.get_reader()
        for countryiso3 in self.countryiso3s:
            countryname = Country.get_country_name_from_iso3(countryiso3).lower()
            datasetinfo = {"dataset": f"fts-requirements-and-funding-data-for-{countryname}", "format": "csv"}
            reader.read_hdx_metadata(datasetinfo)
            source_default_date = datasetinfo["source_date"]["default_date"]
            source_dates[f"CUSTOM_{countryiso3}"] = source_default_date
            source_dates["default_date"] = source_default_date
            sources[f"CUSTOM_{countryiso3}"] = datasetinfo["source"]
            sources["default_source"] = datasetinfo["source"]
            source_urls[f"CUSTOM_{countryiso3}"] = datasetinfo["source_url"]
            source_urls["default_url"] = datasetinfo["source_url"]
        super().add_sources()
