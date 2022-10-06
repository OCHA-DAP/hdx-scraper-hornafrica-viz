import logging

from hdx.utilities.text import get_numeric_if_possible
from scrapers.baseipc import BaseIPC

logger = logging.getLogger(__name__)


class IPCSomalia(BaseIPC):
    def __init__(self, datasetinfo, today, adminone, admintwo):
        super().__init__(
            "ipc_somalia", datasetinfo, today, ("SOM",), adminone, admintwo
        )

    def run(self) -> None:
        national_p3plus = self.get_values("national")[0]
        adminone_p3plus = self.get_values("adminone")[0]
        admintwo_p3plus = self.get_values("admintwo")[0]
        reader = self.get_reader(self.name)
        headers, iterator = reader.read(self.datasetinfo)
        for row in iterator:
            adm1name = row["Region"]
            adm2name = row["District"]
            p3plus = 0
            if row["IPC 3"]:
                p3plus += get_numeric_if_possible(row["IPC 3"])
            if row["IPC 4"]:
                p3plus += get_numeric_if_possible(row["IPC 4"])
            if row["IPC 5"]:
                p3plus += get_numeric_if_possible(row["IPC 5"])
            if p3plus == 0:
                p3plus = None
            if adm1name == "TOTAL":
                national_p3plus["SOM"] = p3plus
                continue
            pcode, _ = self.adminone.get_pcode("SOM", adm1name, self.name)
            if pcode:
                population = adminone_p3plus.get(pcode, 0)
                population += p3plus
                adminone_p3plus[pcode] = population
            pcode, _ = self.admintwo.get_pcode("SOM", adm2name, self.name)
            if pcode:
                admintwo_p3plus[pcode] = p3plus
