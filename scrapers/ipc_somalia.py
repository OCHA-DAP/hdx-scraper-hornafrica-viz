import logging

from hdx.utilities.text import get_numeric_if_possible

logger = logging.getLogger(__name__)


def ipc_post_run(self) -> None:
    identifier = "ipc_override"
    reader = self.get_reader(prefix=identifier)
    index = self.get_headers("admintwo")[1].index(self.p3plus_hxltag)
    p3plus_values = self.get_values("admintwo")[index]
    index = self.get_headers("admintwo")[1].index(self.phase_hxltag)
    phase_values = self.get_values("admintwo")[index]
    dataset = reader.read_dataset(self.overrideinfo["dataset"])
    resource = dataset.get_resource()
    headers, iterator = reader.get_tabular_rows(
        resource["url"], dict_form=True, format="csv", headers=4
    )
    for pcode in [pcode for pcode in p3plus_values if pcode.startswith("SO")]:
        del p3plus_values[pcode]
        del phase_values[pcode]
    for row in iterator:
        adm2name = row["District"]
        if not adm2name:
            continue
        p3plus = 0
        if row["IPC 3"]:
            p3plus += get_numeric_if_possible(row["IPC 3"])
        if row["IPC 4"]:
            p3plus += get_numeric_if_possible(row["IPC 4"])
        if row["IPC 5"]:
            p3plus += get_numeric_if_possible(row["IPC 5"])
        if p3plus == 0:
            p3plus = None
        phase = int(row["Phase"])
        pcode, _ = self.admintwo.get_pcode("SOM", adm2name, identifier)
        if pcode:
            p3plus_values[pcode] = p3plus
            phase_values[pcode] = phase
    logger.info(f"Adding IPC SOM!")
