import logging

from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


def idps_post_run(self) -> None:
    reader = self.get_reader(prefix="idps_override")
    index = self.get_headers("admintwo")[1].index("#affected+idps+ind")
    values = self.get_values("admintwo")[index]
    dataset = reader.read_dataset(self.overrideinfo["dataset"])
    resource = dataset.get_resource()
    headers, iterator = reader.get_tabular_rows(
        resource["url"], dict_form=True, format="xlsx"
    )
    idpsdict = dict()
    for row in iterator:
        if row["Year"] != self.today.year:
            continue
        idps = row["Number of Individuals"]
        if not idps:
            continue
        dict_of_lists_add(idpsdict, row["Current (Arrival) District"], idps)
    for adm2name in idpsdict:
        pcode, _ = self.admintwo.get_pcode("SOM", adm2name, "idps_override")
        if pcode:
            values[pcode] = sum(idpsdict[adm2name])
    logger.info(f"Adding SOM IDPs!")
