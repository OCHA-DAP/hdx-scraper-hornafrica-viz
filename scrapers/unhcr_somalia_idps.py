import logging

from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


def idps_post_run(self) -> None:
    identifier = "idps_override"
    reader = self.get_reader(prefix=identifier)
    index = self.get_headers("admintwo")[1].index(self.hxltag)
    values = self.get_values("admintwo")[index]
    dataset = reader.read_dataset(self.overrideinfo["dataset"])
    resource = dataset.get_resource()
    headers, iterator = reader.get_tabular_rows(
        resource["url"], dict_form=True, format="xlsx"
    )
    idpsdict = dict()
    for row in iterator:
        if row["Year"] not in (self.today.year - 1, self.today.year):
            continue
        if "drought" not in row["Reason"].lower():
            continue
        idps = row["Number of Individuals"]
        if not idps:
            continue
        dict_of_lists_add(idpsdict, row["Current (Arrival) District"], idps)
    for adm2name in idpsdict:
        pcode, _ = self.admintwo.get_pcode("SOM", adm2name, identifier)
        if pcode:
            if pcode in values:
                name = self.admintwo.pcode_to_name[pcode]
                logger.warning(
                    f"SOM UNHCR IDPs override - ignoring repeated pcode {pcode}({name}) with name {adm2name}!"
                )
                continue
            values[pcode] = sum(idpsdict[adm2name])
    logger.info(f"Adding SOM IDPs!")
