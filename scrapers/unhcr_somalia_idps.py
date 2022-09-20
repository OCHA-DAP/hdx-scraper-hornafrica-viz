import logging

logger = logging.getLogger(__name__)


def idps_post_run(self) -> None:
    try:
        url = self.overrideinfo["url"]
        reader = self.get_reader(prefix="idps_override")
        json = reader.download_json(url)
        number_idps = int(json["data"][0]["individuals"])
        index = self.get_headers("national")[1].index("#affected+displaced")
        values = self.get_values("national")[index]
        logger.info(f"Adding {number_idps} for SOM IDPs!")
        values["SOM"] = number_idps
        self.get_source_urls().add(url)
        logger.info("Processed UNHCR Somalia IDPs")
    except Exception as ex:
        msg = "Not using UNHCR Somalia IDPs override!"
        logger.exception(msg)
        if self.errors_on_exit:
            self.errors_on_exit.add(f"{msg} Error: {ex}")
