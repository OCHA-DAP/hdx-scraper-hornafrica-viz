import hxl
from hdx.location.country import Country
from hdx.scraper.utilities.reader import Read
from hxl import InputOptions


def custom_sources(configuration):
    reader = Read.get_reader("hdx")
    path = reader.download_file(configuration["url"], file_prefix="custom_sources")
    data = hxl.data(path, InputOptions(allow_local=True)).cache()
    sources = list()
    for row in data:
        source = row.get("#meta+source")
        if not source or source == "-":
            continue
        base_hxltag = row.get("#indicator+name")
        view = row.get("#meta+view")
        if view.lower() == "regional":
            hxltag = f"{base_hxltag}+regional"
        else:
            countryiso3, _ = Country.get_iso3_country_code_fuzzy(row.get("#country+name"))
            if not countryiso3:
                continue
            hxltag = f"{base_hxltag}+{countryiso3.lower()}"
        date = row.get("#date")
        source_url = row.get("#meta+url")
        sources.append((hxltag, date, source, source_url))
    return sources
