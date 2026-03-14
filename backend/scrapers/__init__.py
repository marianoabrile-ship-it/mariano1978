from scrapers.zonaprop import ZonapropScraper
from scrapers.argenprop import ArgenpropScraper
from scrapers.mercadolibre import MercadoLibreScraper
from scrapers.local_agencies import LocalAgenciesScraper

ALL_SCRAPERS = [
    ZonapropScraper,
    ArgenpropScraper,
    MercadoLibreScraper,
    LocalAgenciesScraper,
]
