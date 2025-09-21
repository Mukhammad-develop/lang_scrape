"""
Configuration for BBC Good Food Scraper
"""

# ScraperAPI Configuration
SCRAPERAPI_KEY = '6056294ef3a1aeb5f0656753043c087e'

# Scraper Settings
MAX_WORKERS = 6  # Reduced for proxy stability
DELAY_BETWEEN_REQUESTS = 2.0  # Increased delay for proxy
MAX_PAGES = 10  # Number of pages to scrape

# Search URL
SEARCH_URL = "https://www.bbcgoodfood.com/search?tab=recipe&mealType=lunch%2Cafternoon-tea%2Cbreads%2Cbreakfast%2Cbrunch%2Cbuffet%2Ccanapes%2Ccheese-course%2Ccocktails%2Ccondiment%2Cdessert%2Cdinner%2Cdrink%2Cfish-course%2Chdpsummer24%2Cmain-course%2Cpasta%2Cside-dish%2Csnack%2Csoup%2Cstarter%2Csupper%2Ctreat%2Cvegetable%2Cpicnic%2Cside&cuisine=afghan%2Cafrican%2Camerican%2Casian%2Caustralian%2Caustrian%2Cazerbaijan%2Cbelgian%2Cbrazilian%2Cbritish%2Ccajun-creole%2Ccaribbean%2Cchinese%2Cczech%2Cdanish%2Ceastern-european%2Cegyptian%2Cenglish%2Cfrench%2Cgerman%2Cgreek%2Chungarian%2Cindian%2Cindonesian%2Cirish%2Citalian%2Cjamaican%2Cjapanese%2Cjewish%2Ckorean%2Clatin-american%2Clithuanian%2Cmalaysian%2Cmediterranean%2Cmexican%2Cmiddle-eastern%2Cmoroccan%2Cnepalese%2Cnigerian%2Cnorth-african%2Cpersian%2Cperuvian%2Cpolish%2Cportuguese%2Cscandinavian%2Cscottish%2Csouthern-soul%2Cspanish%2Csri-lankan%2Cswedish%2Cswiss%2Ctaiwanese%2Cthai%2Ctunisian%2Cturkish%2Cukrainian%2Cvietnamese%2Cwelsh%2Cbalkan&ratings=gte-1%2Cgte-2%2Cgte-3%2Cgte-4%2Cgte-5&page=1"

# Output settings
OUTPUT_FILENAME = "bbc_recipes_proxy.jsonl"
