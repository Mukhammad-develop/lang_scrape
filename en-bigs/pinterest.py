import time
import random
import uuid
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# ✅ CONFIG
KEYWORDS = [
    "classic lasagna recipe",
    "homemade chocolate chip cookies recipe",
    "creamy garlic shrimp pasta recipe",
    "vegetarian chili recipe",
    "spicy thai green curry recipe",
    "fluffy buttermilk pancakes recipe",
    "slow cooker beef stew recipe",
    "authentic guacamole recipe",
    "lemon blueberry pound cake recipe",
    "crispy baked falafel recipe",
    "traditional apple pie recipe",
    "quick stir fry noodles recipe",
    "cheesy broccoli rice casserole recipe",
    "homemade sourdough bread recipe",
    "classic caesar salad recipe",
    "easy banana bread recipe",
    "vegan lentil soup recipe",
    "grilled salmon with dill sauce recipe",
    "stuffed bell peppers recipe",
    "creamy mushroom risotto recipe",
    "spaghetti carbonara recipe",
    "chicken tikka masala recipe",
    "roasted vegetable quinoa bowl recipe",
    "fudgy brownies recipe",
    "buffalo chicken dip recipe",
    "homemade pizza dough recipe",
    "classic french onion soup recipe",
    "easy egg fried rice recipe",
    "baked ziti with ricotta recipe",
    "chicken enchiladas recipe",
    "greek salad with feta recipe",
    "pumpkin spice muffins recipe",
    "shrimp tacos with slaw recipe",
    "beef and broccoli stir fry recipe",
    "homemade hummus recipe",
    "classic meatloaf recipe",
    "chicken alfredo pasta recipe",
    "easy vegetable curry recipe",
    "baked mac and cheese recipe",
    "chocolate lava cake recipe",
    "spicy ramen noodle soup recipe",
    "classic potato salad recipe",
    "chicken parmesan recipe",
    "homemade granola bars recipe",
    "creamy tomato basil soup recipe",
    "easy fish tacos recipe",
    "classic blueberry muffins recipe",
    "roasted garlic mashed potatoes recipe",
    "chicken and dumplings recipe",
    "classic tiramisu recipe",
    "easy chicken pot pie recipe",
    "homemade cinnamon rolls recipe",
    "classic beef bourguignon recipe",
    "vegetarian stuffed zucchini boats recipe",
    "spicy korean bibimbap recipe",
    "creamy spinach artichoke dip recipe",
    "easy pad thai recipe",
    "classic chicken noodle soup recipe",
    "homemade bagels recipe",
    "chocolate peanut butter cookies recipe",
    "grilled vegetable skewers recipe",
    "classic shepherd's pie recipe",
    "easy shrimp scampi recipe",
    "vegan chocolate cake recipe",
    "homemade pesto pasta recipe",
    "classic chicken fried rice recipe",
    "easy lemon bars recipe",
    "spicy buffalo cauliflower wings recipe",
    "classic new york cheesecake recipe",
    "homemade chicken nuggets recipe",
    "easy beef tacos recipe",
    "classic tomato bruschetta recipe",
    "creamy chicken and rice soup recipe",
    "homemade soft pretzels recipe",
    "classic pecan pie recipe",
    "easy vegetable lasagna recipe",
    "spicy szechuan noodles recipe",
    "classic chicken marsala recipe",
    "homemade apple crisp recipe",
    "easy turkey meatballs recipe",
    "classic minestrone soup recipe",
    "creamy garlic mashed cauliflower recipe",
    "homemade blueberry pancakes recipe",
    "classic carrot cake recipe",
    "easy chicken fajitas recipe",
    "spicy peanut noodles recipe",
    "classic beef stroganoff recipe",
    "homemade lemon curd recipe",
    "easy vegetable stir fry recipe",
    "classic chicken piccata recipe",
    "creamy broccoli cheddar soup recipe",
    "homemade cinnamon bread recipe",
    "classic key lime pie recipe",
    "easy teriyaki chicken recipe",
    "spicy jalapeno poppers recipe",
    "classic split pea soup recipe",
    "homemade garlic knots recipe",
    "easy chicken shawarma recipe",
    "classic chocolate mousse recipe",
    "creamy corn chowder recipe",
    "homemade pita bread recipe",
    "classic bolognese sauce recipe",
    "easy chicken tortilla soup recipe",
    "spicy harissa roasted carrots recipe",
    "classic lemon meringue pie recipe",
    "homemade focaccia bread recipe",
    "easy beef and bean chili recipe",
    "classic chicken cacciatore recipe",
    "creamy clam chowder recipe",
    "homemade vanilla ice cream recipe",
    "classic tabbouleh salad recipe",
    "easy chicken satay recipe",
    "spicy thai peanut chicken recipe",
    "classic bread pudding recipe",
    "homemade gnocchi recipe",
    "easy chicken and waffles recipe",
    "classic gazpacho recipe",
    "creamy spinach lasagna recipe",
    "homemade coconut macaroons recipe",
    "classic chicken paprikash recipe",
    "easy beef bourguignon recipe",
    "spicy chipotle chicken tacos recipe",
    "classic rice pudding recipe",
    "homemade naan bread recipe",
    "easy chicken tikka skewers recipe",
    "classic shrimp and grits recipe",
    "creamy chicken gnocchi soup recipe",
    "homemade biscotti recipe",
    "classic beef wellington recipe",
    "easy chicken lettuce wraps recipe",
    "spicy sriracha noodles recipe",
    "classic tres leches cake recipe",
    "homemade pita chips recipe",
    "easy chicken korma recipe",
    "classic eggplant parmesan recipe",
    "creamy tomato tortellini soup recipe",
    "homemade caramel sauce recipe",
    "classic chicken adobo recipe",
    "easy beef empanadas recipe",
    "spicy thai basil chicken recipe",
    "classic coconut cream pie recipe",
    "homemade rye bread recipe",
    "easy chicken mole recipe",
    "classic shrimp creole recipe",
    "creamy chicken tetrazzini recipe",
    "homemade almond croissants recipe",
    "classic beef chili recipe",
    "easy chicken biryani recipe",
    "spicy kung pao chicken recipe",
    "classic pineapple upside down cake recipe",
    "homemade pita pizza recipe",
    "easy chicken tagine recipe",
    "classic chicken fricassee recipe",
    "creamy chicken and leek pie recipe",
    "homemade chocolate truffles recipe",
    "classic beef tacos recipe",
    "easy chicken marsala recipe",
    "spicy thai red curry recipe",
    "classic lemon drizzle cake recipe",
    "homemade garlic breadsticks recipe",
    "easy chicken parmesan sliders recipe",
    "classic chicken pot pie soup recipe",
    "creamy chicken and wild rice soup recipe",
    "homemade oatmeal raisin cookies recipe",
    "classic beef and barley soup recipe",
    "easy chicken and broccoli casserole recipe",
    "spicy cajun jambalaya recipe",
    "classic coconut rice pudding recipe",
    "homemade pita wraps recipe",
    "easy chicken and spinach lasagna recipe",
    "classic chicken and rice casserole recipe",
    "creamy chicken and mushroom pie recipe",
    "homemade chocolate eclairs recipe",
    "classic beef stew with dumplings recipe",
    "easy chicken and vegetable stir fry recipe",
    "spicy thai green papaya salad recipe",
    "classic chicken and biscuits recipe",
    "homemade lemon poppy seed muffins recipe",
    "easy chicken and sausage gumbo recipe",
    "classic chicken and waffles recipe",
    "creamy chicken and broccoli bake recipe",
    "homemade apple cinnamon rolls recipe",
    "classic beef and vegetable soup recipe",
    "easy chicken and cheese quesadillas recipe",
    "spicy buffalo chicken wings recipe",
    "classic chicken and dumplings soup recipe",
    "homemade chocolate chip banana bread recipe",
    "easy chicken and rice soup recipe",
    "classic chicken and stuffing casserole recipe",
    "creamy chicken and corn chowder recipe",
    "homemade blueberry scones recipe",
    "classic beef and mushroom pie recipe",
    "easy chicken and vegetable curry recipe",
    "spicy thai chicken satay recipe",
    "classic chicken and broccoli alfredo recipe",
    "homemade cinnamon sugar donuts recipe",
    "easy chicken and vegetable soup recipe",
    "classic chicken and wild rice casserole recipe",
    "creamy chicken and spinach pasta recipe",
    "homemade chocolate cupcakes recipe",
    "classic beef and potato stew recipe",
    "easy chicken and vegetable pasta recipe",
    "spicy thai chicken noodle soup recipe",
    "classic chicken and rice soup recipe",
    "homemade lemon bars recipe",
    "easy chicken and vegetable fried rice recipe",
    "classic chicken and rice bake recipe",
    "creamy chicken and rice casserole recipe",
    "homemade banana nut muffins recipe",
    "classic beef and vegetable casserole recipe",
    "easy chicken and vegetable stir fry recipe",
    "spicy thai chicken curry recipe",
    "classic chicken and rice pilaf recipe",
    "homemade chocolate brownies recipe",
    "easy chicken and vegetable soup recipe",
    "classic chicken and rice skillet recipe",
    "creamy chicken and rice soup recipe",
    "homemade blueberry muffins recipe",
    "classic beef and vegetable stew recipe",
    "easy chicken and vegetable casserole recipe",
    "spicy thai chicken fried rice recipe",
    "classic chicken and rice soup recipe",
    "homemade lemon muffins recipe",
    "easy chicken and vegetable bake recipe",
    "classic chicken and rice casserole recipe",
    "creamy chicken and rice bake recipe",
    "homemade chocolate chip cookies recipe",
    "classic beef and vegetable pie recipe",
    "easy chicken and vegetable pasta bake recipe",
    "spicy thai chicken soup recipe",
    "classic chicken and rice bake recipe",
    "homemade banana bread recipe",
    "easy chicken and vegetable curry recipe",
    "classic chicken and rice soup recipe",
    "creamy chicken and rice casserole recipe",
    "homemade blueberry pancakes recipe",
    "classic beef and vegetable soup recipe",
    "easy chicken and vegetable stir fry recipe",
    "spicy thai chicken curry recipe",
    "classic chicken and rice pilaf recipe",
    "homemade chocolate brownies recipe",
    "easy chicken and vegetable soup recipe",
    "classic chicken and rice skillet recipe",
    "creamy chicken and rice soup recipe",
    "homemade blueberry muffins recipe",
    "classic beef and vegetable stew recipe",
    "easy chicken and vegetable casserole recipe",
    "spicy thai chicken fried rice recipe",
    "classic chicken and rice soup recipe",
    "homemade lemon muffins recipe",
    "easy chicken and vegetable bake recipe",
    "classic chicken and rice casserole recipe",
    "creamy chicken and rice bake recipe",
    "homemade chocolate chip cookies recipe",
    "classic beef and vegetable pie recipe",
    "easy chicken and vegetable pasta bake recipe",
    "spicy thai chicken soup recipe",
    "classic chicken and rice bake recipe",
    "homemade banana bread recipe"
]
OUTPUT_FILE = "pinterest_recipes.jsonl"
SCROLL_PAUSE = 3
MAX_SCROLLS_PER_KEYWORD = 1000

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--start-maximized")
    service = Service("/opt/homebrew/bin/chromedriver")  # adjust path if needed
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def save_jsonl(record):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def scrape_keyword(driver, keyword):
    print(f"\n🔎 Searching: {keyword}")
    driver.get(f"https://www.pinterest.com/search/pins/?q={keyword.replace(' ', '%20')}")

    seen_urls = set()
    scroll_count = 0

    while scroll_count < MAX_SCROLLS_PER_KEYWORD:
        time.sleep(SCROLL_PAUSE)
        pins = driver.find_elements(By.CSS_SELECTOR, "div[data-test-id='pin']")

        new_pins = 0
        for pin in pins:
            try:
                desc = pin.text.strip()
                if not desc:
                    continue

                link_elem = pin.find_element(By.CSS_SELECTOR, "a")
                pin_url = link_elem.get_attribute("href")

                # ✅ check duplicates
                if pin_url in seen_urls:
                    continue

                seen_urls.add(pin_url)
                new_pins += 1

                record = {
                    "id": str(uuid.uuid4()),
                    "lang": "en",
                    "source_url": pin_url,
                    "title": "",
                    "text": desc,
                    "clean_status": "pending",
                    "category": keyword
                }
                save_jsonl(record)

            except Exception:
                continue

        # if nothing new → stop this keyword early
        if new_pins == 0:
            print(f"⚠️ No new pins after scroll {scroll_count}, ending search for '{keyword}'")
            break

        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(2)

        scroll_count += 1
        print(f"   Scrolled {scroll_count} times for '{keyword}', saved {new_pins} new pins")

    print(f"✅ Finished keyword '{keyword}', total unique pins: {len(seen_urls)}")

def main():
    driver = init_driver()
    for kw in KEYWORDS:
        scrape_keyword(driver, kw)
        wait = random.randint(5, 15)
        print(f"⏸️ Waiting {wait}s before next keyword...")
        time.sleep(wait)
    driver.quit()

if __name__ == "__main__":
    main()