import requests
from bs4 import BeautifulSoup
import json

url = "https://www.bbcgoodfood.com/recipes/seared-scallops-leeks-lemon-chilli-butter"

# Fetch page
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# Find JSON-LD script blocks
data = []
for script in soup.find_all("script", type="application/ld+json"):
    try:
        block = json.loads(script.string)
        # Sometimes it's a list of objects
        if isinstance(block, list):
            data.extend(block)
        else:
            data.append(block)
    except Exception:
        continue

# Filter for recipe objects
recipes = [d for d in data if d.get("@type") == "Recipe"]

if recipes:
    recipe = recipes[0]  # Take the first recipe found
    print("Title:", recipe.get("name"))
    print("Description:", recipe.get("description"))
    print("\nIngredients:")
    for ing in recipe.get("recipeIngredient", []):
        print("-", ing)
    print("\nInstructions:")
    if "recipeInstructions" in recipe:
        for step in recipe["recipeInstructions"]:
            if isinstance(step, dict):
                print("-", step.get("text"))
            else:
                print("-", step)
else:
    print("No recipe data found")
