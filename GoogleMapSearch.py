import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os
import re
import requests
from bs4 import BeautifulSoup

# Load environment variables from .env file
load_dotenv()

def extract_email_from_website(url):
    try:
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.text, "html.parser")
        emails = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", soup.text))
        return list(emails)[0] if emails else None
    except:
        return None


def get_places_info(query, max_results, api_key=os.getenv("GOOGLE_MAP_API")):
    print(f"🔍 Searching for: '{query}'")

    search_url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={api_key}"
    search_response = requests.get(search_url)
    search_data = search_response.json()
    results = search_data.get('results', [])[:max_results]

    if not results:
        print("❌ No results found.")
        return pd.DataFrame()

    data = []

    print(f"✅ Found {len(results)} results. Fetching details...\n")

    for idx, place in enumerate(results, start=1):
        place_id = place.get("place_id")
        name = place.get("name", "N/A")
        print(f"➡️ [{idx}/{len(results)}] Processing: {name}")

        if not place_id:
            print("⚠️ Skipping (missing place_id)")
            continue

        details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={api_key}"
        details_response = requests.get(details_url)
        details = details_response.json().get("result", {})

        data.append({
            "Name": details.get("name"),
            "Phone": details.get("formatted_phone_number"),
            "Website": details.get("website"),
            "Address": details.get("formatted_address"),
            "Latitude": details.get("geometry", {}).get("location", {}).get("lat"),
            "Longitude": details.get("geometry", {}).get("location", {}).get("lng"),
            "Rating": details.get("rating"),
            "Email": extract_email_from_website(details.get("website")) if details.get("website") else None

        })

        time.sleep(1)

    print("\n✅ All details fetched. Returning DataFrame.\n")
    return pd.DataFrame(data)

# Example usage
if __name__ == "__main__":
    query = "tech companies in New York"
    map_search_df = get_places_info(query, max_results=5)
    print(map_search_df.head().to_string())
