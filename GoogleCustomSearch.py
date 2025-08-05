import requests
import pandas as pd
import csv

def search_linkedin_profiles(search_engine_id, csv_path, company_name, url):
    query=f'site:linkedin.com/in "{company_name}" (CEO OR CTO OR COO OR CFO OR CIO OR OWNER OR DIRECTOR FINANCE OR ERP MANAGER OR DIRECTOR ERP OR PRESIDENT OR VICE PRESIDENT OR DIRECTOR IT OR "Decision Maker")'

    # Load API keys from CSV
    def load_api_keys(path):
        with open(path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            header = reader.fieldnames
            if 'API_KEY' not in header:
                raise ValueError(f"CSV must contain 'API_KEY'. Found: {header}")
            return [row['API_KEY'] for row in reader if row['API_KEY'].strip()]

    # Try API keys
    def try_api_keys(api_keys, query):
        for key in api_keys:
            params = {
                'key': key,
                'cx': search_engine_id,
                'q': query
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                results = response.json()
                if 'items' not in results:
                    print(f"⚠️ No results with API key: {key}")
                    continue
                print(f"✅ Success with API key: {key}")
                return results['items']
            except requests.exceptions.HTTPError:
                print(f"❌ HTTP error with API key: {key} — {response.status_code}")
            except Exception as e:
                print(f"❌ Other error with API key: {key} — {e}")
        print("⚠️ All API keys failed.")
        return []

    # Load keys and search
    api_keys = load_api_keys(csv_path)
    items = try_api_keys(api_keys, query)

    # Convert to DataFrame
    data = []
    for item in items:
        data.append({
            "Title": item.get("title"),
            "Link": item.get("link"),
            "Snippet": item.get("snippet")
        })

    return pd.DataFrame(data)



