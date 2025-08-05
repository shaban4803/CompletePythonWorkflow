from GoogleMapSearch import get_places_info
from DatabaseOperations import insert_job_prompt,get_latest_job_id,get_max_staging_company_id,insert_into_staging_company_data,insert_into_staging_email,insert_into_raw_google_map_search,get_company_latest_data,insert_into_raw_google_data,insert_into_staging_people,get_latest_people_linkedin_profiles,save_email_to_database,save_phone_to_database,mark_profile_as_processed
from GoogleCustomSearch import search_linkedin_profiles
from ApolloScraper import extract_contact_data_from_profile

def main():
    sector="Mobile companies"
    location="New York"
    query = f"{sector} in {location}"
    # Google Map Search
    # max limit to get results is 60
    insert_job_prompt(query)
    max_job_id=get_latest_job_id(query)

    map_search_df = get_places_info(query, max_results=2)
    print(map_search_df.head().to_string())
    map_search_dict=map_search_df.to_dict(orient='records')
    insert_into_raw_google_map_search(map_search_dict,max_job_id)



    max_company_id = get_max_staging_company_id()

    insert_into_staging_company_data(map_search_df,max_job_id)

    email_df = map_search_df.dropna(subset=['Email'])

    email_df = email_df.dropna(subset=['Email']).reset_index(drop=True)

    insert_into_staging_email(email_df)




    company_latest_data=get_company_latest_data(max_company_id)

    SEARCH_ENGINE_ID = '24ec5267676044cc7'
    CSV_PATH = r'D:\SalesGMBDataScraper/api_keys.csv'
    url='https://www.googleapis.com/customsearch/v1'

    for index,row in company_latest_data.iterrows():
        company_id=row['CompanyId']
        company_name=row['CompanyName']


        people_linkedin_df = search_linkedin_profiles(SEARCH_ENGINE_ID, CSV_PATH, company_name, url)
        print(people_linkedin_df.columns)
        filtered_linkedin_df = people_linkedin_df[
            ~people_linkedin_df['Link'].apply(lambda x: isinstance(x, str) and ('job' in x.lower() or 'post' in x.lower()))]

        insert_into_raw_google_data(filtered_linkedin_df.to_dict(),company_id)

        insert_into_staging_people(filtered_linkedin_df,company_id)


    linkedin_df=get_latest_people_linkedin_profiles(max_company_id)

    # Process LinkedIn profiles through Apollo to get contact information
    print("\nStarting Apollo scraping process...")
    
    for index, row in linkedin_df.iterrows():
        people_id = row['Id']
        linkedin_url = row['LinkedIn']
        
        print(f"\nProcessing Profile ID: {people_id}")
        
        emails, phones = extract_contact_data_from_profile(linkedin_url)
        
        for email in emails:
            save_email_to_database(email, people_id)
        
        for phone in phones:
            save_phone_to_database(phone, people_id)
        
        mark_profile_as_processed(people_id)
        print(f"Completed Profile ID: {people_id}")
    
    print("Apollo scraping process completed.")


if __name__ == '__main__':
    main()
