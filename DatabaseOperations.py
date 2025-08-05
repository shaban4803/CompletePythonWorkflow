import psycopg2
import uuid
import json
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("GOOGLE_API_KEY")
QUERY = "tech companies in New York"

def postgres_connection():
    conn = psycopg2.connect(
        host=os.getenv("DATABASE_HOST"),
        database=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        port=os.getenv("DATABASE_PORT")
    )
    cursor = conn.cursor()
    print("Database connected successfully")
    return conn, cursor

def insert_job_prompt(query):
    conn, cursor = postgres_connection()
    cursor.execute('INSERT INTO "public"."Job" ("Prompt") VALUES (%s)', [query])
    conn.commit()
    conn.close()
    print(f" Inserted: '{query}' into Job table.")

def get_latest_job_id(query):
    conn, cursor = postgres_connection()
    cursor.execute('SELECT MAX("Id") FROM "Job" WHERE "Prompt" = %s', [query])
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def insert_into_raw_google_map_search(place_data, job_id):
    conn, cursor = postgres_connection()
    insert_sql = """
        INSERT INTO "Raw"."GoogleMapSearch" (job_id, uuid, data, "CreatedAt", "UpdatedAt")
        VALUES (%s, %s, %s::json, %s, %s)
    """
    now = datetime.utcnow()
    cursor.execute(insert_sql, (
        job_id,
        str(uuid.uuid4()),
        json.dumps(place_data),
        now,
        now
    ))
    conn.commit()
    conn.close()

def get_max_staging_company_id():
    conn, cursor = postgres_connection()
    cursor.execute('SELECT MAX("CompanyId") FROM "Staging"."Company"')
    result = cursor.fetchone()
    conn.close()
    return result[0] + 1 if result[0] is not None else 1

def insert_into_staging_company_data(map_search_df, max_job_id):
    conn, cursor = postgres_connection()
    insert_sql = """
        INSERT INTO "Staging"."Company" 
        ("CompanyName", "CompanyURL", "CompanyLat", "CompanyLong", "CompanyAddress", "ratings", "JobId")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    records = [
        (
            row["Name"],
            row["Website"],
            row["Latitude"],
            row["Longitude"],
            row["Address"],
            row["Rating"],
            max_job_id
        )
        for _, row in map_search_df.iterrows()
    ]
    cursor.executemany(insert_sql, records)
    conn.commit()
    conn.close()
    print(f" Inserted {len(records)} records into Staging.Company.")

def insert_into_staging_email(email_df):
    conn, cursor = postgres_connection()
    insert_sql = """
        INSERT INTO "Staging"."Email" 
        ("Email", "CompanyId", "CreatedAt", "UpdatedAt")
        VALUES (%s, %s, %s, %s)
    """
    get_company_id_sql = """
        SELECT MAX("CompanyId") FROM "Staging"."Company" WHERE "CompanyName" = %s
    """
    now = datetime.utcnow()
    inserted_count = 0
    for _, row in email_df.iterrows():
        company_name = row["Name"]
        email = row["Email"]
        cursor.execute(get_company_id_sql, [company_name])
        result = cursor.fetchone()
        company_id = result[0] if result else None
        if company_id:
            cursor.execute(insert_sql, (
                email,
                company_id,
                now,
                now
            ))
            inserted_count += 1
    conn.commit()
    conn.close()
    print(f" Inserted {inserted_count} records into Staging.email.")

def get_company_latest_data(max_company_id):
    conn, cursor = postgres_connection()
    query = '''
        SELECT "CompanyId", "CompanyName" 
        FROM "Staging"."Company" 
        WHERE "CompanyId" > %s
    '''
    df = pd.read_sql(query, conn, params=[max_company_id])
    conn.close()
    return df

def insert_into_raw_google_data(filtered_linkedin_dict, company_id):
    conn, cursor = postgres_connection()
    insert_sql = """
        INSERT INTO "Raw"."GoogleData" (company_id, data)
        VALUES (%s, %s::json)
    """
    cursor.execute(insert_sql, (
        company_id,
        json.dumps(filtered_linkedin_dict)
    ))
    conn.commit()
    conn.close()

def insert_into_staging_people(filtered_linkedin_df, company_id):
    conn, cursor = postgres_connection()
    insert_sql = """
        INSERT INTO "Staging"."People"
        ("Name", "Designation", "LinkedIn", "CompanyId")
        VALUES (%s, %s, %s, %s)
    """
    records = [
        (
            row["Title"],
            row["Snippet"],
            row["Link"],
            company_id
        )
        for _, row in filtered_linkedin_df.iterrows()
    ]
    cursor.executemany(insert_sql, records)
    conn.commit()
    conn.close()
    print(f" Inserted {len(records)} records into Staging.People.")

def get_latest_people_linkedin_profiles(max_company_id):
    conn, cursor = postgres_connection()
    query = '''
    select * from "Staging"."People" where "CompanyId">%s
    '''
    df = pd.read_sql(query, conn, params=[max_company_id])
    conn.close()
    return df

import re

def save_email_to_database(email, people_id):
    if not is_valid_email(email):
        return

    if email_already_exists(email, people_id):
        return

    company_id = get_company_id_for_person(people_id)

    conn, cur = postgres_connection()
    cur.execute('''
        INSERT INTO "Staging"."Email"
        ("Email", "PeopleId", "CompanyId", "UUID", "CreatedAt", "UpdatedAt")
        VALUES (%s, %s, %s, uuid_generate_v4(), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ''', (email, people_id, company_id))
    conn.commit()
    cur.close()
    conn.close()

def save_phone_to_database(phone, people_id):
    if not is_valid_phone(phone):
        return

    phone = phone.strip()
    if phone_already_exists(phone, people_id):
        return

    company_id = get_company_id_for_person(people_id)

    conn, cur = postgres_connection()
    cur.execute('''
        INSERT INTO "Staging"."Contact"
        ("ContactNumber", "PeopleId", "CompanyId", "UUID", "CreatedAt", "UpdatedAt")
        VALUES (%s, %s, %s, uuid_generate_v4(), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ''', (phone, people_id, company_id))
    conn.commit()
    cur.close()
    conn.close()

def mark_profile_as_processed(people_id):
    conn, cur = postgres_connection()
    cur.execute('UPDATE "Staging"."People" SET "Status" = TRUE WHERE "Id" = %s', (people_id,))
    conn.commit()
    cur.close()
    conn.close()

def is_valid_email(email):
    if not email:
        return False
    return bool(re.match(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", email))

def is_valid_phone(phone):
    if not phone:
        return False
    return len(phone.strip()) >= 7

def email_already_exists(email, people_id):
    conn, cur = postgres_connection()
    cur.execute('''
        SELECT COUNT(*) FROM "Staging"."Email"
        WHERE "Email" = %s AND "PeopleId" = %s
    ''', (email, people_id))
    exists = cur.fetchone()[0] > 0
    cur.close()
    conn.close()
    return exists

def phone_already_exists(phone, people_id):
    conn, cur = postgres_connection()
    cur.execute('''
        SELECT COUNT(*) FROM "Staging"."Contact"
        WHERE "ContactNumber" = %s AND "PeopleId" = %s
    ''', (phone, people_id))
    exists = cur.fetchone()[0] > 0
    cur.close()
    conn.close()
    return exists

def get_company_id_for_person(people_id):
    conn, cur = postgres_connection()
    cur.execute('SELECT "CompanyId" FROM "Staging"."People" WHERE "Id" = %s', (people_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None
