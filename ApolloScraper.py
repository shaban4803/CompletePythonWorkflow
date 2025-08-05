import pyautogui
import pyperclip
import time
import re

def wait_and_click_image(image_path, timeout=10, confidence=0.6):
    start_time = time.time()
    while time.time() - start_time < timeout:
        location = pyautogui.locateOnScreen(image_path, confidence=confidence)
        if location:
            center = pyautogui.center(location)
            pyautogui.moveTo(center, duration=0.5)
            pyautogui.click()
            return True
        time.sleep(1)
    return False

def click_apollo_buttons():
    if not wait_and_click_image('apollo_button.png', timeout=30):
        return False
    
    time.sleep(9)
    wait_and_click_image('apollo_button_email.png', timeout=8)
    wait_and_click_image('apollo_button_phone.png', timeout=8)
    time.sleep(6)
    return True

def copy_page_content():
    pyperclip.copy("")
    pyautogui.click(1258, 319)  # Adjust coordinates if needed
    time.sleep(1)
    pyautogui.hotkey("ctrl", "a")
    time.sleep(1)
    pyautogui.hotkey("ctrl", "c")
    time.sleep(2)
    return pyperclip.paste()

def extract_emails_from_text(text):
    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    emails = re.findall(email_pattern, text)
    return list(set(emails))

def extract_phones_from_text(text):
    phone_pattern = r"\+\d[\d\s\-()]{7,}"
    phones = re.findall(phone_pattern, text)
    return list(set(phones))

def extract_contact_data_from_profile(linkedin_url):
    print(f"Processing LinkedIn URL: {linkedin_url}")
    
    if not click_apollo_buttons():
        print("Could not find Apollo buttons")
        return [], []
    
    page_content = copy_page_content()
    
    if not page_content.strip():
        print("No content found on page")
        return [], []
    
    emails = extract_emails_from_text(page_content)
    phones = extract_phones_from_text(page_content)
    
    print(f"Found {len(emails)} emails and {len(phones)} phones")
    return emails, phones

def process_all_linkedin_profiles():
    from database import (
        get_unprocessed_linkedin_profiles, 
        save_email_to_database, 
        save_phone_to_database, 
        mark_profile_as_processed
    )
    
    profiles = get_unprocessed_linkedin_profiles(limit=3)
    
    for people_id, linkedin_url in profiles:
        print(f"\nProcessing Profile ID: {people_id}")
        
        emails, phones = extract_contact_data_from_profile(linkedin_url)
        
        for email in emails:
            save_email_to_database(email, people_id)
        
        for phone in phones:
            save_phone_to_database(phone, people_id)
        
        mark_profile_as_processed(people_id)
        print(f"Completed Profile ID: {people_id}")