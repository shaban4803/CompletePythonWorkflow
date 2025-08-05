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
