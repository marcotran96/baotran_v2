from linkedin_scraper import Person, actions
from selenium import webdriver
from selenium.webdriver.common.by import By 
import time 
driver =  webdriver.Chrome()

email = "marcotran96@gmail.com"
password = "Ftu5718@"
actions.login(driver, email, password) # if email and password isnt given, it'll prompt in terminal
user_url = 'https://www.linkedin.com/in/trungnguyen0412/'
person = Person( user_url, driver=driver)

import pandas as pd

# Get name  
name = person.name
# Other attributes 
about = person.about
educations = person.educations
interests = person.interests
accomplishments = person.accomplishments
contacts = person.contacts

current_exps = [exp for exp in person.experiences if exp.to_date == 'Present']
past_exps = [exp for exp in person.experiences if exp.to_date != 'Present']

driver.get(user_url + 'overlay/contact-info')

# Find the phone number element
phone_element = driver.find_element(By.XPATH, "//section[@class='pv-contact-info__contact-type ci-phone']//li[@class='pv-contact-info__ci-container t-14']")
phone_number = phone_element.text.strip()  # Extract the text and remove leading/trailing spaces

# Find the email element
email_element = driver.find_element(By.XPATH, "//section[@class='pv-contact-info__contact-type ci-email']//a[@class='pv-contact-info__contact-link link-without-visited-state t-14']")
email_address = email_element.get_attribute('href').replace('mailto:', '')  # Extract the email address from the href attribute


# DataFrames
name_df = pd.DataFrame({'name': [name]}) 
about_df = pd.DataFrame({'about': [about]})
# Add email_address and phone_number columns to about_df
about_df['email_address'] = email_address
about_df['phone_number'] = phone_number
current_exp_df = pd.DataFrame([exp.__dict__ for exp in current_exps])
past_exp_df = pd.DataFrame([exp.__dict__ for exp in past_exps])
edu_df = pd.DataFrame([edu.__dict__ for edu in educations])
int_df = pd.DataFrame([int.__dict__ for int in interests])
acc_df = pd.DataFrame([acc.__dict__ for acc in accomplishments])
con_df = pd.DataFrame([con.__dict__ for con in contacts])