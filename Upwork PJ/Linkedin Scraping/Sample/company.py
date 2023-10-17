from linkedin_scraper import Person, actions, Company
from selenium import webdriver
import pandas as pd
driver = webdriver.Chrome()

email = "marcotran96@gmail.com"
password = "Ftu5718@"
actions.login(driver, email, password) # if email and password isnt given, it'll prompt in terminal

company = Company("https://www.linkedin.com/company/price-&-company/",driver=driver)

# Extract attributes
name = company.name
about_us = company.about_us  
website = company.website
industry = company.industry
headquarters = company.headquarters
company_size = company.company_size
founded = company.founded
specialties = company.specialties

# Filter employees
employees = [{k: v for k, v in emp.items() if emp} for emp in company.employees if emp]

# Construct DataFrame 
df = pd.DataFrame({
    'name': [name],
    'about_us': [about_us], 
    'website': [website],
    'industry': [industry],
    'headquarters': [headquarters],
    'company_size': [company_size], 
    'founded': [founded],
    'specialties': [specialties],
    'employees': [employees]
})

print(df)