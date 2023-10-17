from linkedin_scraper import Person, actions
from selenium import webdriver


chrome_driver_path =  r'C:\Users\baotd\Documents\GitHub\Y4A_BA_Team\BA_Team\BaoTran\chrome_driver\chromedriver117.exe'
driver = webdriver.Chrome( executable_path=chrome_driver_path)
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

# DataFrames
name_df = pd.DataFrame({'name': [name]}) 

about_df = pd.DataFrame({'about': [about]})

current_exp_df = pd.DataFrame([exp.__dict__ for exp in current_exps])
past_exp_df = pd.DataFrame([exp.__dict__ for exp in past_exps])

edu_df = pd.DataFrame([edu.__dict__ for edu in educations])

int_df = pd.DataFrame([int.__dict__ for int in interests])

acc_df = pd.DataFrame([acc.__dict__ for acc in accomplishments])

con_df = pd.DataFrame([con.__dict__ for con in contacts])

print(name_df)
print(about_df)

print(current_exp_df)
print(past_exp_df)