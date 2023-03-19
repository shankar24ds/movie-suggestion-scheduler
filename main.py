# importing libraries
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import random
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import storage

print("started")
def get_dataset():
  # creating empty lists for movie parameters
  titles = []
  years = []
  time = []
  imdb_ratings = []
  metascores = []
  votes = []
  
  # extracting data, parsing html and storing required movie parameter values in appropriate lists
  headers = {'Accept-Language': 'en-US, en;q=0.5'}
  pages = np.arange(1, 1001, 50)
  
  for page in pages:
    # Getting the contents from the each url
    page = requests.get('https://www.imdb.com/search/title/?groups=top_1000&start=' + str(page) + '&ref_=adv_nxt', headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    # Aiming the part of the html we want to get the information from
    movie_div = soup.find_all('div', class_='lister-item mode-advanced')
      
      # pprint(movie_div)
      
    for container in movie_div:
      # Scraping movie name
      name = container.h3.a.text
      titles.append(name)
      
      # Scraping movie year
      year = container.h3.find('span', class_='lister-item-year').text
      years.append(year)
      
      # Scraping movie length
      runtime = container.find('span', class_='runtime').text if container.p.find('span', class_='runtime') else '-'
      time.append(runtime)
      
      # Scraping rating
      imdb = float(container.strong.text)
      imdb_ratings.append(imdb)
      
      # Scraping metascore
      m_score = container.find('span', class_='metascore').text if container.find('span', class_='metascore') else '-'
      metascores.append(m_score)
      
      # Scraping votes
      nv = container.find_all('span', attrs={'name':'nv'})
      vote = nv[0].text
      votes.append(vote)
          
  # creating a dataframe from the lists
  movies = pd.DataFrame({'movie':titles,
                        'year':years,
                        'runtime':time,
                        'imdb_rating':imdb_ratings,
                        'metascore':metascores,
                        'vote':votes})
  
  # data cleaning
  # cleaning year column
  movies['year'] = movies['year'].str.extract('(\d+)').astype(int)
  
  # cleaning run time column
  movies['runtime'] = movies['runtime'].str.extract('(\d+)').astype(int)
  
  # Cleaning 'metascore' column
  movies['metascore'] = movies['metascore'].str.extract('(\d+)')
  # convert it to float and if there are dashes turn it into NaN
  movies['metascore'] = pd.to_numeric(movies['metascore'], errors='coerce')
  
  # Cleaning 'vote' column
  movies['vote'] = movies['vote'].str.replace(',', '').astype(int)
  
  return movies

def send_mail(movie_name, google_url):
    # Set up the email addresses and login credentials
    sender_address = 'xxx@gmail.com'
    sender_password = 'passwordhere'
    # provide email ids in the list below
    recipient_address_list = []
    
    # Set up the message template
    subject = 'Movie Suggestion for this Week (Automated Email)'
    body_template = """Hi,\n\nHere is a suggestion for a highly rated movie that you might enjoy: {movie_name}\nMovie Details: {google_url}\nTry watching it this weekend if you haven't already.\n\nHave a wonderful weekend.\nThanks :)"""
    
    # Connect to the SMTP server
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.ehlo()
    smtp_server.starttls()
    smtp_server.login(sender_address, sender_password)
    
    for recipient_address in recipient_address_list:
        # Create a new message for each recipient
        message = MIMEMultipart()
    
        # Add the email addresses and message body to the message
        message['From'] = sender_address
        message['To'] = recipient_address
        message['Subject'] = subject
        body = body_template.format(movie_name=movie_name, google_url=google_url)
        message.attach(MIMEText(body, 'plain'))
    
        # Send the message
        smtp_server.send_message(message)
    
    # Disconnect from the SMTP server
    smtp_server.quit()

    
def generate_random_movie(dataframe):
  r = random.randint(300,999)
  return dataframe.iloc[r, 0]

def file_createOrRead(bucket_name):
    # Set the name of the file to be created or read
    file_name = "movies_suggested.txt"

    # Create a client object for interacting with the GCS API
    client = storage.Client()

    # Get the bucket object from the client
    bucket = client.get_bucket(bucket_name)

    # Create a Blob object for the specified file in the bucket
    blob = bucket.blob(file_name)

    # If the file doesn't exist in the bucket, create a new empty file
    if not blob.exists():
        blob.upload_from_string('')

    # Read the contents of the file and return them as a list
    file_contents = blob.download_as_string().decode('utf-8')
    lines = file_contents.split('\n')
    return [line.strip() for line in lines]

def file_save(movie_name, bucket_name):
    # Set the name of the file to be saved
    file_name = "movies_suggested.txt"

    # Create a client object for interacting with the GCS API
    client = storage.Client()

    # Get the bucket object from the client
    bucket = client.get_bucket(bucket_name)

    # Create a Blob object for the specified file in the bucket
    blob = bucket.blob(file_name)

    # Append the movie name to the file
    current_contents = blob.download_as_string().decode('utf-8')
    new_contents = current_contents + movie_name + '\n'
    blob.upload_from_string(new_contents)

def generate_url(movie_name):
  query = movie_name + ' movie'
  google_url = "https://www.google.com/search?q=" + query.replace(" ", "+")
  return google_url
print("functions defined")

df = get_dataset()
print("dataset fetched")

movie_name = generate_random_movie(df)
print("movie name generated")

bucket_name = "my-movie-project-bucket"
movie_list = file_createOrRead(bucket_name)
flag = 0
while movie_name in movie_list:
  movie_name = generate_random_movie(df)
  file_save(movie_name, bucket_name)
  flag = 1
    
if flag == 0:
  file_save(movie_name, bucket_name)

google_url = generate_url(movie_name)
    
send_mail(movie_name, google_url)
print("mail sent")