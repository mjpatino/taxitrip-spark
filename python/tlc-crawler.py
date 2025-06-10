import os
from os import path
import requests
from bs4 import BeautifulSoup

# URL of the TLC trip data page
url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Get the page content
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Find all links to CSV/Parquet files
links = soup.find_all('a', href=True)
data_links = [a['href'] for a in links if any(ext in a['href'] for ext in ['.csv', '.parquet'])]
data_links = [ l for l in data_links if "fhvhv_tripdata" in l ]


# Download directory
dest_folder = path.join( path.dirname(os.getcwd()),"data","fhvhv_tripdata")

os.makedirs( dest_folder, exist_ok=True)

print(f"Downloading {len(data_links)} files")

# Download each file
for (idx, link) in zip(range(1,len(data_links)+1),data_links):

    filename = os.path.join(dest_folder, link.split("/")[-1])
    print(f"{idx} Downloading {filename}...")
    r = requests.get(link)
    with open(filename, 'wb') as f:
        f.write(r.content)
