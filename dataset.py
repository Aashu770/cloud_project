import urllib.request
from tqdm import tqdm

class DownloadProgressBar(tqdm):
    def update_tp

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-01.csv"

r = requests.get(url)

f = open(local_filename,'w')
f.close()