import urllib.request
import ssl
from bs4 import BeautifulSoup

# Ignore SSL certificate errors (in case they happen)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Input from user
url = input('Enter URL: ')
count = int(input('Enter count: '))
position = int(input('Enter position: '))

# Loop through the count
for i in range(count):
    print("Retrieving:", url)
    html = urllib.request.urlopen(url, context=ctx).read()
    soup = BeautifulSoup(html, 'html.parser')

    # Find all <a> tags
    tags = soup('a')

    # Position is 1-based, so we subtract 1 for 0-based index
    url = tags[position - 1].get('href')

# Final retrieval
print("Retrieving:", url)

