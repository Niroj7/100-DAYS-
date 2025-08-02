import urllib.request
import xml.etree.ElementTree as ET

# Prompt for URL
url = input('Enter location: ')
print('Retrieving', url)

# Read the data from the URL
data = urllib.request.urlopen(url).read()
print('Retrieved', len(data), 'characters')

# Parse the XML
tree = ET.fromstring(data)

# Find all <count> tags using XPath
counts = tree.findall('.//count')

# Initialize sum
total = 0

# Loop through each <count> tag and sum the values
for count in counts:
    total += int(count.text)

# Output
print('Count:', len(counts))
print('Sum:', total)
