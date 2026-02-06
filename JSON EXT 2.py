
"""This is a simple Python script that asks the user for a location (like a university or landmark), then contacts an OpenGeo API to get geographic data. From the response, it extracts and prints the plus_code, which is a short, digital address based on the location.

The program uses Python’s built-in urllib and json libraries to:

Send a request to the API with the given location

Parse the returned JSOnn

Pull out the plus_code from the first result

Display the final plus code in the console

It’s a great beginner-friendly example for learning how to work with web APIs and JSON in Python.

"""
import urllib.request
import urllib.parse
import json

# Base API URL
serviceurl = "http://py4e-data.dr-chuck.net/opengeo?"

# Get location from user
address = input("Enter location: ")

# Prepare URL
params = {'q': address}
url = serviceurl + urllib.parse.urlencode(params)

print("Retrieving", url)

# Fetch and decode data
uh = urllib.request.urlopen(url)
data = uh.read().decode()
print("Retrieved", len(data), "characters")

# Parse JSON
js = json.loads(data)

# Navigate through the structure to get the plus_code
plus_code = js["features"][0]["properties"]["plus_code"]
print("Plus code", plus_code)

