# Import libraries
from imports.imports import requests, logger

def get_city(latitude, longitude):
    """
    This function retrieves the city name based on provided latitude and longitude coordinates.

    Args:
        latitude (float): The latitude coordinate.
        longitude (float): The longitude coordinate.

    Returns:
        str: The city name if successful, otherwise None.
    """

    try:
        # Define the global API key (replace with your actual API key)
        global API_KEY

        # Construct the API request URL using f-strings
        url = f"https://api.geoapify.com/v1/geocode/reverse?lat={latitude}&lon={longitude}&apiKey={API_KEY}"

        # Set headers for the request
        headers = {"Accept": "application/json"}

        # Send a GET request to the API using requests library
        resp = requests.get(url, headers=headers)

        # Check if the response was successful (status code 200)
        if resp.status_code == 200:
            # Convert the response to JSON data
            json_data = resp.json()

            # Extract the city name from the JSON response
            city = json_data['features'][0]['properties']['city']
            return city
        else:
            # Log an error message if the request failed
            print(f"Error: {resp.status_code} - {resp.reason}")

    except requests.RequestException as e:
        # Log an error if there's an issue with the request itself
        logger.error(f"Request Error: {e}")
    except KeyError as e:
        # Log an error if a key is missing in the JSON data
        logger.error(f"Key Error: {e}")
    except Exception as e:
        # Log an unexpected error
        logger.error(f"Unexpected Error: {e}")


API_KEY = "892891b29d594306b579c43b8f5d021a"