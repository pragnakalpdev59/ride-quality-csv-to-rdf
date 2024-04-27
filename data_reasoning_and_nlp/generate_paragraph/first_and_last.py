# Import all functions from the imports module (not recommended)
from imports.imports import *
from generate_paragraph.get_address import get_city

def get_first_and_last_geolocation(graph):
    """
    This function retrieves the first and last geolocation (latitude and longitude) from the provided RDF graph.

    Args:
        graph: An RDFlib Graph object containing the data

    Returns:
        A string in the format "(first_latitude, first_longitude) to (last_latitude, last_longitude)" or None if no geolocations are found.
    """
    try:
        # Define the RideQuality namespace for easier reference
        RideQuality = Namespace("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")

        # Define a SPARQL query to retrieve all latitude and longitude values
        query = prepareQuery(
            """
            PREFIX RideQuality: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#>
            SELECT ?latitude ?longitude
            WHERE {
                ?geoLocation RideQuality:hasLatitude ?latitude ;
                            RideQuality:hasLongitude ?longitude .
            }
            ORDER BY ?geoLocation
            """
        )

        # Execute the SPARQL query on the graph
        results = graph.query(query)

        # Initialize variables to store first and last geo coordinates
        first_latitude = None
        first_longitude = None
        last_latitude = None
        last_longitude = None

        # Iterate through the query results
        for i, row in enumerate(results):
            # Assign values to first coordinates if not set yet
            if first_latitude is None:
                first_latitude = float(row["latitude"])
                first_longitude = float(row["longitude"])
            # Update last coordinates on every iteration
            last_latitude = float(row["latitude"])
            last_longitude = float(row["longitude"])

        # Format the output string with first and last coordinates
        starting_city_name = get_city(first_latitude, first_longitude)
        ending_city_name = get_city(last_latitude, last_longitude)

        city_names = f"{starting_city_name} to {ending_city_name}"

        # Call measure_distance function to calculate total distance
        return city_names, measure_distance(results)

    except Exception as e:
        # Log any errors encountered during execution
        logger.error(f"Error in first_and_last.py: {e}")

def measure_distance(results):
    """
    This function calculates the total distance traveled based on consecutive geolocation points.

    Args:
        results: Query results containing latitude and longitude data.

    Returns:
        float: The total distance traveled.
    """
    previous_lat, previous_lon = None, None
    total_distance = 0

    for row in results:
        current_lat = float(row["latitude"])
        current_lon = float(row["longitude"])

        if previous_lat is not None and previous_lon is not None:
            # Calculate distance between consecutive points and add to total
            distance = haversine_distance(previous_lat, previous_lon, current_lat, current_lon)
            total_distance += distance

        previous_lat, previous_lon = current_lat, current_lon

    # Return the total distance traveled
    return total_distance

# Function to calculate distance between two points using the Haversine formula
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    This function calculates the distance between two points on a sphere using the Haversine formula.

    Args:
        lat1 (float): Latitude of the first point.
        lon1 (float): Longitude of the first point.
        lat2 (float): Latitude of the second point.
        lon2 (float): Longitude of the second point.

    Returns:
        float: The distance between the two points in kilometers.
    """
    try:
        R = 6378.8  # Earth's radius in kilometers

        # Convert degrees to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * sin(sqrt(a))
        distance = R * c

        # Return the calculated distance
        return distance

    except Exception as e:
        logger.error(f"Error in haversine_distance: {e}")



#16.6 KM
if __name__ == "__main__":
    # lat1 = 20.96018033
    # lon1 = 73.13291262
    # lat2 = 20.77539094
    # lon2 = 73.32413434
    # haversine_distance(lat1, lon1, lat2, lon2)
    ttl_file = "/home/pragnakalp-l-12/Desktop/viren_sir/test/data_reasoning_and_nlp/rdf_local_copy/RoadSection_Start22.8194766774.26211833_End22.6850466774.317725_2024-04-20-142124.ttl"
    main(ttl_file)
