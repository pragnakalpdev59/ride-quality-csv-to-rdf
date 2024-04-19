# Importing necessary modules
from imports.imports import json, Graph, URIRef, prepareQuery, get_ontology, sync_reasoner, time, logger  # Importing specific functions/classes
from measure_distance.distance_algo import compare_value  # Importing custom distance algorithm functions

# Function to extract geo-location data from RDF and update JSON
def all_geo_location(rdf_file_path, json_file_path, average_speed_json, aggregation_point_folder):
    """
    This function extracts geo-location data (latitude, longitude, and speed) from an RDF file,
    processes it, and writes the results to a JSON file.

    Args:
        rdf_file_path (str): Path to the RDF file containing geo-location data.
        json_file_path (str): Path to the output JSON file.
        average_speed_json (str): Path to the average speed JSON file (used for processing).
        aggregation_point_folder (str): Path to the aggregation point folder (for potential future use).
    """
    try:
        # Load RDF data and ontology into a graph
        ontology = "ontology.owl"  # Path to the ontology file (assumed to be in the same location)
        g = Graph()
        g.parse(ontology, format="xml")  # Load ontology
        g.parse(rdf_file_path, format="turtle")  # Load RDF data

        # Perform reasoning using HermiT reasoner
        sync_reasoner(g)

        # Define namespaces for RDF queries
        RideQuality = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")

        # Prepare SPARQL query to retrieve relevant data
        query_str = """
            PREFIX RideQuality: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#>
            SELECT ?geoLocation ?latitude ?longitude ?speed
            WHERE {
                ?geoLocation RideQuality:hasLatitude ?latitude ;
                            RideQuality:hasLongitude ?longitude ;
                            RideQuality:hasSpeed ?speed .
            }
        """
        query = prepareQuery(query_str, initNs={"RideQuality": RideQuality})

        # Execute the query and store the results in a list
        result = g.query(query)
        graph = []
        for row in result:
            data = {
                "GeoLocation": str(row["geoLocation"]),
                "Latitude": float(row["latitude"]),
                "Longitude": float(row["longitude"]),
                "Speed": float(row["speed"])
            }
            graph.append(data)

        # Convert the graph data to JSON format
        vehicle_json_data = json.dumps(graph, indent=4)

        # Write the JSON data to a file
        with open(json_file_path, 'w') as file:
            file.write(vehicle_json_data)

            # Call a separate function to compare JSON data with average speed data and perform aggregation (potential future enhancement)
            compare_value(vehicle_json_data, average_speed_json, aggregation_point_folder)

    except Exception as e:
        logger.error(f"Error in all_geo_location: {e}")
