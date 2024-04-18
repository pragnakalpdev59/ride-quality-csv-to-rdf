# Importing necessary modules
from measure_distance.json_from_ttl import all_geo_location  # Importing custom function
from imports.imports import json, time, os  # Importing standard modules

# Main function to process RDF files and generate JSON outputs
def main(rdf_folder, vehicle_json_folder, average_speed_json, aggregation_point_folder):
    try:
        # Loop through RDF files in the specified folder
        for rdf_file in sorted(os.listdir(rdf_folder)):
            if rdf_file.endswith(".ttl"):  # Check if the file is a Turtle file
                rdf_file_path = os.path.join(rdf_folder, rdf_file)  # Get full path of RDF file
                rdf_file = rdf_file.replace("ttl", "json")  # Change file extension to JSON
                json_file_path = os.path.join(vehicle_json_folder, rdf_file)  # JSON file path
                # Call custom function to extract geo-location data and update JSON
                all_geo_location(rdf_file_path, json_file_path, average_speed_json, aggregation_point_folder)
    except Exception as e:
        print(f"Error in main: {e}")

# Entry point of the script
if __name__ == "__main__":
    # Define paths and folders
    rdf_folder = "output/rdf_local_copy"
    vehicle_json_folder = "output/json"
    average_speed_json = "/home/pragnakalp-l-12/Desktop/viren_sir/test/aggregation_point/average_speed_json/average_speed.json"
    aggregation_point_folder = "aggregation_point_rdf"
    # Call main function with specified parameters
    main(rdf_folder, vehicle_json_folder, average_speed_json, aggregation_point_folder)
