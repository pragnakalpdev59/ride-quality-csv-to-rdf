from csv_handling.csv_handler import start_watching_csv_folder  # Import the function for watching CSV folder
from imports.imports import logger, os, configparser  # Import necessary modules

def csv_to_rdf_conversion(column_labels, csv_folder, classified_csv, rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path):
    try:
        # Start watching the CSV folder for changes and perform CSV to RDF conversion
        start_watching_csv_folder(column_labels, csv_folder, classified_csv, rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path)
    except Exception as e:
        logger.error(f"Error in csv_to_rdf_conversion: {e}")

def main():
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        # Define column labels for CSV data
        column_labels = ['time', 'speed', 'lat', 'lon']

        # Create necessary output folders if they don't exist
        output_folder = config.get('folders', 'output')
        csv_folder = os.path.join(output_folder, 'csv')
        classified_csv = os.path.join(output_folder, 'classified_csv')
        rdf_folder = os.path.join(output_folder, 'rdf_folder')
        rdf_local_copy = os.path.join(output_folder, 'rdf_local_copy')
        csv_local_copy = os.path.join(output_folder, 'csv_local_copy')
        json_folder = os.path.join(output_folder, 'json')
        combined_triples_rdf = os.path.join(output_folder, 'combined_triples_rdf')

        # Create output folders if they don't exist
        for folder in [output_folder, csv_folder, classified_csv, rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf]:
            os.makedirs(folder, exist_ok=True)

        ttl_file_path = os.path.join(rdf_folder, "temp.ttl")

        # Perform CSV to RDF conversion
        csv_to_rdf_conversion(column_labels, csv_folder, classified_csv, rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path)
    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
