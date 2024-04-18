from csv_handling.csv_handler import start_watching_csv_folder
from imports.imports import *

def csv_to_rdf_conversion(column_labels, csv_folder, classified_csv,rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path):
    start_watching_csv_folder(column_labels, csv_folder, classified_csv,rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path)

def main():
    column_labels = ['time', 'speed', 'lat', 'lon']

    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    csv_folder = 'output/csv'
    os.makedirs(csv_folder, exist_ok=True)

    classified_csv = 'output/classified_csv'
    os.makedirs(classified_csv, exist_ok=True)

    rdf_folder = 'output/rdf_folder'
    ttl_file_path = os.path.join(rdf_folder, "temp.ttl")
    os.makedirs(rdf_folder, exist_ok=True)

    rdf_local_copy = "output/rdf_local_copy"
    os.makedirs(rdf_local_copy, exist_ok=True)

    csv_local_copy = "output/csv_local_copy"
    os.makedirs(csv_local_copy, exist_ok=True)

    json_folder = "output/json"
    os.makedirs(json_folder, exist_ok=True)

    combined_triples_rdf = "output/combined_triples_rdf"
    os.makedirs(combined_triples_rdf, exist_ok=True)

    csv_to_rdf_conversion(column_labels, csv_folder, classified_csv,rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path)

if __name__ == "__main__":
    main()
