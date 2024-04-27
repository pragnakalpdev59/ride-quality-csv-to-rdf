import ssl #Remove if you ontology is uploaded on a website with ssl certificate.
from imports.imports import *  # Import all necessary libraries

# Importing functions from other modules
from generate_paragraph.first_and_last import get_first_and_last_geolocation  # Function to get first and last geolocation from RDF graph
from generate_paragraph.getPatchCount import patch_counts  # Function to get road condition, ride quality and warnings from RDF graph
from generate_paragraph.numberOfBump import bump_number  # Function to get number of bumps from RDF graph
from generate_paragraph.getspeed import get_average_speed  # Function to get average speed from RDF graph

ssl._create_default_https_context = ssl._create_unverified_context #Remove if you ontology is uploaded on a website with ssl certificate. this line bypases the ssl certificate verification.

def text(ttl_file, text_file_path, i, ontology):
    try:
        # Load the RDF Turtle file
        graph = Graph()
        graph.parse(ontology, format="xml")
        graph.parse(ttl_file, format="turtle")

        # Perform reasoning using HermiT reasoner
        sync_reasoner(graph)

        # Get first and last geolocation
        locations, distance = get_first_and_last_geolocation(graph)
        road_condition, ride_quality, warning = patch_counts(graph)
        number_of_bumps = bump_number(graph)
        avg_speed = get_average_speed(graph)

        # Generate RDF data summary
        rdf_data_summary = f"\nWe have some information on road from {str(locations)}. it is a {round(distance, 2)} KM road section. {road_condition} the road has {int(number_of_bumps)} bumps. The ride quality is {ride_quality} with suggested average speed of {avg_speed} KM/h. {warning}\n"

        # Write summary to text file
        with open(text_file_path, 'a') as file:
            file.write(rdf_data_summary)

    except Exception as e:
        logger.error(f"Error in summary: {e}")

def main():
    try:
        # Define input and output folders
        config = configparser.ConfigParser()
        config.read('config/config.ini')
        ttl_folder = config.get('folders', 'ttl_folder')
        text_folder = config.get('folders', 'text_folder')
        ontology = config.get('urls', 'ontology')
        upload_url = config.get('urls', 'file_upload_api')
        futures = []

        # Process each RDF file in the input folder
        with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            for i, file in enumerate(sorted(os.listdir(ttl_folder), key=lambda x: os.path.getmtime(os.path.join(ttl_folder, x)))):
                if file.endswith(".ttl"):
                    timestamp = str(time.strftime('%Y-%m-%d-%H:%M:%S'))
                    ttl_file = os.path.join(ttl_folder, file)
                    text_file_path = os.path.join(text_folder, f"TextSummary_{timestamp}.txt")
                    futures.append(executor.submit(text, ttl_file, text_file_path, i + 1, ontology))

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

        # Ensure exceptions are propagated and logged
        for future in futures:
            future.result()

        # Upload text files
        file_upload(text_folder, upload_url)

    except Exception as e:
        logger.error(f"Error in main: {e}")

def file_upload(text_folder, upload_url):
    try:
        # API endpoint for file upload

        # Upload each text file in the output folder
        for file in os.listdir(text_folder):
            if file.endswith(".txt"):
                text_file_path = os.path.join(text_folder, file)
                with open(text_file_path, 'rb') as file:
                    files = {'file': file}
                    response = requests.post(upload_url, files=files)
                    response.raise_for_status()  # Raises HTTPError for 4xx and 5xx status codes
                    logger.info(f"File uploaded: {text_file_path}")

    except requests.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
    except requests.ConnectionError as e:
        logger.error(f"Connection Error: {e}")
    except FileNotFoundError as e:
        logger.error(f"File Not Found Error: {e}")
    except Exception as e:
        logger.error(f"Error in file_upload: {e}")

if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter() - start
    print(f"{__file__} executed in {end:0.4f} seconds.")
