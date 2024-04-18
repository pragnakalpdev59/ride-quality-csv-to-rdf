# Importing necessary modules
from measure_distance.json_from_ttl import all_geo_location  # Importing custom function to process RDF and generate JSON
from imports.imports import *  # Importing standard modules (likely containing definitions for functions used throughout the script)

# Main function to process RDF files and generate JSON outputs
def main(rdf_file_path, vehicle_json, average_speed_json, aggregation_point_folder):
    """
    This function is likely the entry point for processing individual RDF files.
    It takes an RDF file path, vehicle JSON path, average speed JSON path, and aggregation point folder path as input.
    It likely calls the `all_geo_location` function to handle the core logic of processing the RDF data and generating the vehicle JSON file.
    The `average_speed_json` and `aggregation_point_folder` might be used by `all_geo_location` for calculations or further processing related to the generated JSON data.

    Args:
        rdf_file_path (str): Path to the RDF file to be processed.
        vehicle_json (str): Path to the output vehicle JSON file that will be generated.
        average_speed_json (str): Path to the average speed JSON file (might be used for calculations within all_geo_location).
        aggregation_point_folder (str): Path to the folder containing aggregation point data (might be used for processing within all_geo_location).
    """
    try:
        all_geo_location(rdf_file_path, vehicle_json, average_speed_json, aggregation_point_folder)
    except Exception as e:
        print(f"Error in main: {e}")

def watching_rdf_folder(rdf_folder, vehicle_json_folder, average_speed_json, aggregation_point_folder):
    """
    This function monitors a folder for changes (new RDF files) and triggers processing for those new files using a process pool executor for concurrency.

    Args:
        rdf_folder (str): Path to the folder containing RDF files that will be monitored for changes.
        vehicle_json_folder (str): Path to the folder where output JSON files will be saved for each processed RDF file.
        average_speed_json (str): Path to the average speed JSON file (might be used for calculations within the triggered main function).
        aggregation_point_folder (str): Path to the folder containing aggregation point data (might be used for processing within the triggered main function).
    """
    try:
        # Create an event handler object to handle file creation events (new RDF files)
        event_handler = MyHandler(rdf_folder, vehicle_json_folder, average_speed_json, aggregation_point_folder)

        # Create an observer object to monitor the specified folder
        observer = Observer()
        observer.schedule(event_handler, path=rdf_folder, recursive=False)  # Monitor only the top level of the folder for new files

        observer.start()

        try:
            print(f"Watching for changes in {rdf_folder}. Press Ctrl+C to stop.")

            # Continuously monitor the folder
            while True:
                current_time = time.time()

            # Check if the event handler has detected any changes during this loop (less likely to be used here)
            if event_handler.has_changes():
                last_time_change = current_time
                event_handler.clear_changes()  # Reset the change flag

        except KeyboardInterrupt:
            print(f"Keyboard interrupt detected")

    except Exception as e:
        print(f"Error in watching_rdf_folder: {e}")

class MyHandler(FileSystemEventHandler):
    """
    This class handles file creation events in specified folders.
    It monitors the `rdf_folder` for new ".ttl" files and triggers processing
    using a concurrent process pool.
    """

    def __init__(self, rdf_folder, vehicle_json_folder, average_speed_json, aggregation_point_folder):
        """
        Initializes the handler with folder paths and processing parameters.

        Args:
            rdf_folder (str): Path to the folder containing RDF files.
            vehicle_json_folder (str): Path to the output folder for vehicle JSON files.
            average_speed_json (str): Path to the average speed JSON file (used for processing).
            aggregation_point_folder (str): Path to the aggregation point folder.
        """
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count())
        self.rdf_folder = rdf_folder
        self.vehicle_json_folder = vehicle_json_folder
        self.average_speed_json = average_speed_json
        self.aggregation_point_folder = aggregation_point_folder
        self.changes_detected = False

    def on_created(self, event):
        """
        Handles file creation events. Processes new ".ttl" files.

        Args:
            event (FileSystemEvent): The event object representing the file system event.
        """
        try:
            if not event.is_directory:
                print(f'File {event.src_path} has been added. Processing')
                file_name = os.path.basename(event.src_path)
                if file_name.endswith(".ttl"):
                    rdf_file_path = os.path.join(self.rdf_folder, file_name)  # Get full path of RDF file
                    json_file_name = file_name.replace("ttl", "json")  # Change file extension to JSON
                    json_file_path = os.path.join(self.vehicle_json_folder, json_file_name)
                    self.executor.submit(main, rdf_file_path, json_file_path, self.average_speed_json, self.aggregation_point_folder)
                    self.changes_detected = True
                    # main(rdf_file_path, json_file_path, self.average_speed_json, self.aggregation_point_folder)
                else:
                    print(f'Ignoring non-TTL file: {file_name}')
        except Exception as e:
            print(f"Error in MyHandler.on_created: {e}")

    def has_changes(self):
        """
        Checks if new files have been processed since the last check.

        Returns:
            bool: True if new files have been processed, False otherwise.
        """

    def clear_changes(self):
        """
        Resets the flag indicating if new files have been processed.
        """
        self.changes_detected = False

# Entry point of the script
if __name__ == "__main__":
    # Define paths and folders
    rdf_folder = "output/rdf_local_copy"
    vehicle_json_folder = "output/json"
    average_speed_json = "/home/pragnakalp-l-12/Desktop/viren_sir/test/aggregation_point/average_speed_json/average_speed.json"
    aggregation_point_folder = "aggregation_point_rdf"
    # Call main function with specified parameters
    watching_rdf_folder(rdf_folder, vehicle_json_folder, average_speed_json, aggregation_point_folder)
