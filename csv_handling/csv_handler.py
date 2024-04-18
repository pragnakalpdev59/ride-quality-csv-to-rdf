from imports.imports import *  # Import statements for necessary modules and functions
from rdf_handling.rdf_generator import rdf_generation  # Import RDF generation function
from rdf_handling.combined_geoLocation_json_creation import json_generation  # Import JSON generation function
from rdf_handling.combined_rdf_generator import combined_triples_generator  # Import combined triples generation function

time_list = []  # Initialize list to store processing times
changes_detected_list = []  # Initialize list to track detected changes
cached_df = None  # Initialize DataFrame for caching data
last_row_count = 0  # Initialize variable to track last row count

def data_labeling(column_labels):
    """Label columns of the cached DataFrame."""
    global cached_df
    df = cached_df
    df = df.set_axis(column_labels, axis=1)  # Set column labels
    cached_df = df  # Update cached DataFrame

def sliding_window(speed):
    """Implement sliding window technique for data processing."""
    try:
        window_size = 5
        if len(speed) <= window_size:
            return [0] * len(speed)  # Return zeros if window size is larger than data

        severities = []
        for i in range(len(speed) - window_size + 1):
            window = speed[i:i+window_size]
            mean_diff = sum((window[j] - window[j - 1]) for j in range(1, len(window))) / (window_size - 1)
            severities.append(mean_diff)  # Calculate mean differences

        last_diff = speed[-1] - speed[-2]
        severities.extend([last_diff] * (len(speed) - len(severities)))  # Extend with last difference
        return severities  # Return calculated severities
    except Exception as e:
        print(f"Error in sliding_window: {e}")  # Log error

def classification():
    """Perform classification based on data."""
    global cached_df
    try:
        df = cached_df
        speed = df['speed'].tolist()  # Get speed data
        severities = sliding_window(speed)  # Get severities using sliding window
        movement_classifications = [classify_movement(c) for c in severities]  # Classify movements
        df['MovementClassification'] = movement_classifications  # Add classification to DataFrame
        cached_df = df  # Update cached DataFrame
    except FileNotFoundError as e:
        print(f"Error: {csv_file_path} not found. {e}")  # Log error if file not found

def classify_movement(severity):
    """Classify movement based on severity."""
    try:
        severity_ranges = {
            (3.0, float('inf')): "High Acceleration",
            (1.0, 3.0): "Normal Acceleration",
            (0, 1.0): "Steady Speed",
            (-3.0, 0): "Normal Braking",
            (-float('inf'), -3.0): "High Braking",
        }

        for range_, movement in severity_ranges.items():
            if range_[0] <= severity < range_[1]:
                return movement  # Return movement classification
    except Exception as e:
        print(f"Error in classify_movement: {e}")  # Log error

def start_watching_csv_folder(column_labels, csv_folder, classified_csv, rdf_folder, rdf_local_copy, csv_local_copy, json_folder, combined_triples_rdf, ttl_file_path, no_change_detected_time=5):
    """Start watching the CSV folder for changes."""
    try:
        global cached_df
        event_handler = MyHandler(column_labels, classified_csv, rdf_folder, rdf_local_copy, ttl_file_path)
        observer = Observer()
        observer.schedule(event_handler, path=csv_folder, recursive=False)
        observer.start()

        try:
            print(f'Watching for changes in {csv_folder}. Press Ctrl+C to stop.')
            last_change_time = time.time()  # Initialize last change time

            while True:
                current_time = time.time()
                # Check for changes every second
                if current_time - last_change_time > no_change_detected_time:
                    # Process files and generate RDF if no changes detected
                    for csv_filename in sorted(os.listdir(csv_folder)):
                        if csv_filename.endswith(".csv"):
                            csv_file_path = os.path.join(csv_folder, csv_filename)
                            destination_path = os.path.join(classified_csv, csv_filename)
                            cached_df.to_csv(destination_path, index=False)  # Save classified CSV

                    # Move files and generate RDF if changes detected
                    for rdf_filename in sorted(os.listdir(rdf_folder)):
                        if rdf_filename.endswith(".ttl"):
                            temp_file_path = os.path.join(rdf_folder, rdf_filename)
                            road_section_list = end_filename(destination_path)
                            output_filename = f"RoadSection_Start:{road_section_list[0]}_End:{road_section_list[1]}_{timestamp}"
                            output_filename = output_filename.replace(' ', '')
                            rdf_local_copy_filename = f"{output_filename}.ttl"
                            rdf_local_copy_path = os.path.join(rdf_local_copy, rdf_local_copy_filename)
                            shutil.move(temp_file_path, rdf_local_copy_path)  # Move RDF file
                            csv_local_copy_filename = f"{output_filename}.csv"
                            csv_local_copypath = os.path.join(csv_local_copy, os.path.basename(csv_local_copy_filename))
                            shutil.move(destination_path, csv_local_copypath)  # Move classified CSV
                            json_file_name = f"{output_filename}.json"
                            json_file_path = os.path.join(json_folder, json_file_name)
                            json_generation(csv_local_copypath, json_file_path)  # Generate JSON file
                            rdf_file_path = os.path.join(combined_triples_rdf, rdf_local_copy_filename)
                            combined_triples_generator(json_file_path, rdf_file_path)  # Generate combined RDF

                            event_handler.cleanup_original(csv_file_path)  # Cleanup original file
                            make_csv_for_graphs(time_list, changes_detected_list)  # Generate CSV for graphs

                time.sleep(1)  # Sleep for 1 second

                # Check if any changes have occurred
                if event_handler.has_changes():
                    last_change_time = current_time
                    event_handler.clear_changes()  # Clear changes flag
        except KeyboardInterrupt:
            pass  # Exit on Ctrl+C

        for filename in os.listdir(csv_folder):
            if filename.endswith((".csv")):
                csv_file_path = os.path.join(csv_folder, filename)
                event_handler.cleanup_original(csv_file_path)  # Cleanup remaining files

    except Exception as e:
        print(f"Error in start_watching_csv_folder: {e}")  # Log error

def end_filename(destination_path):
    """Extract road section details from destination path."""
    road_section_list = []
    with open(destination_path, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile)
        rows = list(csvreader)  # Convert to list for indexing
        if rows:
            first_row = rows[0]
            last_row = rows[-1]
            road_section_list.append((first_row['lat'], first_row['lon']))
            road_section_list.append((last_row['lat'], last_row['lon']))
    return road_section_list  # Return road section details

def make_csv_for_graphs(time_list, changes_detected_list):
    """Generate CSV file for graphs."""
    x_axis = changes_detected_list
    y_axis = time_list

    rows = zip(x_axis, y_axis)
    graph_folder = '/home/pragnakalp-l-12/Desktop/viren_sir/test/output/graphs/'
    os.makedirs(graph_folder, exist_ok=True)
    csv_file_path = "/home/pragnakalp-l-12/Desktop/viren_sir/test/output/graphs/figure1.csv"
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['x_axis', 'y_axis'])  # Write the header row
        writer.writerows(rows)  # Write the data rows

class MyHandler(FileSystemEventHandler):
    """Custom event handler for file system monitoring."""

    def __init__(self, column_labels, classified_csv, rdf_folder, rdf_local_copy, ttl_file_path):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.ttl_file_path = ttl_file_path
        self.column_labels = column_labels
        self.classified_csv = classified_csv
        self.rdf_folder = rdf_folder
        self.changes_detected = False
        self.rdf_local_copy = rdf_local_copy
        self.modification_count = 0
        self.last_row_list = []

    def remove_duplicates(self):
        """Remove duplicate and empty rows from cached DataFrame."""
        global cached_df
        try:
            df = cached_df
            df = df.drop_duplicates(subset=['lat', 'lon'])  # Remove duplicate rows
            df = df[(df['lat'] != 0.0) & (df['lat'] != 0.1)]  # Remove rows with specific values
            df = df.dropna(subset=['lat', 'lon'])  # Remove rows with empty values
            cached_df = df  # Update cached DataFrame
        except Exception as e:
            print(f"Error in remove_duplicates_and_empty_values: {e}")  # Log error

    def data_cleaning(self):
        """Perform data cleaning operations on cached DataFrame."""
        global cached_df
        try:
            df = cached_df
            df = df.iloc[:, :5]  # Select first 5 columns
            cached_df = df  # Update cached DataFrame
        except FileNotFoundError as e:
            print(f"Error: {csv_file_path} not found. {e}")  # Log error if file not found

    def update_cached_df(self, csv_file_path):
        """Update cached DataFrame with data from CSV file."""
        global cached_df, last_row_count
        if last_row_count == 0:
            df = pd.read_csv(csv_file_path)
            df = df.iloc[:, 1:5]
            cached_df = df
        else:
            df = pd.read_csv(csv_file_path)
            df = df.iloc[:, 1:5]
            cached_df = df

        last_row_count += 1  # Increment last row count

    def on_modified(self, event):
        """Handle modified event for file system monitoring."""
        global cached_df
        try:
            start_time = time.time()
            if event.is_directory:
                return

            if event.event_type == 'modified':
                print(f'File {event.src_path} has been modified. Processing...')
                # Process the modified file
                csv_file_path = event.src_path
                destination_path = os.path.join(self.classified_csv, os.path.basename(csv_file_path))

                if not os.path.exists(destination_path):
                    with open(destination_path, 'w'):  # Create the file
                        pass

                self.update_cached_df(csv_file_path)  # Update cached DataFrame
                self.data_cleaning()  # Perform data cleaning
                data_labeling(self.column_labels)  # Label data columns
                self.remove_duplicates()  # Remove duplicates
                classification()  # Perform classification
                self.executor.submit(rdf_generation, cached_df, self.rdf_folder, self.ttl_file_path)  # Generate RDF
                end_time = time.time()

                self.changes_detected = True  # Set changes detected flag
                self.modification_count += 1  # Increment modification count
                changes_detected_list.append(self.modification_count)  # Update changes detected list
                time_list.append(end_time-start_time)  # Update processing time list

        except Exception as e:
            print(f"Error in MyHandler.on_modified: {e}")  # Log error

    def has_changes(self):
        """Check if changes have been detected."""
        return self.changes_detected

    def clear_changes(self):
        """Clear changes detected flag."""
        self.changes_detected = False

    def cleanup_original(self, csv_file_path):
        """Cleanup original CSV file."""
        try:
            os.remove(csv_file_path)  # Remove original CSV file
            print(f'Original file {csv_file_path} deleted.')
        except Exception as e:
            print(f'Error deleting original file {csv_file_path}: {e}')  # Log error deleting file
