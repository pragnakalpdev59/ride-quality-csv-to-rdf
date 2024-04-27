# Importing all modules from imports.imports module
from measure_distance.rdf_generator import rdf_generation
# Importing all modules from imports.imports module
from imports.imports import *  # Import all functions/classes from imports.imports

config = configparser.ConfigParser()
config.read('config/config.ini')
# Path to the old data folder
old_data = config.get('folders', 'old_data')
# Function to calculate average speed and update JSON data
def get_average_speed(AggregationPoint_json, average_speed_json, aggregation_point_folder):
    try:
        # Open the average speed JSON file for reading
        with open(average_speed_json, 'r') as jsonFile:
            speedData = json.load(jsonFile)  # Load speed data from JSON file

        # Calculate average speed for each location
        for key, value in speedData.items():
            if isinstance(value, list):  # Check if value is a list of speed measurements
                avg_value = round(sum(value)/len(value), 3)  # Calculate average speed
                speedData[key] = avg_value  # Update speed data with average value

        # Update AggregationPoint JSON with new speed values
        update_json(AggregationPoint_json, speedData, average_speed_json, aggregation_point_folder)
    except Exception as e:
        logger.error(f"Error in get_average_speed: {e}")

# Function to update JSON data with new speed values
def update_json(AggregationPoint_json, new_speed_values, average_speed_json, aggregation_point_folder):
    try:
        # Open the AggregationPoint JSON file for reading
        with open(AggregationPoint_json, 'r') as aggJsonFile:
            original_data = json.load(aggJsonFile)  # Load original data from JSON file

        # Create a dictionary for faster lookup by GeoLocation key
        speed_dict = {item['GeoLocation']: item for item in original_data}

        # Update speed values in original data using dictionary lookup
        for item in original_data:
            if item['GeoLocation'] in new_speed_values:
                item['speed'] = new_speed_values[item['GeoLocation']]  # Update speed value

        # Convert updated JSON data to string format
        updated_json_data = json.dumps(original_data, indent=4)  # Indent for readability

        # Perform further processing and generate RDF data (function not shown)
        make_new_aggeregation_rdf(updated_json_data, aggregation_point_folder, average_speed_json)
    except Exception as e:
        logger.error(f"Error in update_json: {e}")

# Function to calculate sliding window severity
def sliding_window(speed):
    """
    This function calculates the severity of speed changes within a sliding window.

    Args:
        speed: A list of speed values.

    Returns:
        A list of severity values corresponding to each speed value in the input list.

    Raises:
        Exception: If there is an error during the calculation.
    """
    try:
        window_size = 5  # Define the window size for calculating severity

        # Handle cases where the speed list is shorter than the window size
        if len(speed) <= window_size:
            return [0] * len(speed)  # Return a list of zeros for padding

        severities = []
        for i in range(len(speed) - window_size + 1):
            window = speed[i:i+window_size]  # Get a window of speed values

            # Calculate the mean difference in speed within the window
            mean_diff = sum((window[j] - window[j - 1]) for j in range(1, len(window))) / (window_size - 1)
            severities.append(mean_diff)

        # Handle the last element in the speed list
        last_diff = speed[-1] - speed[-2]
        severities.extend([last_diff] * (len(speed) - len(severities)))  # Pad with last difference

        return severities
    except Exception as e:
        logger.error(f"Error in sliding_window: {e}")

# Function to classify movement based on severity
def classification(df):
    """
    This function classifies movement based on the calculated severity.

    Args:
        df: A pandas DataFrame containing a 'speed' column.

    Raises:
        Exception: If there is an error during classification.
    """
    try:
        speed = df['speed'].tolist()  # Extract speed list from DataFrame
        severities = sliding_window(speed)  # Calculate severity for each speed value

        # Classify movement based on predefined severity ranges
        movement_classifications = [classify_movement(c) for c in severities]
        df['MovementClassification'] = movement_classifications  # Add classification column to DataFrame

        # Consider caching the modified DataFrame for efficiency (commented out for now)
        # cached_df = df
    except Exception as e:
        logger.error(f"Error in classification: {e}")

def classify_movement(severity):
    """
    This function assigns a movement classification based on the severity value.

    Args:
        severity: A single severity value.

    Returns:
        A string representing the movement classification.

    Raises:
        Exception: If there is an error during classification.
    """
    try:
        # Define severity ranges and corresponding movement classifications
        severity_ranges = {
            (3.0, float('inf')): "High Acceleration",
            (1.0, 3.0): "Normal Acceleration",
            (0, 1.0): "Steady Speed",
            (-3.0, 0): "Normal Braking",
            (-float('inf'), -3.0): "High Braking",
        }

        for range_, movement in severity_ranges.items():
            if range_[0] <= severity < range_[1]:
                return movement
    except Exception as e:
        logger.error(f"Error in classify_movement: {e}")

# Function to move files to old_data folder
def move_stuff(aggregation_point_folder):
    """
    This function moves files from the aggregation_point folder to an old_data folder with a timestamp.

    Args:
        aggregation_point_folder: The path to the folder containing the files to be moved.

    Raises:
        Exception: If there is an error during file movement.

    # Potential improvements:
    # 1. Error handling for file operations (e.g., source file not found)
    # 2. Logging moved files for tracking purposes
    # 3. Handling existing old_data folders (e.g., overwrite or create subfolders)
    """
    try:
        global old_data
        timestamp = str(time.strftime('%Y-%m-%d-%H:%M:%S'))
        old_data_folder = os.path.join(old_data, timestamp)
        os.makedirs(old_data_folder)  # Create the old data folder if it doesn't exist

        for files in sorted(os.listdir(aggregation_point_folder)):
            source_file = os.path.join(aggregation_point_folder, files)
            destination_file = os.path.join(old_data_folder, files)
            shutil.move(source_file, destination_file)  # Move the file

            # Add logging for tracking purposes (commented out for now)
            logger.info(f"Moved file: {source_file} to {destination_file}")
    except Exception as e:
        logger.error(f"Error in move_stuff: {e}")


# Function to generate new aggregation RDF data
def make_new_aggeregation_rdf(updated_json_data, aggregation_point_folder, average_speed_json):
    try:
        move_stuff(aggregation_point_folder)
        os.remove(average_speed_json)
        json_io = StringIO(updated_json_data)
        df = pd.read_json(json_io)
        classification(df)
        rdf_generation(df, aggregation_point_folder)
        AggeregationPoint_json = "AggregationPoint.json"
        AggeregationPoint_json = os.path.join(aggregation_point_folder, AggeregationPoint_json)
        with open(AggeregationPoint_json, 'w+') as jsonfile:
            jsonfile.write(updated_json_data)

    except Exception as e:
        logger.error(f"Error in make_new_aggregation_rdf: {e}")