# Importing all modules from imports.imports module
from imports.imports import *
from measure_distance.rdf_generator import rdf_generation

# Path to the old data folder
old_data = "/home/pragnakalp-l-12/Desktop/viren_sir/test/aggregation_point/old_data"

# Function to calculate average speed and update JSON data
def get_average_speed(AggregationPoint_json, average_speed_json, aggregation_point_folder):
    try:
        with open(average_speed_json, 'r') as jsonFile:
            speedData = json.load(jsonFile)

        # Calculate average speed and update speedData dictionary
        for key, value in speedData.items():
            if isinstance(value, list):
                avg_value = round(sum(value)/len(value), 3)
                if key in speedData:
                    speedData[key] = avg_value

        # Update AggregationPoint JSON with new speed values
        update_json(AggregationPoint_json, speedData, average_speed_json, aggregation_point_folder)
    except Exception as e:
        print(f"Error in get_average_speed: {e}")

# Function to update JSON data with new speed values
def update_json(AggregationPoint_json, new_speed_values, average_speed_json, aggregation_point_folder):
    try:
        with open(AggregationPoint_json, 'r') as aggJsonFile:
            original_data = json.load(aggJsonFile)

        # Create a dictionary for faster lookup
        speed_dict = {item['GeoLocation']: item for item in original_data}

        # Update speed values in original data using dictionary lookup
        for item in original_data:
            if item['GeoLocation'] in new_speed_values:
                item['speed'] = new_speed_values[item['GeoLocation']]

        # Convert updated JSON data to string format
        updated_json_data = json.dumps(original_data, indent=4)

        # Perform further processing and generate RDF data
        make_new_aggeregation_rdf(updated_json_data, aggregation_point_folder, average_speed_json)
    except Exception as e:
        print(f"Error in update_json: {e}")

# Function to calculate sliding window severity
def sliding_window(speed):
    try:
        window_size = 5
        if len(speed) <= window_size:
            return [0] * len(speed)

        severities = []
        for i in range(len(speed) - window_size + 1):
            window = speed[i:i+window_size]
            mean_diff = sum((window[j] - window[j - 1]) for j in range(1, len(window))) / (window_size - 1)
            severities.append(mean_diff)

        last_diff = speed[-1] - speed[-2]
        severities.extend([last_diff] * (len(speed) - len(severities)))
        return severities
    except Exception as e:
        print(f"Error in sliding_window: {e}")

# Function to classify movement based on severity
def classification(df):
    try:
        speed = df['speed'].tolist()
        severities = sliding_window(speed)
        movement_classifications = [classify_movement(c) for c in severities]
        df['MovementClassification'] = movement_classifications

        cached_df = df
    except Exception as e:
        print(f"Error in classification: {e}")

def classify_movement(severity):
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
                return movement
    except Exception as e:
        print(f"Error in classify_movement: {e}")

# Function to move files to old_data folder
def move_stuff(aggregation_point_folder):
    try:
        global old_data
        timestamp = str(time.strftime('%Y-%m-%d-%H:%M:%S'))
        old_data_folder = os.path.join(old_data, timestamp)
        os.makedirs(old_data_folder)
        for files in sorted(os.listdir(aggregation_point_folder)):
            shutil.move(os.path.join(aggregation_point_folder, files), old_data_folder)
    except Exception as e:
        print(f"Error in move_stuff: {e}")

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
        print(f"Error in make_new_aggregation_rdf: {e}")

# Entry point of the script
if __name__ == "__main__":
    # Define paths and folders
    AggregationPoint_json = "/home/pragnakalp-l-12/Desktop/viren_sir/test/aggeregation_point/aggeregation_point_rdf/AggeregationPoint.json"
    average_speed_json = "/home/pragnakalp-l-12/Desktop/viren_sir/test/aggeregation_point/avarage_spped_json/output.json"
    final_output_agg_file = "/home/pragnakalp-l-12/Desktop/viren_sir/test/aggeregation_point/aggeregation_point_rdf"
    # Call function to calculate average speed and update JSON data
    get_average_speed(AggregationPoint_json, average_speed_json)
