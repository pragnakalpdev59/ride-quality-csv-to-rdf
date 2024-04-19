# Importing necessary modules and functions
from imports.imports import *
# Importing a specific function from a module
from measure_distance.update_aggeregation_json import get_average_speed

# Path to the aggregation point RDF file (assuming it's a JSON file)
aggeregation_point_rdf = '/home/pragnakalp-l-12/Desktop/viren_sir/test/aggregation_point/aggregation_point_rdf/AggregationPoint.json'


# Function to update JSON data with averages and append or create entries
def update_json(json_file_path, json_data, aggregation_point_folder):
    """
    This function updates a JSON file with averages and appends or creates entries.

    Args:
        json_file_path (str): Path to the JSON file.
        json_data (dict): Dictionary containing data to be updated or appended.
        aggregation_point_folder (str): Path to the folder containing aggregation point data (likely unused in this function).
    """
    try:
        global aggeregation_point_rdf  # Assuming this variable is used within this function

        value_length_flag = False

        # Read existing JSON data
        with open(json_file_path, "r") as file:
            existing_json = json.load(file)

        # Update or append data based on type (list or not)
        for key, value in json_data.items():
            if isinstance(value, list):
                # If value is a list, calculate average and update/append
                avg_value = round((sum(value) / len(value)), 3)
                if key in existing_json:
                    existing_json[key].append(avg_value)
                else:
                    existing_json[key] = [avg_value]
            else:
                # If value is not a list, update directly
                existing_json[key] = value

        # Write updated data back to the JSON file
        with open(json_file_path, "w") as file:
            json.dump(existing_json, file, indent=4)
            logger.info("JSON file updated successfully.")

        # Check if any list in the JSON has a length of 6 or more (might be for triggering further processing)
        for key, value in existing_json.items():
            if isinstance(value, list) and len(value) >= 6:
                value_length_flag = True

        # If any list has a length of 6 or more, perform further processing (function not shown here)
        if value_length_flag:
            get_average_speed(aggeregation_point_rdf, json_file_path, aggregation_point_folder)

    except Exception as e:
        logger.error(f"Error in update_json: {e}")


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
        return distance

    except Exception as e:
        logger.error(f"Error in haversine_distance: {e}")

# Function to check proximity of data points with target locations using R-tree
def check_proximity_with_rtree(vehicle_data_df):
    """Checks if the data points in a DataFrame are within a certain distance threshold of target locations using R-tree.

    This function takes a DataFrame containing vehicle data (vehicle_data_df) and checks if any of the data points
    are within a specified threshold distance (in kilometers) of target locations stored in a separate data source.
    It utilizes an R-tree for efficient spatial search.

    Args:
        vehicle_data_df (pandas.DataFrame): A DataFrame containing vehicle data with columns like 'Latitude' and 'Longitude'.

    Returns:
        dict: A dictionary where keys are target locations (GeoLocation) and values are lists containing speeds of nearby vehicles.

    Raises:
        Exception: Any exception encountered during processing.
    """

    try:
        global aggeregation_point_rdf

        # Set the desired threshold distance in kilometers (adjust this value as needed)
        threshold_distance_km = 0.005

        # Read target locations from the global variable
        target_df = pd.read_json(aggeregation_point_rdf)

        # Make a copy of the input DataFrame to avoid modifying the original data
        data_df = vehicle_data_df.copy()

        # Create an R-tree index for efficient spatial search based on target locations
        idx = index.Index()
        for idx_target, row_target in target_df.iterrows():
            lat_target = row_target['Latitude']
            lon_target = row_target['Longitude']

            # Insert target location coordinates into the R-tree index (bounding box format)
            idx.insert(idx_target, (lat_target, lon_target, lat_target, lon_target))

        # Initialize an empty dictionary to store data points close to each target location
        close_points = {}

        # Loop through each data point (vehicle) in the input DataFrame
        for data_index, data_row in vehicle_data_df.iterrows():
            data_lat = data_row['Latitude']
            data_lon = data_row['Longitude']
            data_speed = data_row['Speed']

            # Search for nearby target locations using the R-tree index based on the current data point's location
            for idx_target in idx.intersection((data_lat, data_lon, data_lat, data_lon)):
                target_row = target_df.loc[idx_target]
                target_lat = target_row['Latitude']
                target_lon = target_row['Longitude']
                geoLocation = target_row['GeoLocation']

                # Calculate the Haversine distance between the data point and the target location
                distance = haversine_distance(data_lat, data_lon, target_lat, target_lon)

                # Check if the distance is less than or equal to the threshold distance
                if distance <= threshold_distance_km:
                    target_point = (target_lat, target_lon)  # Create a tuple for the target location

                    # If this target location doesn't have an entry in the results yet, initialize an empty list
                    if geoLocation not in close_points:
                        close_points[geoLocation] = []

                    # Append the speed of the current data point to the list associated with the target location
                    close_points[geoLocation].append(data_speed)

                    # Since we only care about the closest target location within the threshold, break after finding one
                    break  # Exit the inner loop (no need to check other targets for this data point)

        return close_points

    except Exception as e:
        logger.error(f"Error in check_proximity_with_rtree: {e}")

# Update your compare_value function to use the new check_proximity_with_rtree function
def compare_value(vehicle_json_data, average_speed_json, aggregation_point_folder):
    """
    This function compares vehicle data with average speeds and updates or creates a JSON file.

    Args:
        vehicle_json_data (str): JSON string containing vehicle data.
        average_speed_json (str): Path to the JSON file storing average speeds (might be created if it doesn't exist).
        aggregation_point_folder (str): Path to the folder containing aggregation point data (likely unused in this function).
    """
    try:
        # Load vehicle data from JSON string into a pandas DataFrame
        vehicle_json_data_csv = json.loads(vehicle_json_data)
        vehicle_data_df = pd.DataFrame(vehicle_json_data_csv)

        # Split the DataFrame into two halves for concurrent processing
        midpoint = len(vehicle_data_df) // 2

        # Use a thread pool executor for concurrent processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Submit tasks to check proximity for each half of the data using check_proximity_with_rtree
            future_top = executor.submit(check_proximity_with_rtree, vehicle_data_df.iloc[:midpoint])
            future_bottom = executor.submit(check_proximity_with_rtree, vehicle_data_df.iloc[midpoint:])

            # Wait for both tasks to finish
            concurrent.futures.wait([future_top, future_bottom])

            # Retrieve results from completed tasks (dictionaries containing proximity data)
            close_points_top = future_top.result()
            close_points_bottom = future_bottom.result()

        # Since results might be processed further, combine results from both halves
        close_points = check_proximity_with_rtree(vehicle_data_df)  # Recheck entire data (optional)

        # Convert proximity data dictionary to JSON string
        json_data = json.dumps(close_points, indent=4)

        # Check if the average speed JSON file exists and has data
        if os.path.exists(average_speed_json) and os.path.getsize(average_speed_json) > 0:
            # If it exists and has data, update it using the update_json function
            update_json(average_speed_json, close_points, aggregation_point_folder)
        else:
            # If the file doesn't exist or is empty, create it with the proximity data
            with open(average_speed_json, "w+") as file:
                file.write(json_data)

    except Exception as e:
        logger.error(f"Error in compare_value: {e}")

