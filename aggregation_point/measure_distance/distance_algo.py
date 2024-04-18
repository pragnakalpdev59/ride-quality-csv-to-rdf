from imports.imports import *  # Importing necessary modules and functions
from measure_distance.update_aggeregation_json import get_average_speed  # Importing a function from a specific module
import threading  # Importing threading for concurrent processing
from rtree import index  # Importing the R-tree index for spatial indexing

# Path to the aggregation point RDF file
aggeregation_point_rdf = '/home/pragnakalp-l-12/Desktop/viren_sir/test/aggregation_point/aggregation_point_rdf/AggregationPoint.json'

# Function to update JSON data with averages and append or create entries
def update_json(json_file_path, json_data, aggregation_point_folder):
    try:
        global aggeregation_point_rdf
        value_length_flag = False
        with open(json_file_path, "r") as file:
            existing_json = json.load(file)

        # Update JSON data with averages and append or create entries
        for key, value in json_data.items():
            if isinstance(value, list):  # Check if value is a list
                avg_value = round((sum(value) / len(value)), 3)
                if key in existing_json:
                    existing_json[key].append(avg_value)
                else:
                    existing_json[key] = [avg_value]
            else:
                existing_json[key] = value  # If value is not a list, update directly

        # Write updated JSON data back to the file
        with open(json_file_path, "w") as file:
            json.dump(existing_json, file, indent=4)
            print("JSON file updated successfully.")

        # Check if any list in the JSON has a length of 6 or more
        for key, value in existing_json.items():
            if isinstance(value, list) and len(value) >= 6:
                value_length_flag = True

        # If any list has a length of 6 or more, perform further processing
        if value_length_flag:
            get_average_speed(aggeregation_point_rdf, json_file_path, aggregation_point_folder)
    except Exception as e:
        print(f"Error in update_json: {e}")

# Function to calculate distance between two points using the Haversine formula
def haversine_distance(lat1, lon1, lat2, lon2):
    try:
        """Calculates the distance between two points on a sphere using the Haversine formula."""
        R = 6378.8  # Earth's radius in kilometers
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * sin(sqrt(a))
        distance = R * c
        return distance
    except Exception as e:
        print(f"Error in haversine_distance: {e}")
# Function to check proximity of data points with target locations using R-tree
def check_proximity_with_rtree(vehicle_data_df):
    try:
        """Checks if the data points in a DataFrame are within a certain distance threshold of target locations using R-tree."""
        global aggeregation_point_rdf
        threshold_distance_km = 0.005  # Set your desired threshold distance in kilometers
        target_df = pd.read_json(aggeregation_point_rdf)
        data_df = vehicle_data_df
        # Create R-tree index for target points
        idx = index.Index()
        for idx_target, row_target in target_df.iterrows():
            lat_target = row_target['Latitude']
            lon_target = row_target['Longitude']
            idx.insert(idx_target, (lat_target, lon_target, lat_target, lon_target))

        close_points = {}  # Use a dictionary to store data points close to each target point

        for data_index, data_row in vehicle_data_df.iterrows():
            data_lat = data_row['Latitude']
            data_lon = data_row['Longitude']
            data_speed = data_row['Speed']

            # Search for nearby target points using R-tree index
            for idx_target in idx.intersection((data_lat, data_lon, data_lat, data_lon)):
                target_row = target_df.loc[idx_target]
                target_lat = target_row['Latitude']
                target_lon = target_row['Longitude']
                geoLocation = target_row['GeoLocation']

                distance = haversine_distance(data_lat, data_lon, target_lat, target_lon)
                if distance <= threshold_distance_km:
                    target_point = (target_lat,target_lon)
                    if geoLocation not in close_points:
                        close_points[geoLocation] = []  # Initialize the key with an empty list
                    close_points[geoLocation].append(data_speed)
                    break  # No need to check other target locations if this point is close enough

        return close_points
    except Exception as e:
        print(f"Error in check_proximity_with_rtree: {e}")

# Update your compare_value function to use the new check_proximity_with_rtree function
def compare_value(vehicle_json_data, average_speed_json, aggregation_point_folder):
    try:
        vehicle_json_data_csv = json.loads(vehicle_json_data)
        vehicle_data_df = pd.DataFrame(vehicle_json_data_csv)

        midpoint = len(vehicle_data_df) // 2

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_top = executor.submit(check_proximity_with_rtree, vehicle_data_df.iloc[:midpoint])
            future_bottom = executor.submit(check_proximity_with_rtree, vehicle_data_df.iloc[midpoint:])

            concurrent.futures.wait([future_top, future_bottom])  # Pass futures in a list

            # Retrieve results from futures
            close_points_top = future_top.result()
            close_points_bottom = future_bottom.result()

        # Combine results from both threads
        close_points = check_proximity_with_rtree(vehicle_data_df)
        json_data = json.dumps(close_points, indent=4)
        if os.path.exists(average_speed_json) and os.path.getsize(average_speed_json) > 0:
            update_json(average_speed_json, close_points, aggregation_point_folder)
        else:
            with open(average_speed_json, "w+") as file:
                file.write(json_data)
    except Exception as e:
        print(f"Error in classify_movement: {e}")
# Call your compare_value function with appropriate arguments
# compare_value(vehicle_json_data, average_speed_json, aggregation_point_folder)
