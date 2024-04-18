from imports.imports import *
from measure_distance.update_aggeregation_json import get_average_speed
import threading

# Path to the aggregation point RDF file

def update_json(json_file_path, json_data, aggregation_point_folder):
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

aggeregation_point_rdf = '/home/pragnakalp-l-12/Desktop/viren_sir/test/aggregation_point/aggregation_point_rdf/AggregationPoint.json'

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates the distance between two points on a sphere using the Haversine formula."""
    R = 6378.8  # Earth's radius in kilometers
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * sin(sqrt(a))
    distance = R * c
    return distance

def check_proximity(vehicle_data_df):
    global aggeregation_point_rdf
    """Checks if the data points in a CSV file are within a certain distance threshold of target locations."""
    threshold_distance_km = 0.005  # Set your desired threshold distance in kilometers
    target_df = pd.read_json(aggeregation_point_rdf)
    data_df = vehicle_data_df  # Read the target locations CSV file into a pandas DataFrame
    close_points = {}  # Use a dictionary to store data points close to each target point

    for data_index, data_row in data_df.iterrows():
        data_lat = data_row['Latitude']
        data_lon = data_row['Longitude']
        data_speed = data_row['Speed']

        for target_index, target_row in target_df.iterrows():
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

def compare_value(vehicle_json_data, average_speed_json, aggregation_point_folder):
    vehicle_json_data_csv = json.loads(vehicle_json_data)
    vehicle_data_df = pd.DataFrame(vehicle_json_data_csv)

    midpoint = len(vehicle_data_df) // 2

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_top = executor.submit(check_proximity, vehicle_data_df.iloc[:midpoint])
        future_bottom = executor.submit(check_proximity, vehicle_data_df.iloc[midpoint:])

        concurrent.futures.wait([future_top, future_bottom])  # Pass futures in a list

        # Retrieve results from futures
        close_points_top = future_top.result()
        close_points_bottom = future_bottom.result()

    # Combine results from both threads
    close_points = check_proximity(vehicle_data_df)
    json_data = json.dumps(close_points, indent=4)
    if os.path.exists(average_speed_json) and os.path.getsize(average_speed_json) > 0:
        update_json(average_speed_json, close_points, aggregation_point_folder)
    else:
        with open(average_speed_json, "w+") as file:
            file.write(json_data)


    # top = threading.Thread(target=check_proximity, args=(vehicle_data_df.iloc[:midpoint],))
    # bottom = threading.Thread(target=check_proximity, args=(vehicle_data_df.iloc[midpoint:],))

    # top.start()
    # bottom.start()

    # top.join()
    # bottom.join()

    # # Assuming check_proximity returns dictionaries close_points_top and close_points_bottom
    # close_points_top = {}  # Update with actual method to get results from threads
    # close_points_bottom = {}  # Update with actual method to get results from threads
    # close_points = {**close_points_top, **close_points_bottom}