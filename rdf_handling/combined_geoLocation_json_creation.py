from imports.imports import pd, json, os  # Import necessary modules

def json_generation(csv_file_path, json_file_path):
    """Generate JSON file from CSV data."""
    df = pd.read_csv(csv_file_path)  # Read CSV file into DataFrame
    similar_value = []  # Initialize list for similar values
    unique_value = []  # Initialize list for unique values

    # Extract columns from DataFrame
    MovementClassification = df['MovementClassification'].tolist()
    latitude = df['lat'].tolist()
    longitude = df['lon'].tolist()
    speed = df['speed'].tolist()
    data = {}  # Initialize dictionary for data
    idx = 0  # Initialize index for unique values

    for i in range(len(MovementClassification)):
        # Create data dictionary for each row
        data = {
            "latitude": latitude[i],
            "longitude": longitude[i],
            "MovementClassification": MovementClassification[i],
            "speed": speed[i]
        }

        # Check if movement classification is similar to previous row
        if i == 0 or MovementClassification[i] == MovementClassification[i-1]:
            similar_value.append(data)  # Add data to similar value list
        else:
            # Add unique value to unique value list
            unique_value.append({f"geoLocation{str(idx+1).zfill(5)}": similar_value.copy()})
            similar_value.clear()  # Clear similar value list
            similar_value.append(data)  # Add data to cleared list
            idx += 1  # Increment index for unique values

    # Check if there are remaining similar values
    if similar_value:
        unique_value.append({f"geoLocation{str(idx+1).zfill(5)}": similar_value.copy()})

    # Convert unique value list to JSON format
    json_data = json.dumps(unique_value, indent=4)
    # Write JSON data to file
    with open(json_file_path, 'w') as file:
        file.write(json_data)
