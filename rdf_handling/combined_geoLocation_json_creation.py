from imports.imports import pd, json, os

def json_generation(csv_file_path, json_file_path):
    df = pd.read_csv(csv_file_path)
    similar_value = []
    unique_value = []

    MovementClassification = df['MovementClassification'].tolist()
    latitude = df['lat'].tolist()
    longitude = df['lon'].tolist()
    speed = df['speed'].tolist()
    data = {}
    idx = 0

    for i in range(len(MovementClassification)):
            data = {
                "latitude": latitude[i],
                "longitude": longitude[i],
                "MovementClassification": MovementClassification[i],
                "speed": speed[i]
            }
            if i == 0 or MovementClassification[i] == MovementClassification[i-1]:
                similar_value.append(data)
            else:
                unique_value.append({f"geoLocation{str(idx+1).zfill(5)}" : similar_value.copy()})
                similar_value.clear()
                similar_value.append(data)
                idx += 1

    if similar_value:
            unique_value.append({f"geoLocation{str(idx+1).zfill(5)}" : similar_value.copy()})

    json_data = json.dumps(unique_value, indent=4)
    with open(json_file_path, 'w') as file:
            file.write(json_data)


if __name__ == "__main__":
    csv_folder = "/home/pragnakalp-l-12/Desktop/viren_sir/test/output/csv_local_copy/RoadSection_Start:('22.875595', '74.230773')_End:('22.855836', '74.243385')_20240411181353.csv"
    json_folder = "/home/pragnakalp-l-12/Desktop/viren_sir/gitHub/csv_to_rdf_converter/output/json/D-101 ss to gec.json"
    json_generation(csv_folder, json_folder)