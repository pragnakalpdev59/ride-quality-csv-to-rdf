from rdflib import Graph, Literal
import os
import json

found_data = []

def find_coordinates_in_rdf_files(folder_path, latitude, longitude, movement_classifications, json_path):
    # List all files in the folder
    files = os.listdir(folder_path)
    found_files = []
    global found_data

    for file_name in files:
        # if file_name.endswith('.ttl'):
            file_path = os.path.join(folder_path, file_name)
            g = Graph()
            try:
                g.parse(file_path, format='turtle')
            except Exception as e:
                print(f"Error parsing {file_name}: {e}")
                continue

            # SPARQL query to find the latitude and longitude values in the RDF graph
            query = f"""
                PREFIX RideQuality: <http://www.semanticweb.org/viren/ontologies/2024/0/RideQuality#>
                SELECT ?geoLocation ?latitude ?longitude
                WHERE {{
                    ?geoLocation RideQuality:hasLatitude ?latitude ;
                                 RideQuality:hasLongitude ?longitude .
                    FILTER (str(?latitude) = "{latitude}" && str(?longitude) = "{longitude}")
                }}
            """
            result = g.query(query)
            for row in result:
                data = {
                    "geoLocation": row["geoLocation"],
                    "latitude": float(row["latitude"].value),
                    "longitude": float(row["longitude"].value),
                    "movement_classifications": movement_classifications
                }
                if data not in found_data:
                    found_data.append(data)

            if len(result) > 0:
                found_files.append(file_name)

    if found_files:
        print(f"Found the coordinates ({latitude}, {longitude}) in the following file(s):")
        # print(f"this is the found_data: ", found_data)
        for file_name in found_files:
            print(file_name)
    else:
        print(f"Coordinates ({latitude}, {longitude}) not found in any files.")

    convert_to_json(found_data, json_path)
    return found_files, found_data


def convert_to_json(found_data, file_path):
    json_data = json.dumps(found_data, indent=4)

    with open(file_path, 'w') as file:
        file.write(json_data)
