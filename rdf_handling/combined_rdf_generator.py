from imports.imports import json, os, Graph, Literal, Namespace, URIRef, RDF, XSD, BNode

def create_subject_and_triple(movement_classification, RideQuality):
    if movement_classification in ("High Acceleration", "Normal Acceleration"):
        ride_quality = movement_classification
        sub = RideQuality.PatchGoodToGo
    elif movement_classification == "Normal Braking":
        ride_quality = movement_classification
        sub = RideQuality.PatchDriveCaution
    elif movement_classification == "High Braking":
        ride_quality = movement_classification
        sub = RideQuality.PatchWithBump
    else:
        ride_quality = movement_classification
        sub = RideQuality.PatchGoodToGo

    return sub, ride_quality

def rdf_generation(location_data, rdf_file_path):
    graph = Graph()
    # Define namespaces
    RideQuality = Namespace("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")
    graph.bind("RideQuality", RideQuality)
    PatchDriveCaution = RideQuality["PatchDriveCaution"]
    PatchGoodToGo = RideQuality["PatchGoodToGo"]
    PatchWithBump = RideQuality["PatchWithBump"]
    geoLocation = RideQuality["geoLocation"]
    graph.bind("PatchDriveCaution", PatchDriveCaution)
    graph.bind("PatchGoodToGo", PatchGoodToGo)
    graph.bind("PatchWithBump", PatchWithBump)
    graph.bind("geoLocation", geoLocation)

    for geo_location, startLatitude, startLongitude, endLatitude, endLongitude, classification, speeds in location_data:
        sub, ride_quality = create_subject_and_triple(classification, RideQuality)
        subject = RideQuality[geo_location]
        graph.add((subject, RDF.type, sub))
        graph.add((subject, RideQuality['hasStartLatitude'], Literal(startLatitude, datatype=XSD.decimal)))
        graph.add((subject, RideQuality['hasStartLongitude'], Literal(startLongitude, datatype=XSD.decimal)))
        graph.add((subject, RideQuality['hasEndLatitude'], Literal(endLatitude, datatype=XSD.decimal)))
        graph.add((subject, RideQuality['hasEndLongitude'], Literal(endLongitude, datatype=XSD.decimal)))
        graph.add((subject, RideQuality.hasPatchQuality, Literal(ride_quality)))

        # Calculate average speed
        avg_speed = sum(speeds) / len(speeds)

        # Format speed as decimal number with 2 decimal places
        formatted_speed = "{:.2f}".format(avg_speed)
        graph.add((subject, RideQuality.hasAvarageSpeed, Literal(formatted_speed, datatype=XSD.decimal)))

    graph.serialize(rdf_file_path, format='turtle')

def combined_triples_generator(json_file_path, rdf_file_path):
    location_data = []
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)
        for data_dict in json_data:
            for geo_location, data_list in data_dict.items():
                for data_item in data_list:
                    speeds = [data_item["speed"] for data_item in data_list]
                    latitude = [data_item["latitude"] for data_item in data_list]
                    longitude = [data_item["longitude"] for data_item in data_list]
                    location_data.append((geo_location, latitude[0], longitude[0], latitude[-1], longitude[-1], data_item["MovementClassification"], speeds))

    rdf_generation(location_data, rdf_file_path)

if __name__ == "__main__":
    json_folder = "/home/pragnakalp-l-12/Desktop/viren_sir/test/output/json/RoadSection_Start:('22.875595','74.230773')_End:('22.855836','74.243385')_2024-04-13-15:48:36.json"
    rdf_folder = "/home/pragnakalp-l-12/Desktop/viren_sir/test/output/combined_triples_rdf/RoadSection_Start:('22.875595','74.230773')_End:('22.855836','74.243385')_2024-04-13-15:48:36.ttl"
    os.makedirs(os.path.dirname(rdf_folder), exist_ok=True)
    combined_triples_generator(json_folder, rdf_folder)
