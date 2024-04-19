from imports.imports import json, os, Graph, Literal, Namespace, URIRef, RDF, XSD, BNode, logger

# Function to create subject and triple based on movement classification
def create_subject_and_triple(movement_classification, RideQuality):
    try:
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
    except Exception as e:
        logger.error(f"Error in create_subject_and_triple: {e}")

# Function to generate RDF triples from location data
def rdf_generation(location_data, rdf_file_path):
    try:
        graph = Graph()  # Create RDF graph
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

        # Iterate through location data to create triples
        for geo_location, startLatitude, startLongitude, endLatitude, endLongitude, classification, speeds in location_data:
            sub, ride_quality = create_subject_and_triple(classification, RideQuality)  # Create subject and triple
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

        graph.serialize(rdf_file_path, format='turtle')  # Serialize RDF graph to file
    except Exception as e:
        logger.error(f"Error in rdf_generation: {e}")

# Function to generate combined triples from JSON data
def combined_triples_generator(json_file_path, rdf_file_path):
    try:
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

        rdf_generation(location_data, rdf_file_path)  # Generate RDF triples
    except Exception as e:
        logger.error(f"Error in combined_triples_generator")