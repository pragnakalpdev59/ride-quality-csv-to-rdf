from imports.imports import *

def create_subject_and_triple(idx, row, RideQuality):
    try:
        latitude = Literal(row['lat'], datatype=XSD.decimal)
        longitude = Literal(row['lon'], datatype=XSD.decimal)
        speed = Literal(row['speed'], datatype=XSD.decimal)
        movement_classification = row["MovementClassification"]

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

        padded_idx = str(idx + 1).zfill(5)
        subject = RideQuality[f"geoLocation{padded_idx}"]

        return sub, ride_quality, subject, speed, latitude, longitude
    except Exception as e:
        print(f"Error in create_subject_and_triple: {e}")
        return None, None, None, None, None

def csv_to_rdf(cached_df, rdf_folder, ttl_file_path):
    try:
        df = cached_df

        # Create an RDF graph
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

        if os.path.exists(ttl_file_path):
            graph.parse(ttl_file_path, format="ttl")

        # Get the last 5 rows from the DataFrame
        last_rows = df.tail(10)

        idx = len(df) - len(last_rows)  # Start index for the last 5 rows

        for _, row in last_rows.iterrows():
            sub, ride_quality, subject, speed, latitude, longitude = create_subject_and_triple(idx, row, RideQuality)
            existing_subjects = list(graph.subjects())
            if subject in existing_subjects:
                graph.set((subject, RDF.type, sub))
                graph.set((subject, RideQuality["hasLatitude"], latitude))
                graph.set((subject, RideQuality["hasLongitude"], longitude))
                graph.set((subject, RideQuality["hasSpeed"], Literal(speed)))
                graph.set((subject, RideQuality["hasRideQuality"], Literal(ride_quality)))
            else:
                # Add new subject and triples if it doesn't exist
                graph.add((subject, RDF.type, sub))
                graph.add((subject, RideQuality["hasLatitude"], latitude))
                graph.add((subject, RideQuality["hasLongitude"], longitude))
                graph.add((subject, RideQuality["hasSpeed"], Literal(speed)))
                graph.add((subject, RideQuality["hasRideQuality"], Literal(ride_quality)))

            idx += 1

        # Serialize the updated graph to the TTL file
        graph.serialize(ttl_file_path, format="ttl")

    except Exception as e:
        print(f"Error in rdf_generation: {e}")  # Log error



def rdf_generation(cached_df, rdf_folder, ttl_file_path):
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(csv_to_rdf, cached_df, rdf_folder, ttl_file_path)]

        concurrent.futures.wait(futures)
    except Exception as e:
        print(f"Error in rdf_generation: {e}")  # Log error
