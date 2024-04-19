from imports.imports import *

# Function to create subject and triples for RDF generation
def create_subject_and_triple(idx, row, RideQuality):
    try:
        latitude = Literal(row['Latitude'], datatype=XSD.decimal)  # Convert latitude to RDF Literal with decimal datatype
        longitude = Literal(row['Longitude'], datatype=XSD.decimal)  # Convert longitude to RDF Literal with decimal datatype
        speed = Literal(row['speed'], datatype=XSD.decimal)  # Convert speed to RDF Literal with decimal datatype
        movement_classification = row["MovementClassification"]  # Get movement classification from DataFrame

        # Determine RDF subject based on movement classification
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

        padded_idx = str(idx + 1).zfill(5)  # Pad index with zeros for subject naming convention
        subject = RideQuality[f"geoLocation{padded_idx}"]  # Create RDF subject based on index

        return sub, ride_quality, subject, speed, latitude, longitude  # Return RDF subject and triples
    except Exception as e:
        logger.error(f"Error in create_subject_and_triple: {e}")  # Log error
        return None, None, None, None, None

# Function to convert CSV data to RDF format
def csv_to_rdf(cached_df, rdf_folder):
    try:
        timestamp = time.strftime('%Y-%m-%d-%H:%M:%S')  # Get current timestamp
        ttl_file_name = f"AggeregationPoint_{timestamp}.ttl"  # Create RDF file name with timestamp
        ttl_file_path = os.path.join(rdf_folder, ttl_file_name)  # Create full path for RDF file
        df = cached_df  # Use cached DataFrame

        # Create an RDF graph
        graph = Graph()
        # Define namespaces
        RideQuality = Namespace("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality")
        graph.bind("RideQuality", RideQuality)
        PatchDriveCaution = RideQuality["PatchDriveCaution"]
        PatchGoodToGo = RideQuality["PatchGoodToGo"]
        PatchWithBump = RideQuality["PatchWithBump"]
        geoLocation = RideQuality["geoLocation"]
        graph.bind("PatchDriveCaution", PatchDriveCaution)
        graph.bind("PatchGoodToGo", PatchGoodToGo)
        graph.bind("PatchWithBump", PatchWithBump)
        graph.bind("geoLocation", geoLocation)

        triples_batch = []  # Initialize list for RDF triples batch
        idx = 0  # Initialize index for subject naming
        for _, row in df.iterrows():
            sub, ride_quality, subject, speed, latitude, longitude = create_subject_and_triple(idx, row, RideQuality)
            triples_batch.extend([(subject, RDF.type, sub),
                                  (subject, RideQuality["hasLatitude"], latitude),
                                  (subject, RideQuality["hasLongitude"], longitude),
                                  (subject, RideQuality["hasSpeed"], Literal(speed)),
                                  (subject, RideQuality["hasRideQuality"], Literal(ride_quality))])
            idx += 1  # Increment index for next subject

        # Add the batch of triples to the graph
        graph += triples_batch

        # Serialize the RDF graph to a TTL file
        graph.serialize(ttl_file_path)

    except Exception as e:
        logger.error(f"Error in csv_to_rdf: {e}")  # Log error

# Function for RDF generation using multiprocessing
def rdf_generation(cached_df, rdf_folder):
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [executor.submit(csv_to_rdf, cached_df, rdf_folder)]  # Submit CSV to RDF conversion task

        concurrent.futures.wait(futures)  # Wait for tasks to complete
    except Exception as e:
        logger.error(f"Error in rdf_generation: {e}")  # Log error