from imports.imports import logger, Graph, URIRef, prepareQuery, get_ontology, sync_reasoner

def get_average_speed(graph):
    """
    Calculates the average speed limit for road patches with bumps based on the provided RDF graph.

    Args:
        graph (Graph): An RDFlib Graph object containing the road quality data.

    Returns:
        float: The average speed limit for road patches with bumps (rounded to two decimal places),
              or None if an error occurs.
    """

    try:
        # Define the namespace for the RideQuality ontology used in the graph
        RideQuality = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")

        # Construct a SPARQL query to retrieve speed limits for road patches with bumps
        #  - Selects geoLocation (possibly an identifier for the location), latitude, longitude and speed attributes
        #  - Uses the RideQuality namespace for properties related to ride quality
        query_str = """
            PREFIX RideQuality: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#>
            SELECT ?geoLocation ?latitude ?longitude ?speed
            WHERE {
                ?geoLocation RideQuality:hasLatitude ?latitude ;
                            RideQuality:hasLongitude ?longitude ;
                            RideQuality:hasSpeed ?speed .
            }
        """
        query = prepareQuery(query_str, initNs={"RideQuality": RideQuality})

        # Execute the SPARQL query on the RDF graph
        speed_list = []
        result = graph.query(query)
        for row in result:
            # Extract the speed value from each query result and add it to the list
            speed_list.append(float(row['speed']))

        # Calculate and return the average speed (rounded to 2 decimal places) if there are results
        if speed_list:
            avg_speed = round(sum(speed_list) / len(speed_list), 2)
            return avg_speed

        # Handle the case where no speed information is found in the graph
        else:
            logger.info("No speed information found for road patches with bumps.")
            return None

    except Exception as e:
        # Log any errors encountered while fetching speed data
        logger.error(f"Error in finding patches with bumps: {e}")
