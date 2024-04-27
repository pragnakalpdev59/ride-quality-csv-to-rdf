from imports.imports import *

def bump_number(graph):
    try:
        # Define namespaces (RDF/OWL URI references) for better readability
        RideQuality = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")
        PatchWithBump = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchWithBump")

        # Prepare SPARQL query to count the number of bumps after reasoning is applied
        query_str = """
            PREFIX RideQuality: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#>
            PREFIX PatchWithBump: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchWithBump>
            SELECT (COUNT(?patch) AS ?count)
            WHERE {
                ?patch a PatchWithBump: ;  # Find all instances of PatchWithBump class

            }
        """
        query = prepareQuery(query_str, initNs={"RideQuality": RideQuality, "PatchWithBump": PatchWithBump})

        # Execute the query on the inferred graph and get the results
        result = graph.query(query)

        # Extract the bump count from the query result
        for row in result:
            bump_count = (row["count"])

        return bump_count

    except Exception as e:
        logger.error(f"Error in bump_number: {e}")
        return None
