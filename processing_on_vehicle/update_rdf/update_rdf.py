from rdflib import Graph, Literal, Namespace, URIRef
from decimal import Decimal, ROUND_HALF_UP

def update_rdf_triples(rdf_file_path, geo_location_uri, new_latitude, new_longitude, new_ride_quality):
    try:
        # Load the RDF file into an RDF graph
        g = Graph()
        g.parse(rdf_file_path, format="turtle")

        # Define namespaces
        RideQuality = Namespace("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")
        location_tuple = (Literal(new_latitude.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)),Literal(new_longitude.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)))
        # Find the triples to update based on the geoLocation URI
        triples_to_update = [
            (URIRef(geo_location_uri), RideQuality["hasLocation"], ),
            (URIRef(geo_location_uri), RideQuality["hasPatchQuality"], Literal(new_ride_quality))
        ]

        # Modify the objects of the triples to the new values
        for triple_to_update in triples_to_update:
            g.set(triple_to_update)

        # Serialize the updated RDF graph back to the RDF file
        g.serialize(destination=rdf_file_path, format="turtle")

        print("Triples updated successfully.")

    except Exception as e:
        print(f"Error updating RDF triples: {e}")


if __name__ == "__main__":
    rdf_file_path = "/home/pragnakalp-l-12/Desktop/viren_sir/gitHub/csv_to_rdf_converter/test_rdf/test.ttl"
    location_uri = "http://www.semanticweb.org/viren/ontologies/2024/0/RideQuality#geoLocation1"
    new_latitude = Decimal("42.122999")
    new_longitude = Decimal("-71.456000")
    # Example usage:
    update_rdf_triples(rdf_file_path, location_uri, new_latitude, new_longitude, "High Acceleration")
