from imports.imports import *  # Import all necessary libraries

def patch_counts(graph):
    """
    This function analyzes the road quality based on the types of patches present in the RDF graph.

    Args:
        graph: The RDF graph object containing the road data.

    Returns:
        A tuple containing three strings: road condition, ride quality, and warning message.

    Raises:
        Exception: If there is an error during the process.
    """
    try:
        # Define namespaces for better readability
        RideQuality = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#")
        PatchWithBump = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchWithBump")
        PatchGoodToGo = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchGoodToGo")
        PatchDriveCaution = URIRef("http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchDriveCaution")

        # Prepare SPARQL query to count different patch types after reasoning
        query_str = """
            PREFIX RideQuality: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#>
            PREFIX PatchWithBump: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchWithBump>
            PREFIX PatchGoodToGo: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchGoodToGo>
            PREFIX PatchDriveCaution: <http://www.gecdahod.ac.in/ontologies/2024/0/RideQuality#PatchDriveCaution>
            SELECT ?patchType (COUNT(?patch) AS ?count)
            WHERE {
                ?patch a ?patchType .
                VALUES ?patchType { PatchWithBump: PatchGoodToGo: PatchDriveCaution: }
            }
            GROUP BY ?patchType
        """
        query = prepareQuery(query_str, initNs={"RideQuality": RideQuality,
                                                "PatchWithBump": PatchWithBump,
                                                "PatchGoodToGo": PatchGoodToGo,
                                                "PatchDriveCaution": PatchDriveCaution})

        # Execute the query on the RDF graph after reasoning
        patch_type_list = []
        patch_type_list_percentage = []
        total_patches = 0
        result = graph.query(query)
        for row in result:
            # Get the patch type without the namespace prefix
            patch_type = str(row["patchType"]).split("#")[-1]
            count = int(row["count"])
            total_patches += count
            patch_type_list.append({patch_type: count})

        # Calculate percentages for each patch type
        for patch_dict in patch_type_list:
            for key, value in patch_dict.items():
                percentage = round((value / total_patches) * 100, 3)
                patch_type_list_percentage.append(f"{key}: {percentage:.2f}%")

        # Delegate road quality analysis based on patch type percentages
        return analyze_road_quality(patch_type_list_percentage)

    except Exception as e:
        logger.error(f"Error in patch_count: {e}")
        return None, None, None  # Return None for all outputs on error


def analyze_road_quality(road_patches):
    """
    Analyzes the overall quality of a road based on provided road patch data.

    Args:
        road_patches (list): A list of strings representing data for each road patch.
            Each string should be formatted like 'PatchGoodToGo:70%' or 'PatchWithBump:30%'

    Returns:
        tuple (str, str, str): A tuple containing three elements:
            - road_condition (str): Overall road condition description. (e.g., "Good to go")
            - ride_quality (str): Description of the ride quality based on road condition. (e.g., "Smooth")
            - warning (str): A safety warning based on the analyzed road quality.
    """

    try:
        # Initialize variables to store percentages for different road conditions
        good_to_go_percentage = 0
        drive_caution_percentage = 0
        with_bump_percentage = 0

        # Loop through each road patch data
        for patch in road_patches:
            if 'PatchGoodToGo' in patch:
                # Extract percentage value from 'PatchGoodToGo' data
                good_to_go_percentage = float(patch.split(':')[1].strip('% '))
            elif 'PatchDriveCaution' in patch:
                # Extract percentage value from 'PatchDriveCaution' data
                drive_caution_percentage = float(patch.split(':')[1].strip('% '))
            elif 'PatchWithBump' in patch:
                # Extract percentage value from 'PatchWithBump' data
                with_bump_percentage = float(patch.split(':')[1].strip('% '))

        # Analyze overall road condition based on percentages
        if good_to_go_percentage > 50:
            road_condition = "Overall, the road quality seems good to go."
            ride_quality = "overall smooth"
            warning = "Drive safely and observe all traffic regulations."
            return road_condition, ride_quality, warning

        elif drive_caution_percentage > 50:
            road_condition = "Overall, the road quality seems cautious to drive."
            ride_quality = "overall not good"
            warning = "Exercise caution while driving. Be aware of potential hazards on the road and maintain a safe speed."
            return road_condition, ride_quality, warning

        else:
            road_condition = "Overall, the road quality seems somewhat mixed, leaning towards being drivable but with areas that require caution."
            ride_quality = 'overall mediocre'
            warning =  "Proceed with caution. Variable road conditions may require adjustments to driving behavior. Watch out for sudden changes in road surface or visibility."
            return road_condition, ride_quality, warning

    except Exception as e:
        # Log any errors encountered during analysis
        logger.error(f"Error in analyze_road_quality: {e}")
        return None, None, None
