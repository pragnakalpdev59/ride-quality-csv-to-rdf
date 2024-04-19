# from imports.imports import *

import csv
import os
import time
import pandas as pd
from multiprocessing import cpu_count
import concurrent.futures
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shutil
from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, XSD
import requests
from rdflib.plugins.sparql import prepareQuery
from owlready2 import get_ontology, sync_reasoner
import json
from memory_profiler import profile

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

# @profile
def rdf_generation(csv_file_path, rdf_folder):
    try:
        # Create an RDF graph
        df = pd.read_csv(csv_file_path)
        graph = Graph()
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

        # Open and read the CSV file
        with open(csv_file_path, newline='') as csvfile:
            csvreader = csv.DictReader(csvfile)
            idx = 0
            for row in csvreader:
                sub, ride_quality, subject, speed, latitude, longitude = create_subject_and_triple(idx, row, RideQuality)
                if sub is not None:
                    graph.add((subject, RDF.type, sub))
                    graph.add((subject, RideQuality["hasLatitude"], latitude))
                    graph.add((subject, RideQuality["hasLongitude"], longitude))
                    graph.add((subject, RideQuality["hasSpeed"], Literal(speed)))
                    graph.add((subject, RideQuality["hasRideQuality"], Literal(ride_quality)))

                    idx += 1

        rdf_file_path = os.path.join(rdf_folder, "temp.ttl")
        graph.serialize(rdf_file_path)

    except Exception as e:
        print(f"Error in csv_to_rdf: {e}")  # Log error

if __name__ == "__main__":
    csv_file_path = "/home/pragnakalp-l-12/Desktop/viren_sir/test/output/csv_local_copy/RoadSection_Start:('22.81947667','74.26211833')_End:('22.68504667','74.317725')_2024-04-15-17:30:02.csv"  # Replace with your CSV file path
    rdf_folder = "/home/pragnakalp-l-12/Desktop/viren_sir/test/output/rdf_folder"  # Replace with your RDF output folder path
    rdf_generation(csv_file_path, rdf_folder)
