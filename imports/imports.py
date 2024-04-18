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