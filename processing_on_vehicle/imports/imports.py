import os
import csv
import time
import json
import shutil
import requests
import pandas as pd
import configparser
import concurrent.futures
from imports.config import logger
from rdflib.namespace import RDF, XSD
from multiprocessing import cpu_count
from watchdog.observers import Observer
from rdflib.plugins.sparql import prepareQuery
from owlready2 import get_ontology, sync_reasoner
from watchdog.events import FileSystemEventHandler
from rdflib import Graph, Literal, Namespace, URIRef, BNode