import requests, re, io, json, os
import pandas as pd

from bs4 import BeautifulSoup
from google.cloud import storage
from math import log
from google.cloud import secretmanager
from google.cloud import pubsub_v1
from google.cloud import firestore
from datetime import datetime as dt


###>>>>> GCP CLIENT and GCP param <<<<<###
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
DB = firestore.Client()