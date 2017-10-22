# Jordan Braiuka
# Instaclustr - 2017

# Import the packages needed
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
import csv
from cassandra.policies import DCAwareRoundRobinPolicy
import argparse

# Parse the Arguments needed
parser = argparse.ArgumentParser(description='Import player data into a cassandra cluster')
parser.add_argument('-ip', 
                   help='A comma separated list of Node IP Addresses')
parser.add_argument('-url', 
                   help='The URL address of the Elasticsearch cluster e.g. fb8da90356c94a0bb4ebf52579e814bb.cu.dev.instaclustr.com')
parser.add_argument('-u', 
                   help='Username for Elasticsearch')
parser.add_argument('-p', 
                   help='Username for Elasticsearch')
parser.add_argument('-f', 
                   help='Filename of the player data csv')

args = parser.parse_args()

# Define the cassandra cluster settings
cluster = Cluster(
    # Define the nodes we are connecting to
    contact_points=args.ip.split(','),
    # Specifies the Data Centre
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='AWS_VPC_US_EAST_1'), # your local data centre
    # Specifies the port
    port=9042
)

# Connect to the cassandra
session = cluster.connect()
print(args.u, args.p, args.url)
# Specify the Elasticsearch URL, embedding the username and pasword
es = Elasticsearch(['https://' + args.u + ':' + args.p + '@' + args.url+':9201'])

# Sets outmappings for categories we will be creating
body='''{ 
  "mappings" : {
      "player" : {
        "properties" : {
          "3PM2017" : {
            "type" : "float"
          },
          "3PM2018" : {
            "type" : "float"
          },
          "AST2017" : {
            "type" : "float"
          },
          "AST2018" : {
            "type" : "float"
          },
          "ATO2017" : {
            "type" : "float"
          },
          "ATO2018" : {
            "type" : "float"
          },
          "BLK2017" : {
            "type" : "float"
          },
          "BLK2018" : {
            "type" : "float"
          },
          "ESPN Rank" : {
            "type" : "float"
          },
          "FGP2017" : {
            "type" : "float"
          },
          "FGA2017" : {
            "type" : "float"
          },
          "FGP2018" : {
            "type" : "float"
          },
          "FTP2017" : {
            "type" : "float"
          },
          "FTP2018" : {
            "type" : "float"
          },
          "GP2017" : {
            "type" : "float"
          },
          "GP2018" : {
            "type" : "float"
          },
          "Min2017" : {
            "type" : "float"
          },
          "Min2018" : {
            "type" : "float"
          },
          "PTS2017" : {
            "type" : "float"
          },
          "PTS2018" : {
            "type" : "float"
          },
          "Player Name" : {
            "type" : "string",
            "index":    "not_analyzed"
          },
          "REB2017" : {
            "type" : "float"
          },
          "REB2018" : {
            "type" : "float"
          },
          "STL2017" : {
            "type" : "float"
          },
          "STL2018" : {
            "type" : "float"
          },
          "TO2017" : {
            "type" : "float"
          },
          "TO2018" : {
            "type" : "float"
          },
          "3PP2017" : {
            "type" : "float"
          },
          "3PA2017" : {
            "type" : "float"
          },
          "DD2017" : {
            "type" : "float"
          },
          "FTA2017" : {
            "type" : "float"
          },
          "FTM2017" : {
            "type" : "float"
          },
          "Team + Position" : {
            "type" : "string"
          },
          "FGP_W" : {
            "type" : "float"
          },
          "FTP_W" : {
            "type" : "float"
          },
          "TPP_W" : {
            "type" : "float"
          }
        }
      }
    }
}'''

# Try and delete the index if it already exists
# try:
#   es.indices.delete(index='fantasy')
# except Exception as e:
#   pass

# Create the fantasy index through Elasticsearch
es.indices.create(index ='fantasy', body=body)
# Connect to the cassandra cluster, the keyspace fantasy
session = cluster.connect("fantasy")

# Create the query
prepared = session.prepare("""
        INSERT INTO player ("3PM2017","3PM2018","AST2017","AST2018","ATO2017","ATO2018","BLK2017","BLK2018","ESPN Rank","FGP2017","FGA2017","FGP2018","FTP2017","FTP2018","GP2017","GP2018",
              "Min2017","Min2018","PTS2017","PTS2018","Player Name",
              "REB2017","REB2018","STL2017","STL2018","TO2017","TO2018","3PP2017","3PA2017","DD2017","FTA2017","FTM2017","Team + Position", 
              "FGP_W", "FTP_W", "TPP_W", "_id")
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
x="0"

# Open the csv
with open(args.f) as csvfile:
  reader = csv.DictReader(csvfile)
  for player in reader:

    # Load in each row of player data
    session.execute(prepared.bind((
      [float(player["3PM2017"])],
      [float(player["3PM2018"])],
      [float(player["AST2017"])],
      [float(player["AST2018"])],
      [float(player["ATO2017"])],
      [float(player["ATO2018"])],
      [float(player["BLK2017"])],
      [float(player["BLK2018"])],
      [float(player["ESPN Rank"])],
      [float(player["FGP2017"])],
      [float(player["FGA2017"] if player["FGA2017"] is not "" else 0)],
      [float(player["FGP2018"])],
      [float(player["FTP2017"])],
      [float(player["FTP2018"])],
      [float(player["GP2017"])],
      [float(player["GP2018"])],
      [float(player["Min2017"])],
      [float(player["Min2018"])],
      [float(player["PTS2017"])],
      [float(player["PTS2018"])],
      [player["Player Name"]],
      [float(player["REB2017"])],
      [float(player["REB2018"])],
      [float(player["STL2017"])],
      [float(player["STL2018"])],
      [float(player["TO2017"])],
      [float(player["TO2018"])],
      [float(player["3PP2017"] if player["3PP2017"] is not "" else 0)],
      [float(player["3PA2017"] if player["3PA2017"] is not "" else 0)],
      [float(player["DD2017"] if player["DD2017"] is not "" else 0)],
      [float(player["FTA2017"])],
      [float(player["FTM2017"])],
      [player["Team + Position"]],
      [float(player["FGP2018"]) * float(player["FGA2017"] if player["FGA2017"] is not "" else 0)],
      [float(player["FTP2018"]) * float(player["FTA2017"] if player["FTA2017"] is not "" else 0)],
      [float(player["3PA2017"] if player["3PA2017"] is not "" else 0) * float(player["3PP2017"] if player["3PP2017"] is not "" else 0)],
      x
      )))
    x=str(int(x)+1)
    

