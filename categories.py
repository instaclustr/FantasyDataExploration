# Jordan Braiuka
# Instaclustr - 2017

# Import the packages needed
from cassandra.cluster import Cluster
from elasticsearch_dsl import Search
import json
from cassandra.policies import DCAwareRoundRobinPolicy
import argparse

# Parse the Arguments needed
parser = argparse.ArgumentParser(description='Import player data into a cassandra cluster')
parser.add_argument('-url', 
                   help='The URL address of the Elasticsearch cluster e.g. fb8da90356c94a0bb4ebf52579e814bb.cu.dev.instaclustr.com')
parser.add_argument('-u', 
                   help='Username for Elasticsearch')
parser.add_argument('-p', 
                   help='Username for Elasticsearch')
args = parser.parse_args()

# Connect to the Elasticsearch Cluster
client = Elasticsearch()
es = Elasticsearch(['https://' + args.u + ':' + args.p + '@' + args.url+':9201'])

# Create the mapping for the Category category
body='''{
  "mappings" : {
      "player" : {
        "properties" : {
          "category" : {
            "type" : "string",
            "index": "not_analyzed"
          },"value" : {
            "type" : "float",
          }
      	}
  	  }
	}
}'''

# A list of the categories
categories = [	"3PM2018",
				"AST2018",
				"PTS2018",
				"REB2018",
				"STL2018",
				"TO2018",
				"DD2017",
				"BLK2018",
				"FGP_W",
				"TPP_W",
				"FTP_W"]

# Using the client perform a search
s = Search(using=client)
# Set the maximum number of returns
s._params['size'] = 150

# Complete the search
players = s.filter('term', _type="player").execute().to_dict().get("hits").get("hits")

for p in players:
	player = p.get("_source")
	for category in categories:
		# Write the category data back into Elassandra
		body2 = {"category" : category,
				"value" : player[category]
				}
		es.index(index='fantasy', doc_type='category', body=body2)

