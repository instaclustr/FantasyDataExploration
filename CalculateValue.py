# Jordan Braiuka
# Instaclustr - 2017

# Import Required Packages
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import argparse

# Parse the Arguments
parser = argparse.ArgumentParser(description='Import player data into a cassandra cluster')
parser.add_argument('-url', 
                   help='The URL address of the Elasticsearch cluster e.g. fb8da90356c94a0bb4ebf52579e814bb.cu.dev.instaclustr.com')
parser.add_argument('-u', 
                   help='Username for Elasticsearch')
parser.add_argument('-p', 
                   help='Username for Elasticsearch')
parser.add_argument('-n', default='player_value', 
                   help='Name of the Value')
parser.add_argument('-s', default="dif_3PM2018,dif_AST2018,dif_PTS2018,dif_REB2018,dif_STL2018,dif_TO2018,dif_DD2017,dif_BLK2018,dif_FGP_W,dif_3PP_W,dif_FTP_W",
                   help='Comma separated list of Statistics to sum e.g. dif_3PM2018,dif_DD2017')
args = parser.parse_args()

es = Elasticsearch(['https://' + args.u + ':' + args.p + '@' + args.url+':9201'])

# Search with a max size of 150
s = Search(using=es)
s._params['size'] = 150

# Get the player averages
player_averages = s.filter('term', _type="player_averages").execute().to_dict().get("hits").get("hits")

print(player_averages)
# for every player
for player in player_averages:
	p = player.get("_source")
	# Their value starts at 0
	value = 0.0
	body2 = {
          	"Player Name" 		: p.get("Player Name"),
          	}
  	# For every category we care about
	for category in args.s.split(','):
		# Add it to their value
		value+= float(p.get(category))

	body2.update({args.n : value, 
  		})
	# Write this value back into the players value doctype
	es.index(index='fantasy', doc_type='player_values', body=body2)

