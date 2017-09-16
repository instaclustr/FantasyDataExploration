from elasticsearch import Elasticsearch
import json
import csv

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
i=0
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
          }
        }
      }
  	}
}'''
# 
try:
  es.indices.delete(index='fantasy')
except Exception as e:
  pass
es.indices.create(index ='fantasy', body=body)

with open('OUTPUT_STATS.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for player in reader:
    	print(player)
    	body2 = { '3PM2017'    : player["3PM2017"],
  				      "3PM2018" 		: player["3PM2018"],
                "3PP2017"     : (player["3PP2017"] if player["3PP2017"] else 0),
                "3PA2017"     : (player["3PA2017"] if player["3PA2017"] else 0),
		         	  "AST2017" 		: player["AST2017"],
		          	"AST2018" 		: player["AST2018"],
		          	"ATO2017" 		: player["ATO2017"],
		          	"ATO2018" 		: player["ATO2018"],
		          	"BLK2017" 		: player["BLK2017"],
		          	"BLK2018" 		: player["BLK2018"],
		          	"ESPN Rank" 	: player["ESPN Rank"],
		          	"FGP2017" 		: player["FGP2017"],
                "FGA2017"     : (player["FGA2017"] if player["FGA2017"] else float(player["PTS2018"])/2.0),
		          	"FGP2018" 		: player["FGP2018"],
		          	"FTP2017" 		: player["FTP2017"],
		          	"FTP2018" 		: player["FTP2018"],
                "FTA2017"     : player["FTA2017"],
                "FTM2017"     : player["FTM2017"],
		          	"GP2017" 		: player["GP2017"],
		          	"GP2018" 		: player["GP2018"],
		          	"Min2017" 		: player["Min2017"],
		          	"Min2018" 		: player["Min2017"],
		          	"PTS2017" 		: player["PTS2017"],
		          	"PTS2018" 		: player["PTS2018"],
		          	"Player Name" 	: player["Player Name"],
		          	"REB2017" 		: player["REB2017"],
		          	"REB2018" 		: player["REB2018"],
		          	"STL2017" 		: player["STL2017"],
		          	"STL2018" 		: player["STL2018"],
		          	"TO2017" 		: player["TO2017"],
		          	"TO2018" 		: player["TO2018"],
                "DD2017"    : (player["DD2017"] if player["DD2017"] else '0'),
		          	"Team + Position" : player["Team + Position"]}

                # "3PP2017", "3PA2017", "DD2017", "FTA2017", "FTM2017"
    	print(body2)
    	es.index(index='fantasy', doc_type='player', id=i, body=body2)
    	i=i+1
    	# Input the player into the Elassandra cluster        