from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

client = Elasticsearch()

body='''{
  "mappings" : {
      "player_differences" : {
        "properties" : {
          "dif_3PM2017" : {
            "type" : "float"
          },
          "dif_3PM2018" : {
            "type" : "float"
          },
          "dif_AST2017" : {
            "type" : "float"
          },
          "dif_AST2018" : {
            "type" : "float"
          },
          "dif_ATO2017" : {
            "type" : "float"
          },
          "dif_ATO2018" : {
            "type" : "float"
          },
          "dif_BLK2017" : {
            "type" : "float"
          },
          "dif_BLK2018" : {
            "type" : "float"
          },
          "dif_FGP2017" : {
            "type" : "float"
          },
          "dif_FGP2018" : {
            "type" : "float"
          },
          "dif_FTP2017" : {
            "type" : "float"
          },
          "dif_FTP2018" : {
            "type" : "float"
          },
          "dif_GP2017" : {
            "type" : "float"
          },
          "dif_GP2018" : {
            "type" : "float"
          },
          "dif_Min2017" : {
            "type" : "float"
          },
          "dif_Min2018" : {
            "type" : "float"
          },
          "dif_PTS2017" : {
            "type" : "float"
          },
          "dif_PTS2018" : {
            "type" : "float"
          },
          "Player Name" : {
            "type" : "string",
        	"index":    "not_analyzed"
          },
          "dif_REB2017" : {
            "type" : "float"
          },
          "dif_REB2018" : {
            "type" : "float"
          },
          "dif_STL2017" : {
            "type" : "float"
          },
          "dif_STL2018" : {
            "type" : "float"
          },
          "dif_TO2017" : {
            "type" : "float"
          },
          "dif_TO2018" : {
            "type" : "float"
          },
          "dif_3PP2017" : {
            "type" : "float"
          },
          "dif_3PA2017" : {
            "type" : "float"
          },
          "dif_DD2017" : {
            "type" : "float"
          },
          "dif_FTA2017" : {
            "type" : "float"
          },
          "dif_FTM2017" : {
            "type" : "float"
          },
          "dif_3PP_W": {
          	"type" : "float"
          },
          "dif_FTP_W": {
          	"type" : "float"
          },
          "dif_FGP_W": {
          	"type" : "float"
          }
        }
      }
  	}
}'''

sa = Search(using=client)
averages = sa.filter('term', _type="averages").execute().to_dict().get("hits").get("hits")[-1].get("_source")
# print(averages)
# .get("hits").get("hits")[0].get("_source")


s = Search(using=client)
s._params['size'] = 150
players = s.filter('term', _type="player").execute().to_dict().get("hits").get("hits")

 
i=0
for player_ery in players:
	# print(averages)
	player = player_ery.get("_source")
	body2 = {	
  				"dif_3PM2018" 		: ((player["3PM2018"]- averages["avg3PM2018"])/averages["avg3PM2018"]*100)*(player["GP2018"]/82),
	          	"dif_AST2018" 		: ((player["AST2018"]- averages["avgAST2018"])/averages["avgAST2018"]*100)*(player["GP2018"]/82),
	          	
	          	
	          	
	          	"dif_BLK2018" 		: ((player["BLK2018"]- averages["avgBLK2018"])/averages["avgBLK2018"]*100)*(player["GP2018"]/82),
	          	
	          	"dif_FGP2018" 		: ((player["FGP2018"]- averages["avgFGP2018"])/averages["avgFGP2018"]*100)*(player["GP2018"]/82),
	          	"dif_FGP_W"			: ((player["FGA2017"]-averages["avg3PA2017"]) * (player["FGP2017"] - averages["avgFGP2017"])*100)*(player["GP2018"]/82),
	          	"dif_FTP2017" 		: ((player["FTP2017"]- averages["avgFTP2017"])/averages["avgFTP2017"]*100)*(player["GP2018"]/82),
	          	"dif_FTP2018" 		: ((player["FTP2018"]- averages["avgFTP2018"])/averages["avgFTP2018"]*100)*(player["GP2018"]/82),
	          	
	          	"dif_GP2018" 		: ((player["GP2018"]- averages["avgGP2018"])/averages["avgGP2018"]*100)*(player["GP2018"]/82),
	          	
	          	"dif_3PP_W"			: ((player["3PA2017"]-averages["avg3PA2017"]) * (player["3PP2017"] - averages["avg3PP2017"])*100)*(player["GP2018"]/82),

	          	"dif_FTP_W"			: ((player["FTA2017"]-averages["avgFTA2017"]) * ((player["FTA2017"] / player["FTM2017"])/((averages["avgFTA2017"] / averages["avgFTM2017"])))),
	          	
	          	"dif_PTS2018" 		: ((player["PTS2018"]- averages["avgPTS2018"])/averages["avgPTS2018"]*100)*(player["GP2018"]/82),
	          	
	          	"dif_REB2018" 		: ((player["REB2018"]- averages["avgREB2018"])/averages["avgREB2018"]*100)*(player["GP2018"]/82),
	          	
	          	"dif_STL2018" 		: ((player["STL2018"]- averages["avgSTL2018"])/averages["avgSTL2018"]*100)*(player["GP2018"]/82),

	          	"dif_3PP2017" 		: ((player["3PP2017"]- averages["avg3PP2017"])/averages["avg3PP2017"]*100)*(player["GP2018"]/82),
	          	"dif_3PA2017" 		: ((player["3PA2017"]- averages["avg3PA2017"])/averages["avg3PA2017"]*100)*(player["GP2018"]/82),
	          	"dif_DD2017" 		: ((player["DD2017"]- averages["avgDD2017"])/averages["avgDD2017"]*100),
	          	"dif_FTA2017" 		: ((player["FTA2017"]- averages["avgFTA2017"])/averages["avgFTA2017"]*100)*(player["GP2018"]/82),
	          	"dif_FTM2017" 		: ((player["FTM2017"]- averages["avgFTM2017"])/averages["avgFTM2017"]*100)*(player["GP2018"]/82),
	          	
	          	"Player Name" 		: player["Player Name"],
	          	"dif_TO2018" 		: ((-player["TO2018"]+ averages["avgTO2018"])/averages["avgTO2018"]*100)*(player["GP2018"]/82),
	          	"player value" 		: sum([
					((player["3PM2018"]- averages["avg3PM2018"])/averages["avg3PM2018"]*100)*(player["GP2018"]/82),
					((player["AST2018"]- averages["avgAST2018"])/averages["avgAST2018"]*100)*(player["GP2018"]/82),
					((player["FGA2017"]-averages["avg3PA2017"]) * (player["FGP2017"] - averages["avgFGP2017"])*100)*(player["GP2018"]/82),
					((player["PTS2018"]- averages["avgPTS2018"])/averages["avgPTS2018"]*100)*(player["GP2018"]/82),
					((player["REB2018"]- averages["avgREB2018"])/averages["avgREB2018"]*100)*(player["GP2018"]/82),
					((player["STL2018"]- averages["avgSTL2018"])/averages["avgSTL2018"]*100)*(player["GP2018"]/82),
					((-player["TO2018"]+ averages["avgTO2018"])/averages["avgTO2018"]*100)*(player["GP2018"]/82),
					((player["FTA2017"]-averages["avgFTA2017"]) * ((player["FTA2017"] / player["FTM2017"])/((averages["avgFTA2017"] / averages["avgFTM2017"])))),
					((player["FTP2018"]- averages["avgFTP2018"])/averages["avgFTP2018"]*100)*(player["GP2018"]/82),
					((player["3PA2017"]-averages["avg3PA2017"]) * (player["3PP2017"] - averages["avg3PP2017"])*100)*(player["GP2018"]/82),
					((player["DD2017"]- averages["avgDD2017"])/averages["avgDD2017"]*100),
					]
					)}
	print(body2)
	es.index(index='fantasy', doc_type='player_averages', id=i, body=body2)
	i=i+1

# print(stuff.to_dict())
