from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

def get_value(raw_value, average, games_played, mini, maxi):
	# print(raw_value, average, games_played, mini, maxi)
	values = ((normalise(mini, maxi, raw_value)- normalise(mini, maxi, average))*(games_played/82))/(normalise(mini, maxi, average))
	return values	

def normalise(mini, maxi, value):
	return (value - mini)/float(maxi-mini)

def value_helper(category, player, averages, minimum, maximum):
	return get_value(player[category], averages["avg" + category], player["GP2018"], minimum[category], maximum[category])

def value_helper_weighted():
	return 1

def average(list_of_stuff):
	return sum(list_of_stuff)/float(len(list_of_stuff))


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


weight_perc = dict()
FGP_W = []

FTP_W = []

TPP_W = []
def weight_value(perc, attempts):
	return (perc)*attempts

client = Elasticsearch()

body2 = [	"3PM2017"		 ,
			"3PM2018" 		 ,
         	"AST2017" 		 ,
          	"AST2018" 		 ,
          	"ATO2017" 		 ,
          	"ATO2018" 		 ,
          	"BLK2017" 		 ,
          	"BLK2018" 		 ,
          	"ESPN Rank" 	 ,
          	"FGP2017" 		 ,
          	"FGA2017" 		 ,
          	"FGP2018" 		 ,
          	"FTP2017" 		 ,
          	"FTP2018" 		 ,
          	"GP2017" 		 ,
          	"GP2018" 		 ,
          	"Min2017" 		 ,
          	"Min2018" 		 ,
          	"PTS2017" 		 ,
          	"PTS2018" 		 ,
          	"REB2017" 		 ,
          	"REB2018" 		 ,
          	"STL2017" 		 ,
          	"STL2018" 		 ,
          	"TO2017" 		 ,
          	"TO2018" 		 ,
          	"DD2017"		 ,
          	"3PP2017"		 , 
          	"3PA2017"		 , 
          	"FTA2017"		 , 
          	"TPP_W"		 , 
          	"FTP_W"		 , 
          	"FGP_W"		 , 
          	"FTM2017"]

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


maximum = dict()
for stat in body2:
	s = Search(using=client)
	s.aggs.metric(stat, 'max', field=stat)
	maximum[stat] = s.execute().to_dict().get("aggregations").get(stat).get("value")

minimum = dict()
for stat in body2:
	s = Search(using=client)
	s.aggs.metric(stat, 'min', field=stat)
	minimum[stat] = s.execute().to_dict().get("aggregations").get(stat).get("value")	
print(maximum)
print(minimum)
print(averages)
# .get("hits").get("hits")[0].get("_source")


s = Search(using=client)
s._params['size'] = 150
players = s.filter('term', _type="player").execute().to_dict().get("hits").get("hits")
weight_perc["FGP_W"] = []
weight_perc["FTP_W"] = []
weight_perc["TPP_W"] = []


for player_ery in players:
	player = player_ery.get("_source")
	weight_perc["FGP_W"].append(weight_value(player["FGP2017"], player["FGP2017"]))
	weight_perc["FTP_W"].append(weight_value(player["FTP2017"], player["FTA2017"]))
	weight_perc["TPP_W"].append(weight_value(player["3PP2017"], player["3PA2017"]))

 
i=0
for player_ery in players:
	# print(averages)
	player = player_ery.get("_source")
	body2 = {	
  				"dif_3PM2018" 		: value_helper("3PM2018", player, averages, minimum, maximum),
	          	"dif_AST2018" 		: value_helper("AST2018", player, averages, minimum, maximum),
	          	"dif_BLK2018" 		: value_helper("BLK2018", player, averages, minimum, maximum),
	          	"dif_FGP2018" 		: value_helper("FGP2018", player, averages, minimum, maximum),
	          	
	          	"dif_FTP2017" 		: value_helper("FTP2017", player, averages, minimum, maximum),
	          	"dif_FTP2018" 		: value_helper("FTP2018", player, averages, minimum, maximum),
	          	
	          	"dif_GP2018" 		: value_helper("GP2018", player, averages, minimum, maximum),
	          	
          	
	          	"dif_PTS2018" 		: value_helper("PTS2018", player, averages, minimum, maximum),
	          	
	          	"dif_REB2018" 		: value_helper("REB2018", player, averages, minimum, maximum),
	          	
	          	"dif_STL2018" 		: value_helper("STL2018", player, averages, minimum, maximum),

	          	"dif_3PP2017" 		: value_helper("3PP2017", player, averages, minimum, maximum),
	          	"dif_3PA2017" 		: value_helper("3PA2017", player, averages, minimum, maximum),
	          	"dif_DD2017" 		: value_helper("DD2017", player, averages, minimum, maximum),
	          	"dif_FTA2017" 		: value_helper("FTA2017", player, averages, minimum, maximum),
	          	"dif_FTM2017" 		: value_helper("FTM2017", player, averages, minimum, maximum),
	          	
	          	"Player Name" 		: player["Player Name"],
	          	"dif_TO2018" 		: -value_helper("TO2018", player, averages, minimum, maximum),
	          						  
	          	"dif_FGP_W"			: value_helper("FGP_W", player, averages, minimum, maximum),

	          	"dif_3PP_W"			: value_helper("TPP_W", player, averages, minimum, maximum),

	          	"dif_FTP_W"			: value_helper("FTP_W", player, averages, minimum, maximum),
	
	          	}
	body2.update({"player value" : sum([
  				body2["dif_3PM2018"], 
				body2["dif_AST2018"],
				body2["dif_PTS2018"],
				body2["dif_REB2018"],
				body2["dif_STL2018"],
				body2["dif_TO2018"],
				body2["dif_DD2017"],
				body2["dif_BLK2018"],
				body2["dif_FGP_W"],
				body2["dif_3PP_W"],
				body2["dif_FTP_W"]])
  		})
	body2.update({"player value punted" : sum([
  				# body2["dif_3PM2018"], 
				body2["dif_AST2018"],
				body2["dif_PTS2018"],
				body2["dif_REB2018"],
				body2["dif_STL2018"],
				body2["dif_TO2018"],
				# body2["dif_DD2017"],
				body2["dif_BLK2018"],
				body2["dif_FGP_W"],
				# body2["dif_3PP_W"],
				body2["dif_FTP_W"]])
  		})
	# print(body2)
	es.index(index='fantasy', doc_type='player_averages', id=i, body=body2)
	i=i+1




