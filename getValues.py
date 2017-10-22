from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

import argparse

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

def get_value(raw_value, average, games_played, mini, maxi):
  values = ((normalise(mini, maxi, raw_value)- normalise(mini, maxi, average))*(games_played/82))
  return values 

def normalise(mini, maxi, value):
  return (value - mini)/float(maxi-mini)

def value_helper(category, player, averages, minimum, maximum):
  return get_value(player[category], averages["avg" + category], player["GP2018"], minimum[category], maximum[category])

def average(list_of_stuff):
  return sum(list_of_stuff)/float(len(list_of_stuff))

weight_perc = dict()
FGP_W = []

FTP_W = []

TPP_W = []

def weight_value(perc, attempts):
  return (perc)*attempts


body2 = [ "3PM2017"    ,
      "3PM2018"      ,
          "AST2017"      ,
            "AST2018"      ,
            "ATO2017"      ,
            "ATO2018"      ,
            "BLK2017"      ,
            "BLK2018"      ,
            "ESPN Rank"    ,
            "FGP2017"      ,
            "FGA2017"      ,
            "FGP2018"      ,
            "FTP2017"      ,
            "FTP2018"      ,
            "GP2017"     ,
            "GP2018"     ,
            "Min2017"      ,
            "Min2018"      ,
            "PTS2017"      ,
            "PTS2018"      ,
            "REB2017"      ,
            "REB2018"      ,
            "STL2017"      ,
            "STL2018"      ,
            "TO2017"     ,
            "TO2018"     ,
            "DD2017"     ,
            "3PP2017"    , 
            "3PA2017"    , 
            "FTA2017"    , 
            "TPP_W"    , 
            "FTP_W"    , 
            "FGP_W"    , 
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

sa = Search(using=es)
averages = sa.filter('term', _type="averages").execute().to_dict().get("hits").get("hits")[-1].get("_source")


maximum = dict()
for stat in body2:
  s = Search(using=es)
  s.aggs.metric(stat, 'max', field=stat)
  maximum[stat] = s.execute().to_dict().get("aggregations").get(stat).get("value")

minimum = dict()
for stat in body2:
  s = Search(using=es)
  s.aggs.metric(stat, 'min', field=stat)
  minimum[stat] = s.execute().to_dict().get("aggregations").get(stat).get("value")  

s = Search(using=es)
s._params['size'] = 150
players = s.filter('term', _type="player").execute().to_dict().get("hits").get("hits")
 
x=0
for p in players:
  value=0.0
  player = p.get("_source")
  body2 = { 
          "dif_3PM2018"     : value_helper("3PM2018", player, averages, minimum, maximum),
              "dif_AST2018"     : value_helper("AST2018", player, averages, minimum, maximum),
              "dif_BLK2018"     : value_helper("BLK2018", player, averages, minimum, maximum),
              "dif_FGP2018"     : value_helper("FGP2018", player, averages, minimum, maximum),
              "dif_FTP2017"     : value_helper("FTP2017", player, averages, minimum, maximum),
              "dif_FTP2018"     : value_helper("FTP2018", player, averages, minimum, maximum),
              "dif_GP2018"    : value_helper("GP2018", player, averages, minimum, maximum),
              "dif_PTS2018"     : value_helper("PTS2018", player, averages, minimum, maximum),
              "dif_REB2018"     : value_helper("REB2018", player, averages, minimum, maximum),
              "dif_STL2018"     : value_helper("STL2018", player, averages, minimum, maximum),
              "dif_3PP2017"     : value_helper("3PP2017", player, averages, minimum, maximum),
              "dif_3PA2017"     : value_helper("3PA2017", player, averages, minimum, maximum),
              "dif_DD2017"    : value_helper("DD2017", player, averages, minimum, maximum),
              "dif_FTA2017"     : value_helper("FTA2017", player, averages, minimum, maximum),
              "dif_FTM2017"     : value_helper("FTM2017", player, averages, minimum, maximum),
              "Player Name"     : player["Player Name"],
              "dif_TO2018"    : -value_helper("TO2018", player, averages, minimum, maximum),
              "dif_FGP_W"     : value_helper("FGP_W", player, averages, minimum, maximum),
              "dif_3PP_W"     : value_helper("TPP_W", player, averages, minimum, maximum),
              "dif_FTP_W"     : value_helper("FTP_W", player, averages, minimum, maximum),
              "id"            : x
              }
  for category in args.s.split(','):
    # Add it to their value
    value+= float(body2[category])
  body2.update({args.n : value, 
      })
  es.index(index='fantasy', doc_type='player_values', body=body2, id=x)
  x = x+1




