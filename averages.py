from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


es = Elasticsearch(['https://icelassandra:393e5687f6680c5d0b1ba1f00da7faba@fb8da90356c94a0bb4ebf52579e814bb.cu.dev.instaclustr.com:9201'])

client = Elasticsearch()
# Work get averages for each field from top 150 players
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
          	"TPP_W"			 ,
          	"FTP_W"			 ,
          	"FGP_W"			 ,
          	"FTM2017"]
          	
averages = dict()

body='''{
	  "mappings" : {
	      "averages" : {
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

	          "FGP_W" : {
	            "type" : "float"
	          },

	          "FTP_W" : {
	            "type" : "float"
	          },

	          "TPP_W" : {
	            "type" : "float"
	          },
	          "Team + Position" : {
	            "type" : "string"
	          }
	        }
	      }
	  	}
	}'''


for stat in body2:
	s = Search(using=client)
	s.aggs.metric(stat, 'avg', field=stat)
	averages["avg"+stat] = s.execute().to_dict().get("aggregations").get(stat).get("value")

try:
	es.indices.delete(index='averages')
except Exception as e:
	es.indices.create(index ='averages', body=body)

try:
	es.delete(index='fantasy', doc_type='averages', id=1)
except Exception as e:
	pass
es.index(index='fantasy', doc_type='averages', id=1, body=averages)

