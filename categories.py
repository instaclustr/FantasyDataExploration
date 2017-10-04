from elasticsearch import Elasticsearch

import json
from elasticsearch_dsl import Search

client = Elasticsearch()

es = Elasticsearch(['https://icelassandra:393e5687f6680c5d0b1ba1f00da7faba@fb8da90356c94a0bb4ebf52579e814bb.cu.dev.instaclustr.com:9201'])

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

s = Search(using=client)
s._params['size'] = 150
players = s.filter('term', _type="player").execute().to_dict().get("hits").get("hits")

for p in players:
	player = p.get("_source")
	for category in categories:
		body2 = {"category" : category,
				"value" : player[category]
				}
		es.index(index='fantasy', doc_type='category', body=body2)

