from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import operator


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

client = Elasticsearch()

s = Search(using=client)
s._params['size'] = 150
player_averages = s.filter('term', _type="player_averages").execute().to_dict().get("hits").get("hits")
av = dict()

# print(player_averages)
players = dict()
for player in player_averages:
	p = player.get("_source")
	# print(p)

	players[p.get("Player Name")] = player

	av[p.get("Player Name")] = sum([
		p.get("dif_3PM2018"),
		p.get("dif_3PP_W"),
		p.get("dif_AST2018"),
		p.get("dif_FTP2018"),
		p.get("dif_PTS2018"),
		p.get("dif_REB2018"),
		p.get("dif_STL2018"),
		p.get("dif_TO2018"),
		(p.get("dif_DD2017")),
		p.get("dif_FTP_W"),
		p.get("dif_FGP_W")
		]
		)

sorted_x = sorted(av.items(), key=operator.itemgetter(1), reverse=True)
for player in sorted_x:
	print(player)
	# print(players[player[0]])
	# print("dif_AST2018" + "   " + str(players[player[0]].get("_source").get("dif_AST2018")))
	# print("dif_FTP2018" + "   " + str(players[player[0]].get("_source").get("dif_FTP2018")))
	# print("dif_FTP2018" + "   " + str(players[player[0]].get("_source").get("dif_FTP2018")))
	# print("dif_FTP_W" + "   " + str(players[player[0]].get("_source").get("dif_FTP_W")))
	# print("dif_REB2018" + "   " + str(players[player[0]].get("_source").get("dif_REB2018")))
	# print("dif_STL2018" + "   " + str(players[player[0]].get("_source").get("dif_STL2018")))
	# print("dif_TO2018" + "   " + str(players[player[0]].get("_source").get("dif_TO2018")))
	# print("dif_FGP2018" + "   " + str(players[player[0]].get("_source").get("dif_FGP2018")))
	# print("dif_3PP_W" + "   " + str(players[player[0]].get("_source").get("dif_3PP_W")))
	# print("dif_DD2017" + "   " + str(players[player[0]].get("_source").get("dif_DD2017")))
	# print("dif_FGP_W" + "   " + str(players[player[0]].get("_source").get("dif_FGP_W")))