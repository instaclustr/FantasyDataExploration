import csv
players = dict()

with open('espn-predictions.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for player in reader:
    	players[player["Player Name"]] = player


with open('3pp.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for player in reader:
    	name = player["PLAYER"].split(",")[0]
    	if(name in players.keys()):
    		players[name].update({"3PP2017": player["3P%"]})
    		players[name].update({"3PA2017": player["3PA"]}) 	

with open('dd.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for player in reader:
    	name = player["PLAYER"].split(",")[0]
    	if(name in players.keys()):
    		players[name].update({"DD2017": player["DBLDBL"]})

with open('ftp.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for player in reader:
    	name = player["PLAYER"].split(",")[0]
    	if(name in players.keys()):
    		players[name].update({"FTA2017": player["FTA"]})
    		players[name].update({"FTM2017": player["FTM"]})

with open('FGP.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for player in reader:
        name = player["PLAYER"].split(",")[0]
        if(name in players.keys()):
            players[name].update({"FGA2017": player["FGA"]})

# print(players["James Harden"].keys())
with open("OUTPUT_STATS.csv", 'w') as csvfile:
	fieldnames = ['ESPN Rank', 'FGA2017', 'Player Name', 'Team + Position', 'GP2017', 'Min2017', 'FGP2017', 'FTP2017', '3PM2017', 'REB2017', 'AST2017', 'ATO2017', 'STL2017', 'BLK2017', 'TO2017', 'PTS2017', 'GP2018', 'Min2018', 'FGP2018', 'FTP2018', '3PM2018', 'REB2018', 'AST2018', 'ATO2018', 'STL2018', 'BLK2018', 'TO2018', 'PTS2018', '3PP2017', '3PA2017', 'DD2017', 'FTA2017', 'FTM2017']
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()
	for key in players.keys():
		# print(key)
		writer.writerow(players[key])
    