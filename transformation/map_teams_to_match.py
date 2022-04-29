from difflib import SequenceMatcher
import mysql.connector

import random
import json


class MapOddsToMatch:
	def __init__(self, data):
		self.bookie, self.odds_timestamp, self.match_title, self.odds = data
	
	def ScrapeMatchTitles(self):
		self.conn = mysql.connector.connect(
			host = "localhost",
			user = "david",
			password = "open1010",
			database = "BookiesPipelineDB"
		)

		self.cur = self.conn.cursor()

		self.cur.execute("SELECT * FROM bookie_odds")
		self.bookie_odds = self.cur.fetchall()

	def CheckSimilarity(self):
		for record in self.bookie_odds:
			current_match_title = record[1]
			similarity = SequenceMatcher(None, current_match_title, self.match_title).ratio()

			if similarity > 0.7:
				self.match_record_to_update = record[0]
				return
			
		self.match_record_to_update = 0

	def UpdateRecord(self):
		# if no match is registered
		if self.match_record_to_update == 0:
			match_uid = "".join( [ str(random.randint(0,9)) for i in range(0,5) ] )
			self.cur.execute("INSERT INTO bookie_odds (match_uid, match_title, bookie, odds_timestamp, odds) VALUES ('{}', '{}', '{}', '{}', '{}')".format(match_uid, self.match_title, self.bookie, self.odds_timestamp, json.dumps(self.odds)))

		# updating odds for a registered match
		else:
			self.cur.execute("UPDATE bookie_odds SET odds_timestamp='{}', odds='{}' WHERE match_uid='{}' and bookie='{}'".format(self.odds_timestamp, json.dumps(self.odds), self.match_record_to_update, self.bookie))

		self.conn.commit()
		self.conn.close()
		
	def Run(self):
		self.ScrapeMatchTitles()
		self.CheckSimilarity()
		self.UpdateRecord()


# similarity = SequenceMatcher(None, i[1], self.match_title).ratio()
