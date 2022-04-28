class TransformBookieData:
	def __init__(self, data):
		self.bookie, self.odds_timestamp, self.match_title, self.odds = data


	def CheckOddsType(self):
		odd = self.odds[0]

		# checking for decimal odds
		if "." in odd:
			self.odd_type = 1

		# checking for fraction odds
		if "/" in odd:
			self.odd_type = 2

	def ConvertOddsToDecimal(self):
		for index in range(len(self.odds)):
			odd = self.odds[index]

			if self.odd_type == 1:
				odd = int(odd)

			elif self.odd_type == 2:
				numerator, denominator = odd.split("/")
				# converting probility --> decimal odd
				odd = round( int(numerator)/int(denominator) + 1, 2)

			self.odds[index] = odd

	def Run(self):
		self.CheckOddsType()
		self.ConvertOddsToDecimal()

		return self.bookie, self.odds_timestamp, self.match_title, self.odds