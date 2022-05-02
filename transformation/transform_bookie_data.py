class TransformBookieData:
	def __init__(self, data):
		self.data = data
		self.transformed_data = {}

	def ConvertOddsToDecimal(self):
		for bookie in self.data:
			decimal_odds = []

			odds = self.data[bookie]

			for probability in odds:
				numerator, denominator = probability.split("/")
				# converting probility --> decimal odd
				decimal_odd = round( int(numerator)/int(denominator) + 1, 2)

				decimal_odds.append(decimal_odd)
			
			self.transformed_data[bookie]=decimal_odds

		return self.transformed_data