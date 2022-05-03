import pika, sys, os, json
import transform_bookie_data
import mysql.connector

def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()

	channel.queue_declare(queue='odds')

	conn = mysql.connector.connect(
		host="localhost",
		user="david",
		password="open1010",
		database="BookiesPipelineDB"
	)
	cur = conn.cursor()

	def callback(ch, method, properties, body):
		match_url, match_odds = json.loads(body.decode())

		TransformBookieData = transform_bookie_data.TransformBookieData(match_odds)
		transformed_odds = TransformBookieData.ConvertOddsToDecimal()
		transformed_odds = json.dumps(transformed_odds)

		cur.execute("""
			INSERT INTO bookie_odds
				(match_url, timestamp, odds)
			VALUES
				('{}', CURRENT_TIMESTAMP(), '{}')

			ON DUPLICATE KEY UPDATE
				timestamp = CURRENT_TIMESTAMP(),
				odds = '{}'
		""".format(match_url, transformed_odds, transformed_odds))
	
		conn.commit()
	
	channel.basic_consume(queue='odds', on_message_callback=callback, auto_ack=True)

	print(' [*] Waiting for messages. To exit press CTRL+C')
	channel.start_consuming()

main()