import pika, sys, os, json
import transform_bookie_data

def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()

	channel.queue_declare(queue='odds')

	def callback(ch, method, properties, body):
		data = json.loads(body.decode())
		TransformData = transform_bookie_data.TransformBookieData(data)
		bookie, odds_timestamp, match_title, odds = TransformData.Run()

	channel.basic_consume(queue='odds', on_message_callback=callback, auto_ack=True)

	print(' [*] Waiting for messages. To exit press CTRL+C')
	channel.start_consuming()

main()