from google.cloud import storage
import pika, json, os
import pandas as pd

def callback(ch, method, properties, body):
	body = json.loads(body.decode())

	blob_path = "gs://bookies_csv_files/" + body
	df = pd.read_csv(blob_path)

	df.to_csv(os.path.join("/home/david/Documents/projects/files", body), index=False)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='filenames')
channel.basic_consume(queue='filenames', on_message_callback=callback, auto_ack=True)
channel.start_consuming()