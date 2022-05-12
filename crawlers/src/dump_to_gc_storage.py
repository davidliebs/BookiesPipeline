from google.cloud import storage
import pandas as pd

def UploadDfToGoogleCloudStorage(df, filename):
	client = storage.Client()
	bucket = client.get_bucket('bookies_csv_files')

	bucket.blob(filename).upload_from_string(df.to_csv(), 'text/csv')