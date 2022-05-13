from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import random

from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import json
import pika

import dump_to_gc_storage

class OddsportalScraper:
	def __init__(self):
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self.channel = connection.channel()
		self.channel.queue_declare(queue='filenames')

		options = Options()
		options.headless = True
		self.driver = webdriver.Chrome("/home/david/Downloads/chromedriver", options=options)

	def ScrapeMatchUrls(self):
		url = "https://www.oddsportal.com/soccer/england/premier-league/"
		self.driver.get(url)

		soup = BeautifulSoup(self.driver.page_source, features="lxml")

		td = soup.find_all("td", {"class": "table-participant"})

		self.match_urls = []
		for i in td:
			a_href = i.find_all("a")
			for x in a_href:
				if x.get("href") != "javascript:void(0);":
					self.match_urls.append("https://www.oddsportal.com" + x.get("href"))

	def ScrapeMatchOdds(self, match_url):
		df_data = [["match_url", "bookie", "odds"]]

		self.driver.get(match_url)
		soup = BeautifulSoup(self.driver.page_source, features="lxml")

		table = soup.find("table", {"class": "table-main"})
		tbody = table.find("tbody")

		for row in tbody.find_all("tr"):
			try:
				bookie_html, home_html, draw_html, away_html, i, x = row.find_all("td")

				bookie = bookie_html.text.strip()
				home_odds = home_html.text
				draw_odds = draw_html.text
				away_odds = away_html.text

				odds = [home_odds, draw_odds, away_odds]
				df_data.append([match_url, bookie, odds])

			except Exception:
				pass

		self.match_df = pd.DataFrame(columns=df_data[0], data=df_data[1:])

		filename = "".join([str(random.randint(0, 9)) for i in range(0,9)]) + ".csv"
		dump_to_gc_storage.UploadDfToGoogleCloudStorage(self.match_df, filename)

		self.channel.basic_publish(exchange='', routing_key='filenames', body=json.dumps(filename))

	def Run(self):
		self.ScrapeMatchUrls()
		for match_url in self.match_urls:
			self.ScrapeMatchOdds(match_url)


OddsportalScraper = OddsportalScraper()
OddsportalScraper.Run()