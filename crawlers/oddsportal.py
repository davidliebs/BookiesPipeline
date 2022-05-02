from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import random

from bs4 import BeautifulSoup
import time
from datetime import datetime
import json
import pika

class OddsportalScraper:
	def __init__(self):
		connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
		self.channel = connection.channel()
		self.channel.queue_declare(queue='odds')

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
		self.match_odds = {}

		self.driver.get(match_url)
		soup = BeautifulSoup(self.driver.page_source)

		table = soup.find("table", {"class": "table-main"})
		tbody = table.find("tbody")

		for row in tbody.find_all("tr"):
			try:
				bookie_html, home_html, draw_html, away_html, i, x = row.find_all("td")

				bookie = bookie_html.text.strip()
				home_odds = home_html.text
				draw_odds = draw_html.text
				away_odds = away_html.text

				self.match_odds[bookie] = [home_odds, draw_odds, away_odds]
			except Exception:
				pass

	def Run(self):
		self.ScrapeMatchUrls()
		for match_url in self.match_urls:
			self.ScrapeMatchOdds(match_url)
			self.channel.basic_publish(exchange='', routing_key='odds', body=json.dumps([match_url, self.match_odds]))

OddsportalScraper = OddsportalScraper()
OddsportalScraper.Run()