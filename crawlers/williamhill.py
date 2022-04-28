# selenium test scraper for williamhill

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import mysql.connector
import random

from bs4 import BeautifulSoup
import time
from datetime import datetime
import json
import pika

url = "https://sports.williamhill.com/betting/en-gb/football/competitions/OB_TY295/english-premier-league/matches/OB_MGMB/Match-Betting"

class WilliamHillScraper:
	def __init__(self, WilliamHillUrl):
		connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
		self.channel = connection.channel()
		self.channel.queue_declare(queue='odds')
		
		self.host = '127.0.0.1'
		self.user = 'david'
		self.password = 'open1010'
		self.port = 3306
		self.db = 'BookiesPipelineDB'

		self.WilliamHillUrl = WilliamHillUrl

		options = Options()
		options.headless = True
		self.driver = webdriver.Chrome("/home/david/Downloads/chromedriver", options=options)

	def ReturnDataFromWebpage(self):
		self.driver.get(self.WilliamHillUrl)
		self.page_source = self.driver.page_source

	def ParseThroughPage(self):
		self.data = {}

		soup = BeautifulSoup(self.page_source, 'html.parser')
		for i in soup.find_all("article", {"class": "sp-o-market"}):

			odds_id = "".join( [ str(random.randint(0,9)) for i in range(0,6) ] )

			match_title = i.find("main", {"class": "sp-o-market__title"}).text
			odds = [x.text for x in i.find_all("button", {"class": "sp-betbutton"})]

			self.data[odds_id] = ["williamhill", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), match_title, odds]

	def WriteToQueue(self):
		for i in self.data:
			self.channel.basic_publish(exchange='', routing_key='odds', body=json.dumps(self.data[i]))

	def Run(self):
		while True:
			self.ReturnDataFromWebpage()
			self.ParseThroughPage()
			self.WriteToQueue()

			time.sleep(5)

WilliamHillScraper = WilliamHillScraper(url)
WilliamHillScraper.Run()