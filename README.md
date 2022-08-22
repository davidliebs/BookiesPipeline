# BookiesPipeline

System Architecture:
<img src="https://github.com/davidliebs/BookiesPipeline/blob/main/systems-architecture.png"></img>

Scrapes bookie odds from an odds comparison site. This is pushed to Google Cloud Storage as csv files. A python program downloads these files and a Pyspark streaming program ingests these files, runs transformations and pushes them to Google Cloud SQL
