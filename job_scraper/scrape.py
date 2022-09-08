import json
import time
from typing import List
import multiprocessing as mp
from multiprocessing import Pool
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from job_scraper.log import logger


class JobScraper:
    def __init__(self, job: str, location: str, n_pages: int = 3):
        self.job = job
        self.location = location
        assert n_pages >= 0, "Must >= 0"
        self.n_pages = n_pages
        self.urls = []
        self.result_df = None

    def __repr__(self):
        return (
            f"JobScraper(job={self.job}, "
            f"location={self.location}, "
            f"n_pages={self.n_pages})"
        )

    def get_urls_n_pages(self) -> None:
        urls = []
        for i in tqdm(range(self.n_pages)):
            time.sleep(5)
            urls = self.get_urls_from_one_page(i)
            urls += urls
        self.urls = list(set(urls))
        logger.info(
            f"Total jobs as {self.job.upper()} in {self.location.upper()} found: {len(urls)}"
        )

    def get_urls_from_one_page(self, i: int) -> List[str]:
        logger.info(f"Scraping page {i + 1}")
        start = i  # 1 + 24 * i
        url = f"https://www.linkedin.com/jobs/search/?keywords={self.job.replace(' ', '%20')}&location={self.location}&start={start}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text)

        blocks = soup.find_all(attrs={"class": "base-card__full-link"})
        urls = [b["href"].split("?refId")[0] for b in blocks]
        return urls

    @staticmethod
    def fetch_info_from_one_url(url: str) -> List[str]:
        response = requests.get(url)
        soup = BeautifulSoup(response.text)
        data = [
            json.loads(x.string)
            for x in soup.find_all("script", type="application/ld+json")
        ]

        # Basic
        title = data[0]["title"]
        descrip = data[0]["description"]
        emp_type = data[0]["employmentType"]

        # Time
        date_posted = data[0]["datePosted"]
        data_valid = data[0]["validThrough"]

        # Company info
        company_name = data[0]["hiringOrganization"]["name"]
        company_type = data[0]["hiringOrganization"]["@type"]
        company_url = data[0]["hiringOrganization"]["sameAs"]
        company_industry = data[0]["industry"]

        # Location
        country = data[0]["jobLocation"]["address"]["addressCountry"]
        city = data[0]["jobLocation"]["address"]["addressLocality"]
        lat = data[0]["jobLocation"]["latitude"]
        long = data[0]["jobLocation"]["longitude"]

        # Job requirement
        education = data[0]["educationRequirements"]["credentialCategory"]

        return [
            title,
            descrip,
            emp_type,
            date_posted,
            data_valid,
            company_name,
            company_type,
            company_url,
            company_industry,
            country,
            city,
            lat,
            long,
            education,
        ]

    def scrape_pages(self):
        logger.info("*** Start scraping results ***")

        col_names = [
            "title",
            "descrip",
            "emp_type",
            "date_posted",
            "data_valid",
            "company_name",
            "company_type",
            "company_url",
            "company_industry",
            "country",
            "city",
            "lat",
            "long",
            "education",
            "url",
        ]
        result_df = pd.DataFrame({col: [] for col in col_names})

        # Scrape pages with multiprocessing to improve efficiency
        content = []
        failed = []
        for url in tqdm(self.urls):
            time.sleep(
                1
            )  # it has to have to some pauses or else it will not return the right response
            try:
                result = self.fetch_info_from_one_url(url)
                result.append(url)
                content.append(result)
            except:
                failed.append(url)

        for i, c in enumerate(content):
            result_df.loc[len(result)] = c
        self.result_df = result_df
        return result_df

    def run(self):
        self.get_urls_n_pages()
        result = self.scrape_pages()
        return result
