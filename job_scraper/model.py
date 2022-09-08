import os
import pickle
import time
from dataclasses import dataclass
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm

from job_scraper.log import logger

DRIVER_PATH = "/Users/willchien/PycharmProjects/linkedin-job/chromedriver"

@dataclass
class LinkedinCredentials:
    """
    For security purpose, the LinkedIn account and password are set in environment variables.
    """
    @property
    def account(self):
        if os.getenv("ACCOUNT") is not None:
            return os.getenv("ACCOUNT")
        else:
            raise ValueError("Please specify your Linkedin account email in the environment variable. "
                             "eg. os.environ['ACCOUNT'] = 'your_email_address'")

    @property
    def password(self):
        if os.getenv("PASSWORD") is not None:
            return os.getenv("PASSWORD")
        else:
            raise ValueError("Please specify your Linkedin password in the environment variable. "
                             "eg. os.environ['PASSWORD'] = 'your_password'")


def check_dir():
    if not os.path.exists("data"):
        os.makedirs("data")


class JobGetter:
    def __init__(self, job: str, location: str, n_pages: int = 3):
        self.job = job
        self.location = location
        self.n_pages = n_pages
        self.links: List[str] = []
        self.failed = []
        self.driver = None
        check_dir()

    def __repr__(self):
        return (
            f"JobScraper(job={self.job}, "
            f"location={self.location}, "
            f"n_pages={self.n_pages})"
        )

    def set_credentials(self, account: str, password: str) -> None:
        os.environ["ACCOUNT"] = account
        os.environ["PASSWORD"] = password
        logger.info("Account and password have been set.")

    def login(self) -> None:
        # Instantiate the credentials for login
        cred = LinkedinCredentials()

        # Go to LinkedIn
        logger.info("Log in")
        time.sleep(3)
        self.driver.maximize_window()
        self.driver.get("https://www.linkedin.com/login")
        time.sleep(3)

        self.driver.find_element(By.ID, "username").send_keys(cred.account)
        self.driver.find_element(By.ID, "password").send_keys(cred.password)
        self.driver.find_element(By.ID, "password").send_keys(Keys.RETURN)
        time.sleep(3)

    def save_cookie(self, path: str) -> None:
        with open(path, "wb") as filehandler:
            pickle.dump(self.driver.get_cookies(), filehandler)

    def load_cookie(self, path: str) -> None:
        with open(path, "rb") as cookiesfile:
            cookies = pickle.load(cookiesfile)
            for cookie in cookies:
                self.driver.add_cookie(cookie)

    def get_job_links(self) -> None:
        url = f"https://www.linkedin.com/jobs/search/?keywords={self.job.replace(' ', '%20')}&location={self.location}"
        self.driver.get(url)

        # Get all the urls across pages
        links = []
        for page in tqdm(range(2, self.n_pages)):
            time.sleep(3)
            jobs_block = self.driver.find_element(
                By.CLASS_NAME, "jobs-search-results-list"
            )
            jobs_list = jobs_block.find_elements(By.CSS_SELECTOR, ".job-card-list")

            # Scrape all the jobs in one page
            for job in jobs_list:
                all_links = job.find_elements(By.TAG_NAME, "a")
                for a in all_links:
                    link = str(a.get_attribute("href"))
                    if link.startswith("https://www.linkedin.com/jobs/view/"):
                        links.append(link.split("?eBP")[0])

                # Scroll down for respective job element
                self.driver.execute_script("arguments[0].scrollIntoView();", job)
                time.sleep(1)

            # Click the next page
            self.driver.find_element(By.XPATH, f"//button[@aria-label='Page {page}']").click()
            time.sleep(3)

        links = list(set(links))
        logger.info(f"Found {len(links)} offers.")
        self.links = links
        self.close_session()

    @staticmethod
    def get_content_from_one_url(url: str) -> List[str]:
        # Prepare the soup
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html")

        # Basic info
        uuid = url.split("/")[-2]
        title = soup.select("h3")[0].text
        company = soup.select("img")[1].get("alt")
        place = (
            soup.select("title")[0]
            .text.split("hiring")[1]
            .split(" in ")[-1]
            .replace(" | LinkedIn", "")
        )
        post_since = (
            soup.find_all("span", {"class": "posted-time-ago__text"})[0]
            .text.replace("\n", "")
            .strip()
        )

        # Criteria
        details = soup.find_all("span",
                                {"class": "description__job-criteria-text description__job-criteria-text--criteria"})
        if len(details) > 1:
            level = details[0].text.strip()
            job_type = details[1].text.strip()
            job_cat = details[2].text.strip()
            industry = details[3].text.strip()
        else:
            job_type = details[0].text.strip()
            level = "na"
            job_cat = "na"
            industry = "na"
        descrip = soup.find_all(
            "div",
            {
                "class": "show-more-less-html__markup show-more-less-html__markup--clamp-after-5"
            },
        )[0].text

        return [
            uuid,
            title,
            company,
            place,
            post_since,
            level,
            job_type,
            job_cat,
            industry,
            descrip,
        ]

    def scrape_pages(self) -> List[List[str]]:
        results = []
        failed = []
        for url in tqdm(self.links):
            time.sleep(1)
            try:
                result = self.get_content_from_one_url(url)
                results.append(result)
            except:
                failed.append(url)
        self.failed += failed
        return results

    def to_df(self) -> pd.DataFrame:
        results = self.scrape_pages()
        col_names = ["uuid", "title", "company", "place", "post_since",
                     "level", "job_type", "job_cat", "industry", "descrip"]
        df = pd.DataFrame({c: [] for c in col_names})

        for i, r in enumerate(results):
            df.loc[len(df)] = r

        return df

    def start_session(self) -> None:
        """Turn on the driver"""
        logger.info("Session starting")
        self.driver = webdriver.Chrome(
            executable_path=DRIVER_PATH
        )

    def close_session(self) -> None:
        """Turn off the driver"""
        logger.info("Session closing")
        self.driver.close()

    def run(self) -> pd.DataFrame:
        self.start_session()
        if os.path.exists("data/cookies.txt"):
            self.driver.get("https://www.linkedin.com/")
            self.load_cookie("data/cookies.txt")
            self.driver.get("https://www.linkedin.com/")
        else:
            self.login()
            self.save_cookie("data/cookies.txt")

        logger.info("Start scraping.")
        self.get_job_links()

        df = self.to_df()

        logger.info("Done scraping.")

        return df


if __name__ == "__main__":
    getter = JobGetter("data scientist", "netherlands")
    getter.run()
