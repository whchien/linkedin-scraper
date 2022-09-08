from typing import List
import pandas as pd
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import pickle
import os
from dataclasses import dataclass

from tqdm import tqdm

from job_scraper.log import logger


@dataclass
class LinkedinCredentials:
    @property
    def account(self):
        return os.getenv("ACCOUNT")

    @property
    def password(self):
        return os.getenv("PASSWORD")


def check_dir():
    if not os.path.exists("data"):
        os.makedirs("data")


class LinkedInScraper:
    def __init__(self, job: str, location: str, delay=5):
        self.job = job
        self.location = location
        self.links: List[str] = []
        check_dir()

        logger.info("Starting driver")
        self.driver = webdriver.Chrome(
            executable_path="/Users/willchien/PycharmProjects/linkedin-job/chromedriver"
        )

    def login(self) -> None:
        """Go to linkedin and login"""

        # Instantiate the credentials for login
        cred = LinkedinCredentials()

        # Go to LinkedIn
        logger.info("Log in")
        time.sleep(3)
        self.driver.maximize_window()
        self.driver.get("https://www.linkedin.com/login")
        time.sleep(self.delay)

        self.driver.find_element(By.ID, "username").send_keys(cred.account)
        self.driver.find_element(By.ID, "password").send_keys(cred.password)

        self.driver.find_element(By.ID, "password").send_keys(Keys.RETURN)
        time.sleep(self.delay)

    def save_cookie(self, path: str) -> None:
        with open(path, "wb") as filehandler:
            pickle.dump(self.driver.get_cookies(), filehandler)

    def load_cookie(self, path: str):
        with open(path, "rb") as cookiesfile:
            cookies = pickle.load(cookiesfile)
            for cookie in cookies:
                self.driver.add_cookie(cookie)

    def get_job_links(self) -> None:
        """Enter keywords into search bar"""
        logger.info("Searching jobs page")
        url = f"https://www.linkedin.com/jobs/search/?keywords={self.job.replace(' ', '%20')}&location={self.location}"
        self.driver.get(url)

        # Get all links for these offers
        links = []
        for page in range(2, 10):
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
                        links.append(link)

                # Scroll down for each job element
                self.driver.execute_script("arguments[0].scrollIntoView();", job)
                time.sleep(1)

            # go to next page:
            logger.info(f"Done page: {page-1}")
            self.driver.find_element(
                By.XPATH, f"//button[@aria-label='Page {page}']"
            ).click()
            time.sleep(4)

        links = list(set(links))
        logger.info(f"Found {len(links)} offers.")
        self.links = links

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

    def scrape_pages(self):
        results = []
        failed = []
        for url in tqdm(self.links):
            time.sleep(1)
            try:
                result = self.get_content_from_one_url(url)
                results.append(result)
            except:
                failed.append(url)
        return results

    def to_df(self) -> pd.DataFrame:
        results = self.scrape_pages()
        col_names = ["uuid", "title", "company", "place", "post_since",
                     "level", "job_type", "job_cat", "industry", "descrip"]
        df = pd.DataFrame({c: [] for c in col_names})

        for i, r in enumerate(results):
            df.loc[len(df)] = r

        return df

    def close_session(self):
        """This function closes the actual session"""
        logger.info("Closing session")
        self.driver.close()

    def run(self):
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
        self.close_session()


if __name__ == "__main__":
    bot = LinkedInScraper("data scientist", "netherlands")
    bot.run()
    df = pd.DataFrame({"links": bot.links})
    df.to_csv("links.csv")
