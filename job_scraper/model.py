from typing import List
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pickle
import os
from dataclasses import dataclass
from job_scraper.log import logger


@dataclass
class LinkedinCredentials:

    @property
    def account(self):
        return os.getenv("ACCOUNT")

    @property
    def password(self):
        return os.getenv("PASSWORD")


class LinkedInScraper:
    def __init__(self, job: str, location: str, delay=5):
        self.job = job
        self.location = location
        self.links: List[str] = []

        if not os.path.exists("data"):
            os.makedirs("data")

        self.delay = delay
        logger.info("Starting driver")
        self.driver = webdriver.Chrome(executable_path="/Users/willchien/PycharmProjects/linkedin-job/chromedriver")

    def login(self) -> None:
        """Go to linkedin and login"""

        # Instantiate the credentials for login
        cred = LinkedinCredentials()

        # Go to LinkedIn
        logger.info("Log in")
        self.driver.maximize_window()
        self.driver.get('https://www.linkedin.com/login')
        time.sleep(self.delay)

        self.driver.find_element(By.ID, 'username').send_keys(cred.account)
        self.driver.find_element(By.ID, 'password').send_keys(cred.password)

        self.driver.find_element(By.ID, 'password').send_keys(Keys.RETURN)
        time.sleep(self.delay)

    def save_cookie(self, path: str) -> None:
        with open(path, 'wb') as filehandler:
            pickle.dump(self.driver.get_cookies(), filehandler)

    def load_cookie(self, path: str):
        with open(path, 'rb') as cookiesfile:
            cookies = pickle.load(cookiesfile)
            for cookie in cookies:
                self.driver.add_cookie(cookie)

    def get_links_from_one_page(self):
        pass

    def get_job_links(self, keywords: str, location: str) -> None:
        """Enter keywords into search bar
        """
        logger.info("Searching jobs page")
        url = f"https://www.linkedin.com/jobs/search/?keywords={keywords.replace(' ', '%20')}&location={location}"
        self.driver.get(url)

        # Get all links for these offers
        links = []
        # Navigate 13 pages
        try:
            for page in range(2, 3):
                time.sleep(2)
                jobs_block = self.driver.find_element_by_class_name('jobs-search-results__list')
                jobs_list = jobs_block.find_elements(By.CSS_SELECTOR, '.jobs-search-results__list-item')

                for job in jobs_list:
                    all_links = job.find_elements_by_tag_name('a')
                    for a in all_links:
                        if str(a.get_attribute('href')).startswith(
                                "https://www.linkedin.com/jobs/view") and a.get_attribute('href') not in links:
                            links.append(a.get_attribute('href'))
                        else:
                            pass
                    # scroll down for each job element
                    self.driver.execute_script("arguments[0].scrollIntoView();", job)

                logger.info(f'Collecting the links in the page: {page - 1}')
                # go to next page:
                self.driver.find_element_by_xpath(f"//button[@aria-label='Page {page}']").click()
                time.sleep(3)
        except:
            pass
        logger.info('Found ' + str(len(links)) + ' links for job offers')
        self.links = links
        print(links)

    def wait(self, t_delay: int = None):
        """Just easier to build this in here.
        Parameters
        ----------
        t_delay [optional] : int
            seconds to wait.
        """
        delay = self.delay if t_delay == None else t_delay
        time.sleep(delay)

    def scroll_to(self, job_list_item):
        """Just a function that will scroll to the list item in the column
        """
        self.driver.execute_script("arguments[0].scrollIntoView();", job_list_item)
        job_list_item.click()
        time.sleep(self.delay)

    def get_position_data(self, job: str) -> List[str]:
        """Gets the position data for a posting.
        Parameters
        ----------
        job : Selenium webelement
        Returns
        -------
        list of strings : [position, company, location, details]
        """
        [position, company, location] = job.text.split('\n')[:3]
        details = self.driver.find_element(By.ID, "job-details").text
        return [position, company, location, details]

    def wait_for_element_ready(self, by, text):
        try:
            WebDriverWait(self.driver, self.delay).until(EC.presence_of_element_located((by, text)))
        except TimeoutException:
            logger.debug("wait_for_element_ready TimeoutException")
            pass

    def close_session(self):
        """This function closes the actual session"""
        logger.info("Closing session")
        self.driver.close()

    def run(self, email: str, password: str, keywords: str, location: str):
        if os.path.exists("data/cookies.txt"):
            self.driver.get("https://www.linkedin.com/")
            self.load_cookie("data/cookies.txt")
            self.driver.get("https://www.linkedin.com/")
        else:
            self.login()
            self.save_cookie("data/cookies.txt")

        logger.info("Begin linkedin keyword search")
        self.get_job_links(keywords, location)
        self.wait()
        #
        # # scrape pages,only do first 8 pages since after that the data isn't
        # # well suited for me anyways:
        # df = pd.DataFrame({"title": [],
        #                    "company": [],
        #                    "location": [],
        #                    "detail": []})
        # content = []
        # for page in range(2, 3):
        #     # get the jobs list items to scroll through:
        #     jobs = self.driver.find_elements(By.CLASS_NAME, "occludable-update")
        #     for job in jobs:
        #         try:
        #             self.scroll_to(job)
        #             result = self.get_position_data(job)
        #             logger.info(f"DONE: {result[0]} @ {result[1]}")
        #             content.append(result)
        #         except ValueError:
        #             pass
        #     bot.driver.execute_script("arguments[0].scrollIntoView();", job)
        #
        #     # go to next page:
        #     bot.driver.find_element(By.XPATH, f"//button[@aria-label='Page {page}']").click()
        #     bot.wait()
        #
        # for i, c in enumerate(content):
        #     df.loc[len(df)] = c
        # df.to_csv("results.csv")
        # logger.info("Done scraping.")
        # bot.close_session()


if __name__ == "__main__":
    cred = LinkedinCredentials()
    print(cred.account)
    # email = "locriginal@gmail.com"
    # password = "twtw54115"
    # bot = LinkedInScraper()
    # bot.run(email, password, "Data Scientist", "Netherlands")
