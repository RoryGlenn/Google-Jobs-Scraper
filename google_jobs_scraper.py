"""
This module is a web scraper that uses Playwright to search for job listings related to a given term in multiple US cities on Google.
It then extracts job details from the listings, saves the data to a JSON file, and processes the keywords to calculate their frequency and save the results in a summary file. 

The module imports several Python libraries, including `argparse`, `asyncio`, `collections`, `datetime`, `json`, `logging`, `pathlib`, `quote` from `urllib.parse`, `Locator`, `Page`, `Playwright`, and `async_playwright` from `playwright.async_api`, and `tqdm`. 

The module defines several functions, including:
- `strip_non_computer_word`: Filters out non-computer science terms from a list of tokens.
- `process_keyword`: Calculates the frequency of keywords in a JSON file containing job descriptions and highlights.
- `save_data`: Saves data to a JSON file.
- `clean_data`: Cleans data by removing extra whitespace and joining strings with a "|" separator.
- `extract_data`: Extracts job data from a web page using XPath locators and saves it in a dictionary format.
- `parse_listing_page`: Parses a listing page to extract job details.
- `run`: Opens a browser, navigates to a Google search page for job listings, scrolls down the page, parses the listings, saves the data, and processes the keywords.
- `parse_args`: Parses command line arguments.
- `main`: Runs the program.

These functions are used to perform various tasks, such as filtering out non-computer science terms from a list of tokens,
calculating the frequency of keywords in a JSON file containing job descriptions and highlights, 
saving data to a JSON file, cleaning data by removing extra whitespace and joining strings with a "|" separator, 
extracting job data from a web page using XPath locators and saving it in a dictionary format, 
parsing a listing page to extract job details, opening a browser, 
navigating to a Google search page for job listings, scrolling down the page, parsing the listings, saving the data, 
and processing the keywords, and parsing command line arguments. 

The module also defines several variables, including `logger`, `output_dir`, `dt`, `json_file_path`, `summary_file_path`, and `data`, which are used to store and manipulate data throughout the program.
"""

import argparse
import asyncio
import datetime
import json
import logging
from pathlib import Path
import time
from urllib.parse import quote

from playwright.async_api import Locator, Page, Playwright, async_playwright
from tqdm import tqdm

from keyword_const import US_CITIES

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


output_dir = Path.cwd().joinpath("output")
output_dir.mkdir(exist_ok=True)
dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
json_file_path = str(output_dir.joinpath(f"google_jobs_data_{dt}.json"))

logger.debug(f"Writing data into '{json_file_path}'")

data = []


def save_data() -> None:
    """The `save_data` function saves the `data` variable to a JSON file specified by `json_file_path`."""
    with open(json_file_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)


def clean_data(dirty_data: str or list) -> str:
    """The `clean_data` function takes in a string or a list of strings and returns a cleaned version of
    the input by removing extra whitespace and joining the strings with a "|" separator if the input is
    a list.

    Parameters
    ----------
    dirty_data : str or list
        The `dirty_data` parameter can be either a string or a list. If it is a string, the function will
    remove any extra whitespace and return the cleaned string. If it is a list, the function will clean
    each element of the list by removing extra whitespace, and then join the cleaned elements

    Returns
    -------
        The function `clean_data` returns a cleaned version of the input `dirty_data`. If `dirty_data` is a
    string, it removes any extra whitespace and returns the cleaned string. If `dirty_data` is a list of
    strings, it removes extra whitespace from each string in the list, joins them with a "|" separator,
    and returns the cleaned string.

    """
    if isinstance(dirty_data, str):
        dirty_data = " ".join(dirty_data.split()).strip()
        return dirty_data

    dirty_data = [" ".join(i.split()).strip() for i in dirty_data]
    dirty_data = " | ".join(dirty_data)
    return dirty_data


async def extract_data(job_element: Locator) -> None:
    """The function `extract_data` extracts job data from a web page using XPath locators and saves it in a
    dictionary format.

    Parameters
    ----------
    job_element : Locator
        The `job_element` parameter is a Locator object that represents a specific job element on a
    webpage. It is used to extract data from that element.

    """
    xpath_title = "//h2[@class='KLsYvd']"
    xpath_job_description_span = "//span[@class='HBvzbc']"
    xpath_job_highlights = "//div[@class='JxVj3d']"
    xpath_employer = 'div[class*="nJlQNd"]'

    title = await job_element.locator(xpath_title).inner_text()
    employer = await job_element.locator(xpath_employer).inner_text()
    job_description = await job_element.locator(
        xpath_job_description_span
    ).all_inner_texts()

    highlights_elements = job_element.locator(xpath_job_highlights)
    highlights_count = await highlights_elements.count()
    all_text_highlights = []
    for i in range(highlights_count):
        all_text_highlights.extend(
            await highlights_elements.nth(i)
            .locator("//div[@class='IiQJ2c']")
            .all_inner_texts()
        )

    title = clean_data(title)
    job_description = clean_data(job_description)
    highlights = clean_data(all_text_highlights)
    employer = clean_data(employer)

    data_to_save = {
        "title": title,
        "employer": employer,
        "job_description": job_description,
        "job_highlights": highlights,
    }

    data.append(data_to_save)


async def parse_listing_page(page: Page) -> None:
    """The `parse_listing_page` function parses a listing page to extract job details.

    Parameters
    ----------
    page : Page
        The `page` parameter is an instance of the `Page` class. It represents a web page that you want to
    parse.

    """
    xpath_jobs_tabs = "//div[@class='gws-plugins-horizon-jobs__tl-lif']"

    try:
        await page.wait_for_selector(xpath_jobs_tabs)
    except Exception as e:
        # Playwright Timeout error can occur when no jobs are found in the search
        logger.debug(e)
        return

    jobs = page.locator(xpath_jobs_tabs)
    jobs_count = await jobs.count()
    logger.debug(f"Parse {jobs_count} jobs")

    xpath_job_detail = "//div[@id='gws-plugins-horizon-jobs__job_details_page']"
    job_details = page.locator(xpath_job_detail)
    for i in range(jobs_count):
        job_element = job_details.nth(i)
        await extract_data(job_element)


async def run(playwright: Playwright, max_scroll: int, query: str) -> None:
    """The function opens a browser, navigates to a Google search page for job listings, scrolls down the
    page, parses the listings, saves the data, and processes the keyword.

    Parameters
    ----------
    playwright : Playwright
        The `playwright` parameter is an instance of the Playwright class, which is used to control the
    browser and perform actions on web pages.
    max_scroll : int
        The `max_scroll` parameter determines the maximum number of times the page will be scrolled down to
    load more content. It is an integer value that specifies the number of scroll actions to perform.
    query : str
        The `query` parameter is a string that represents the search query to be used on Google. It is used
    to search for job listings related to the query.

    """
    # Initializing browser and opening a new page
    browser = await playwright.firefox.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    url = f"https://www.google.com/search?hl=en&q={quote(query)}&ibp=htl;jobs"
    await page.goto(url, wait_until="domcontentloaded")

    await page.wait_for_timeout(2000)
    await asyncio.sleep(5)

    job_tree = page.locator("//div[@role='tree']")
    await job_tree.click()
    previousYBound = 0

    for _ in tqdm(range(max_scroll), desc="Scroll"):
        await page.mouse.wheel(0, 5000)
        await asyncio.sleep(2)
        box3 = await job_tree.bounding_box()
        if previousYBound == box3["y"]:
            break
        previousYBound = box3["y"]

    await parse_listing_page(page)
    logger.debug(f"Finished Parsing `{query}`")
    save_data()
    # process_keyword()
    await context.close()
    await browser.close()


def parse_args():
    """The `parse_args` function is used to parse command line arguments, specifically the job term to
    search and the maximum scrolling count.

    Returns
    -------
        The function `parse_args()` returns the parsed command-line arguments.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("term", help="Job term to search")
    parser.add_argument(
        "--max_scroll", default=100, type=int, help="Maximum scrolling count"
    )

    return parser.parse_args()


async def main() -> None:
    """The `main` function uses Playwright to run a search query for a given term in multiple US cities."""
    start_time = time.perf_counter()
    args = parse_args()

    for city in US_CITIES:
        async with async_playwright() as playwright:
            await run(
                playwright,
                max_scroll=args.max_scroll,
                query=f"{args.term} in {city}",
            )

    minutes = (time.perf_counter() - start_time) / 60
    logger.debug(f"Time elapsed: {round(minutes, 1)} minutes")


if __name__ == "__main__":
    asyncio.run(main())
