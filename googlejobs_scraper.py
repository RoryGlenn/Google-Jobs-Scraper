"""
Google Job Search Web Scraper

This script performs web scraping on Google job search results to extract job listings and related data.
It allows users to specify a job term to search for and the maximum number of times to scroll for additional job listings.
The extracted data includes job titles, employers, job descriptions, and highlights.
Keyword frequencies are calculated from job descriptions and highlights, and the results are saved in JSON files.

Dependencies:
- argparse
- asyncio
- collections
- datetime
- json
- nltk
- pathlib
- urllib.parse
- playwright.async_api
- tqdm

Usage:
1. Run this script from the command line with the job term to search for as a required argument.
2. Optionally, specify the maximum scrolling count for additional job listings.

The extracted data is saved in JSON files in an 'output' directory, and keyword frequencies are saved in separate JSON files.

For more details on the functionality and how to use this script, please refer to the code comments.
"""


import argparse
import asyncio
import collections
import datetime
import json
import logging
from pathlib import Path
from urllib.parse import quote

import nltk
from nltk.tokenize import word_tokenize
from playwright.async_api import Locator, Page, Playwright, async_playwright
from tqdm import tqdm

from keyword_const import COMPUTER_SCIENCE_KEYS

TIMEOUT = 2000
SLEEP_TIME = 2

nltk.download("stopwords")
nltk.download("punkt")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

data = []

# TODO: write unit tests


def word_list_to_freq_dict(wordlist: list[str]) -> dict[str, int]:
    """The function takes a list of words and returns a dictionary where the keys are the words and the
    values are the frequencies of those words in the list.

    Parameters
    ----------
    wordlist : list[str]
        A list of strings representing words.

    Returns
    -------
        a dictionary where the keys are the words from the input wordlist and the values are the
    frequencies of those words in the wordlist.

    """
    wordfreq = [wordlist.count(p) for p in wordlist]
    return dict(list(zip(wordlist, wordfreq)))


def remove_stopwords(tokens: list[str]) -> list[str]:
    """The function `remove_stopwords` takes a list of tokens and removes any tokens that are stopwords in
    the English language.

    Parameters
    ----------
    tokens : list[str]
        A list of strings representing the tokens or words from a text.

    Returns
    -------
        The function `remove_stopwords` returns a list of tokens (words) from the input list `tokens`,
    excluding any stopwords (common words that are typically removed in natural language processing
    tasks).

    """
    stopwords = nltk.corpus.stopwords.words("english")
    return [token for token in tokens if token not in stopwords]


def remove_single_char(text: str, threshold: int = 2) -> str:
    """The function `remove_single_char` removes single-character words from a given text, with an optional
    threshold parameter to specify the minimum word length to keep.

    Parameters
    ----------
    text : str
        The `text` parameter is a string that represents the input text that you want to process and remove
    single-character words from.
    threshold : int, optional
        The threshold parameter is an optional integer that specifies the minimum length of a word for it
    to be included in the output. By default, the threshold is set to 2, meaning that any word with a
    length less than or equal to 2 will be excluded from the output.

    Returns
    -------
        a string.

    """
    return " ".join([word for word in word_tokenize(text) if len(word) > threshold])


def strip_non_computer_word(tokens: list[str]) -> list[str]:
    """The function `strip_non_computer_word` takes a list of tokens and returns a new list containing only
    the tokens that are computer science related.

    Parameters
    ----------
    tokens : list[str]
        A list of strings representing words or tokens.

    Returns
    -------
        a list of tokens that are computer science-related words.

    """
    return [token for token in tokens if token in COMPUTER_SCIENCE_KEYS]


def process_keyword(json_file_path: str, summary_file_path: str) -> None:
    """The `process_keyword` function calculates the frequency of keywords in a JSON file and saves the
    results in a summary file.

    Parameters
    ----------
    json_file_path : str
        The `json_file_path` parameter is the file path to the JSON file that contains the data to be
    processed. This file should be in UTF-8 encoding.
    summary_file_path : str
        The `summary_file_path` parameter is the file path where the summary of keyword frequencies will be
    saved. It should be a string representing the file path, including the file name and extension. For
    example, "summary.json" or "path/to/summary.json".

    """
    logger.debug("Calculating Keywords...")
    rows = []
    with open(json_file_path, "r", encoding="utf-8") as json_file:
        _data = json.load(json_file)
        for job in _data:
            wordstring = " ".join(
                [job.get("job_description").lower(), job.get("job_highlights").lower()]
            )
            wordstring = remove_single_char(wordstring)
            wordlist = wordstring.split()
            wordlist = remove_stopwords(wordlist)
            wordlist = strip_non_computer_word(wordlist)
            rows.extend(wordlist)

    word_freq = dict(collections.Counter(rows))
    sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    word_freq = dict(sorted_by_freq)

    with open(summary_file_path, "w", encoding="utf-8") as outfile:
        json.dump(word_freq, outfile, indent=4)


def save_data(json_file_path: str) -> None:
    """The function `save_data` saves data to a JSON file.

    Parameters
    ----------
    json_file_path : str
        The `json_file_path` parameter is a string that represents the file path where the JSON data will
    be saved.

    """
    with open(json_file_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)


def clean_data(data_unclean: str or list) -> str:
    """The `clean_data` function takes in either a string or a list of strings and returns a cleaned
    version of the input data by removing extra whitespace and joining the strings with a "|" separator
    if the input is a list.

    Parameters
    ----------
    data_unclean : str or list
        The `data_unclean` parameter can be either a string or a list.

    Returns
    -------
        The function `clean_data` returns a cleaned version of the input data. If the input `data_unclean`
    is a string, the function returns the string with extra whitespace removed. If the input
    `data_unclean` is a list, the function returns a string where each element of the list is cleaned
    and separated by a "|" character.

    """
    if isinstance(data_unclean, str):
        return " ".join(data_unclean.split()).strip()

    data_unclean = [" ".join(i.split()).strip() for i in data_unclean]
    return " | ".join(data_unclean)


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


async def parse_listing_page(
    page: Page,
) -> None:
    """The `parse_listing_page` function parses a job listing page by extracting data from each job
    element.

    Parameters
    ----------
    page : Page
        The `page` parameter is an instance of the `Page` class. It represents the web page that you want
    to parse.

    """
    xpath_jobs_tabs = "//div[@class='gws-plugins-horizon-jobs__tl-lif']"
    await page.wait_for_selector(xpath_jobs_tabs)
    jobs = page.locator(xpath_jobs_tabs)
    jobs_count = await jobs.count()
    logger.debug(f"Parsing `{jobs_count}` jobs")

    xpath_job_detail = "//div[@id='gws-plugins-horizon-jobs__job_details_page']"
    job_details = page.locator(xpath_job_detail)
    for i in range(jobs_count):
        job_element: Locator = job_details.nth(i)
        await extract_data(
            job_element,
            # i,
            # json_file_path=json_file_path,
            # summary_file_path=summary_file_path,
        )


async def run(
    playwright: Playwright,
    max_scroll: int,
    query: str,
    json_file_path: str,
    summary_file_path: str,
) -> None:
    """The `run` function launches a Firefox browser, navigates to a Google search page, scrolls down to
    load more job listings, parses the listings, saves the data to a JSON file, and processes the
    keyword for a summary file.

    Parameters
    ----------
    playwright : Playwright
        The `playwright` parameter is an instance of the Playwright class, which is used to launch and
    control the browser.
    max_scroll : int
        The `max_scroll` parameter is an integer that determines the maximum number of times the page will
    be scrolled down to load more job listings.
    query : str
        The `query` parameter is a string that represents the search query to be used on Google. It is used
    to search for job listings related to the query.
    json_file_path : str
        The `json_file_path` parameter is the file path where the parsed data will be saved in JSON format.
    summary_file_path : str
        The `summary_file_path` parameter is a string that represents the file path where the summary data
    will be saved. This file will contain the summarized information extracted from the job listings.

    """
    browser = await playwright.firefox.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()

    url = f"https://www.google.com/search?hl=en&q={quote(query)}&ibp=htl;jobs"
    await page.goto(url, wait_until="domcontentloaded")

    await page.wait_for_timeout(TIMEOUT)
    await asyncio.sleep(5)

    job_tree = page.locator("//div[@role='tree']")
    await job_tree.click()
    previous_y_bound = 0
    for _ in tqdm(range(max_scroll), desc="Scroll"):
        await page.mouse.wheel(0, 5000)
        await asyncio.sleep(SLEEP_TIME)
        box3 = await job_tree.bounding_box()
        if previous_y_bound == box3["y"]:
            break
        previous_y_bound = box3["y"]

    await parse_listing_page(page)  # json_file_path, summary_file_path)
    logger.debug("Finished parsing page")
    save_data(json_file_path)
    process_keyword(json_file_path, summary_file_path)
    # await page.screenshot(path=f"screenshot{dt}.png")
    await context.close()
    await browser.close()


def parse_args() -> argparse.Namespace:
    """The `parse_args` function is used to parse command line arguments, specifically the job term to
    search and the maximum scroll count.

    Returns
    -------
        The function `parse_args()` returns an `argparse.Namespace` object.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("term", help="Job term to search")
    parser.add_argument(
        "--max_scroll", default=100, type=int, help="Maximum scroll count"
    )

    return parser.parse_args()


async def main() -> None:
    """The `main` function is an asynchronous function that performs a web scraping task using Playwright
    to extract data from Google Jobs based on user-defined parameters.
    """
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    args = parse_args()
    query = str(args.term)
    max_scroll = args.max_scroll

    # Define the output directory for saving files
    output_dir = Path.cwd().joinpath("output")
    output_dir.mkdir(exist_ok=True)
    json_file_path = str(
        output_dir.joinpath(
            f"google_jobs_{args.term}_maxscroll_{args.max_scroll}_{dt}.json"
        )
    )
    summary_file_path = str(
        output_dir.joinpath(
            f"keywords_{args.term}_maxscroll_{args.max_scroll}_{dt}.json"
        )
    )
    logger.debug(f"Writing data into `{json_file_path}`")

    async with async_playwright() as playwright:
        await run(playwright, max_scroll, query, json_file_path, summary_file_path)


if __name__ == "__main__":
    asyncio.run(main())
