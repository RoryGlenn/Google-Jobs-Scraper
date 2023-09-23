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
from pathlib import Path
from urllib.parse import quote

import nltk
from nltk.tokenize import word_tokenize
from playwright.async_api import Locator, Page, Playwright, async_playwright
from tqdm import tqdm

from keyword_const import COMPUTER_SCIENCE_KEYS

nltk.download("stopwords")
nltk.download("punkt")

# Define the output directory for saving files
# output_dir = Path.cwd().joinpath("output")
# output_dir.mkdir(exist_ok=True)
# dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
# json_file_path = str(output_dir.joinpath(f"google_jobs_data{dt}.json"))
# summary_file_path = str(output_dir.joinpath(f"keywords_data{dt}.json"))
# print(f"write into {json_file_path}")

dt = datetime.datetime.now().strftime("%Y%m%d%H%M")
json_file_path = None
summary_file_path = None
data = []


def word_list_to_freq_dict(wordlist):
    """
    Convert a list of words into a frequency dictionary.

    Args:
        wordlist (list): List of words.

    Returns:
        dict: A dictionary where keys are words, and values are their frequencies.
    """
    wordfreq = [wordlist.count(p) for p in wordlist]
    return dict(list(zip(wordlist, wordfreq)))


def remove_stopwords(tokens):
    """
    Remove stopwords from a list of tokens.

    Args:
        tokens (list): List of tokens (words).

    Returns:
        list: List of tokens with stopwords removed.
    """
    stopwords = nltk.corpus.stopwords.words("english")
    return [token for token in tokens if token not in stopwords]


def remove_single_char(text, threshold=2):
    """
    Remove single-character words from a text.

    Args:
        text (str): Input text.
        threshold (int): Minimum word length to retain. Default is 2.

    Returns:
        str: Text with single-character words removed.
    """
    threshold = threshold
    words = word_tokenize(text)
    return " ".join([word for word in words if len(word) > threshold])


def strip_non_computer_word(tokens: []):
    """
    Filter a list of tokens to retain only those related to computer science.

    Args:
        tokens (list): List of tokens (words).

    Returns:
        list: List of tokens containing only computer science-related words.
    """
    return [token for token in tokens if token in COMPUTER_SCIENCE_KEYS]


def process_keyword():
    """
    Process keyword frequencies from job descriptions and highlights in the data.
    Saves the keyword frequencies to a JSON file.
    """
    print("calculate keywords")
    rows = []
    with open(json_file_path) as json_file:
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

    with open(f"{summary_file_path}", "w") as outfile:
        json.dump(word_freq, outfile, indent=4)


def save_data():
    """
    Save the extracted job data to a JSON file.
    """
    with open(json_file_path, "w") as outfile:
        json.dump(data, outfile, indent=4)


def clean_data(data: str or list) -> str:
    """
    Clean and format data for consistency.

    Args:
        data (str or list): Input data, which can be a string or a list of strings.

    Returns:
        str: Cleaned and formatted string data.
    """
    if isinstance(data, str):
        data = " ".join(data.split()).strip()
        return data

    data = [" ".join(i.split()).strip() for i in data]
    data = " | ".join(data)
    return data


async def extract_data(job_element: Locator, row_number: int) -> None:
    """
    Extract job data (title, employer, job description, and highlights) from a job element on the page.

    Args:
        job_element (Locator): Job element on the page.
        row_number (int): The row number of the job element in the list.
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
    """
    Parse job listing page to extract job data.

    Args:
        page (Page): The web page to parse.
    """
    xpath_jobs_tabs = "//div[@class='gws-plugins-horizon-jobs__tl-lif']"
    await page.wait_for_selector(xpath_jobs_tabs)
    jobs = page.locator(xpath_jobs_tabs)
    jobs_count = await jobs.count()
    print(f"Parse {jobs_count} jobs")

    xpath_job_detail = "//div[@id='gws-plugins-horizon-jobs__job_details_page']"
    job_details = page.locator(xpath_job_detail)
    for i in range(jobs_count):
        job_element: Locator = job_details.nth(i)
        await extract_data(job_element, i)


async def run(playwright: Playwright, max_scroll: int, query: str) -> None:
    """Initialize the Playwright browser, create a new page, and navigate to Google job search results.

    Args:
        playwright (Playwright): The Playwright instance.
        max_scroll (int): Maximum number of times to scroll for additional job listings.
        query (str): The job term to search for.
    """
    # Initializing browser and opening a new page
    browser = await playwright.firefox.launch(headless=False)
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
    print("finished parsing page")
    save_data()
    process_keyword()
    await page.screenshot(path=f"screenshot{dt}.png")
    await context.close()
    await browser.close()


def parse_args():
    """Parse command-line arguments and return the parsed arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("term", help="Job term to search")
    parser.add_argument(
        "--max_scroll", default=100, type=int, help="Maximum scrolling count"
    )

    return parser.parse_args()


async def main() -> None:
    """Main entry point of the script. Parses command-line arguments and initiates web scraping.

    Raises:
        Exception: Any exceptions raised during the execution.
    """
    args = parse_args()
    query = f"{args.term}"
    max_scroll = args.max_scroll

    # Define the output directory for saving files
    output_dir = Path.cwd().joinpath("output")
    output_dir.mkdir(exist_ok=True)
    global json_file_path
    json_file_path = str(
        output_dir.joinpath(
            f"google_jobs_{args.term}_maxscroll_{args.max_scroll}_{dt}.json"
        )
    )
    global summary_file_path
    summary_file_path = str(
        output_dir.joinpath(
            f"keywords_{args.term}_maxscroll_{args.max_scroll}_{dt}.json"
        )
    )
    print(f"write into {json_file_path}")

    async with async_playwright() as playwright:
        await run(playwright, max_scroll=max_scroll, query=query)


if __name__ == "__main__":
    asyncio.run(main())
