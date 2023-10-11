import argparse
import asyncio
import collections
import datetime
import json
import logging
import os
from pprint import pprint
from urllib.parse import quote
from dataclasses import dataclass
from typing import Any

from playwright.async_api import Locator, Page, Playwright, async_playwright
from tqdm import tqdm

from keyword_const import US_CITIES, COMPUTER_SCIENCE_TERMS
from timing import TimeIt

TIMEOUT = 2000
SLEEP_TIME = 2

# * TODO: count the number of job titles and the keywords associated with each job title


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


def save_data(data: Any, file_path: str) -> None:
    with open(file_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)


# Count dictionaries in the master dictionary
def count_dictionaries(d):
    return sum(len(lst) for key, lst in d.items())


def count_strings(strings: list[str]) -> dict[str, int]:
    """
    Count the occurrences of each string in a list and create a dictionary.

    Args:
        strings (list): List of strings.

    Returns:
        dict: Dictionary where the keys are the strings and the values are the counts.
    """
    counts = {}
    for s in strings:
        if s in counts:
            counts[s] += 1
        else:
            counts[s] = 1
    return counts


def aggregate_keywords(keywords_data) -> None:
    unique_keywords_data = remove_duplicates(keywords_data)
    rows = []
    for query, lst in unique_keywords_data.items():
        for job in lst:
            word_string = " ".join(
                [job.get("job_description").lower(), job.get("job_highlights").lower()]
            )
            word_list = word_string.split()
            word_list = strip_non_computer_word(word_list)

            rows.extend(list(set(word_list)))

    word_freq = dict(collections.Counter(rows))
    sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return dict(sorted_by_freq)


def remove_duplicates(data: dict[list[dict]]) -> dict[list[dict]]:
    unique_dicts = {}
    for query, job_list in data.items():
        for job in job_list:
            if job not in unique_dicts.get(query, []):
                if query not in unique_dicts:
                    unique_dicts[query] = [job]
                else:
                    unique_dicts[query].append(job)
    return unique_dicts


def strip_non_computer_word(tokens: list[str]) -> list[str]:
    return [token for token in tokens if token in COMPUTER_SCIENCE_TERMS]


def process_keyword(job_data: dict) -> None:
    logger.debug("Calculating Keywords")

    rows = []

    for job_list in job_data.values():
        for job in job_list:
            word_string = " ".join(
                [job["job_description"].lower(), job["job_highlights"].lower()]
            )
            word_list = word_string.split()
            word_list = strip_non_computer_word(word_list)

            rows.extend(list(set(word_list)))

    word_freq = dict(collections.Counter(rows))
    sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    word_freq = dict(sorted_by_freq)

    # with open(keywords_file_path, "w", encoding="utf-8") as outfile:
    #     json.dump(word_freq, outfile, indent=4)
    return word_freq


def clean_data(data_raw: str or list) -> str:
    if isinstance(data_raw, str):
        return " ".join(data_raw.split()).strip()

    data_raw = [" ".join(i.split()).strip() for i in data_raw]
    return " | ".join(data_raw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("term", help="Job term to search")
    parser.add_argument(
        "--max_scroll", default=100, type=int, help="Maximum scroll count"
    )

    return parser.parse_args()


def aggregate_job_descriptions(job_data: list[dict]) -> None:
    unique_job_data = remove_duplicates(job_data)
    res = []
    for job_list in unique_job_data.values():
        res.extend(iter(job_list))
    return res


@dataclass(frozen=True)
class XPaths:
    title = "//h2[@class='KLsYvd']"
    job_description_span = "//span[@class='HBvzbc']"
    job_highlights = "//div[@class='JxVj3d']"
    employer = 'div[class*="nJlQNd"]'
    jobs_tabs = "//div[@class='gws-plugins-horizon-jobs__tl-lif']"
    job_detail = "//div[@id='gws-plugins-horizon-jobs__job_details_page']"


class GoogleJobsScraper:
    def __init__(self):
        self.query = None
        self.job_data = {}
        self.all_job_data = []

        self.job_titles = {}
        self.xpaths = XPaths()

        self.max_scroll = None
        self.jobs_file_path_list = []
        self.keywords_file_path_list = []
        self.total_num_jobs = 0

        self.jobs_file_path = None
        self.keywords_file_path = None

    async def extract_data(self, job_element: Locator) -> None:
        title = await job_element.locator(self.xpaths.title).inner_text()

        logger.debug(
            f"Extracting data for {count_dictionaries(self.job_data)}: {title}"
        )

        employer = await job_element.locator(self.xpaths.employer).inner_text()
        job_description = await job_element.locator(
            self.xpaths.job_description_span
        ).all_inner_texts()

        highlights_elements = job_element.locator(self.xpaths.job_highlights)
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

        if self.job_data.get(self.query):
            self.job_data[self.query].append(data_to_save)
        else:
            self.job_data[self.query] = [data_to_save]

    async def parse_listing_page(
        self,
        page: Page,
    ) -> None:
        await page.wait_for_selector(self.xpaths.jobs_tabs)
        jobs = page.locator(self.xpaths.jobs_tabs)
        jobs_count = await jobs.count()
        logger.debug(f"Parsing {jobs_count} jobs")

        job_details = page.locator(self.xpaths.job_detail)
        for i in range(jobs_count):
            job_element = job_details.nth(i)
            await self.extract_data(job_element)

    async def run_scraper(
        self,
        playwright: Playwright,
    ) -> None:
        browser = await playwright.firefox.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        url = f"https://www.google.com/search?hl=en&q={quote(self.query)}&ibp=htl;jobs"
        await page.goto(url, wait_until="domcontentloaded")

        await page.wait_for_timeout(timeout=TIMEOUT)
        await asyncio.sleep(delay=SLEEP_TIME)

        job_tree = page.locator("//div[@role='tree']")
        await job_tree.click()
        previous_y_bound = 0
        for _ in tqdm(range(self.max_scroll), desc="Scroll"):
            await page.mouse.wheel(delta_x=0, delta_y=5000)
            await asyncio.sleep(SLEEP_TIME)
            box3 = await job_tree.bounding_box()
            if previous_y_bound == box3["y"]:
                break
            previous_y_bound = box3["y"]

        await self.parse_listing_page(page)
        await context.close()
        await browser.close()

        processed_keywords = process_keyword(self.job_data)

        # save the data
        save_data(processed_keywords, self.keywords_file_path)
        save_data(self.job_data, self.jobs_file_path)

        self.jobs_file_path_list.append(self.jobs_file_path)
        self.keywords_file_path_list.append(self.keywords_file_path)

        self.all_job_data.append(self.job_data)
        self.job_data = {}  # ! reset the data list for the next search


async def main() -> None:
    scraper = GoogleJobsScraper()
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    args = parse_args()

    # Define the output directory for saving files
    results_dir = os.path.join(os.getcwd(), "results")
    os.makedirs(results_dir, exist_ok=True)

    # create the output subdirectory for the current run
    output_name = f"{args.term} {dt}"
    output_dir = os.path.join(results_dir, output_name)
    os.makedirs(output_dir, exist_ok=True)

    # for city in CITIES:
    for city in [
        "New York City",
        "San Francisco",
    ]:
        scraper.query = f"{args.term} {city}"
        # create the json file path for the current run
        scraper.jobs_file_path = os.path.join(output_dir, f"jobs {scraper.query}.json")
        scraper.keywords_file_path = os.path.join(
            output_dir, f"keywords {scraper.query}.json"
        )
        scraper.max_scroll = args.max_scroll

        # start the browser and run the search
        with TimeIt(f"Running search for `{scraper.query}`"):
            async with async_playwright() as playwright:
                await scraper.run_scraper(playwright)

    # jobs_agg_file_path = os.path.join(output_dir, "jobs_agg.json")
    # keywords_agg_file_path = os.path.join(output_dir, "keywords_agg.json")

    # save_data(aggregate_job_descriptions(scraper.job_data), jobs_agg_file_path)
    # save_data(aggregate_keywords(scraper.job_data), keywords_agg_file_path)

    # aggregate all job titles found
    job_title_list = []
    for job_list in scraper.all_job_data:
        for query, lst_of_dicts in job_list.items():
            job_title_list.extend(job["title"] for job in lst_of_dicts)

    title_map = count_strings(job_title_list)
    title_map = sorted(title_map.items(), key=lambda x: x[1], reverse=True)
    pprint(title_map)


if __name__ == "__main__":
    asyncio.run(main())
