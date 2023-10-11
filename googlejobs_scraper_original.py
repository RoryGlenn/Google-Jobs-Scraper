import argparse
import asyncio
import collections
import datetime
import json
from pathlib import Path
from urllib.parse import quote

from playwright.async_api import Locator, Page, Playwright, async_playwright
from tqdm import tqdm

from keyword_const import US_CITIES, COMPUTER_SCIENCE_TERMS

output_dir = Path.cwd().joinpath("output")
output_dir.mkdir(exist_ok=True)
dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
json_file_path = str(output_dir.joinpath(f"google_jobs_data{dt}.json"))
summary_file_path = str(output_dir.joinpath(f"keywords_data{dt}.json"))
print(f"write into {json_file_path}")

data = []


def strip_non_computer_word(tokens: []):
    return [token for token in tokens if token in COMPUTER_SCIENCE_TERMS]


def process_keyword():
    print("calculate keywords")
    rows = []
    with open(json_file_path, "r", encoding="utf-8") as json_file:
        _data = json.load(json_file)
        for job in _data:
            word_string = " ".join(
                [job.get("job_description").lower(), job.get("job_highlights").lower()]
            )
            word_list = word_string.split()
            word_list = strip_non_computer_word(word_list)
            rows.extend(set(word_list))

    word_freq = dict(collections.Counter(rows))
    sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    word_freq = dict(sorted_by_freq)

    with open(f"{summary_file_path}", "w", encoding="utf-8") as outfile:
        json.dump(word_freq, outfile, indent=4)


def save_data():
    with open(json_file_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)


def clean_data(dirty_data: str or list) -> str:
    if isinstance(dirty_data, str):
        dirty_data = " ".join(dirty_data.split()).strip()
        return dirty_data

    dirty_data = [" ".join(i.split()).strip() for i in dirty_data]
    dirty_data = " | ".join(dirty_data)
    return dirty_data


async def extract_data(job_element: Locator) -> None:
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
    xpath_jobs_tabs = "//div[@class='gws-plugins-horizon-jobs__tl-lif']"
    await page.wait_for_selector(xpath_jobs_tabs)
    jobs = page.locator(xpath_jobs_tabs)
    jobs_count = await jobs.count()
    print(f"Parse {jobs_count} jobs")

    xpath_job_detail = "//div[@id='gws-plugins-horizon-jobs__job_details_page']"
    job_details = page.locator(xpath_job_detail)
    for i in range(jobs_count):
        job_element: Locator = job_details.nth(i)
        await extract_data(job_element)


async def run(playwright: Playwright, max_scroll: int, query: str) -> None:
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
    await context.close()
    await browser.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("term", help="Job term to search")
    parser.add_argument(
        "--max_scroll", default=100, type=int, help="Maximum scrolling count"
    )

    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    for city in US_CITIES:
        async with async_playwright() as playwright:
            await run(
                playwright,
                max_scroll=args.max_scroll,
                query=f"{args.term} in {city}",
            )


if __name__ == "__main__":
    asyncio.run(main())
