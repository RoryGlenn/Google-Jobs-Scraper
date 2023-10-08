"""parse_data.py is meant for parsing raw data that has already been scraped using the googlejobs_scraper_original.py script."""


import collections
import itertools
import json
import logging
from pprint import pformat, pprint
from unittest import result

from tqdm import tqdm

from keyword_const import COMPUTER_SCIENCE_TERMS

logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def strip_non_computer_word(tokens):
    return [token for token in tokens if token in COMPUTER_SCIENCE_TERMS]


def occurrence(data, col_name):
    result = []
    for job_dict in data:
        column_data = job_dict[col_name]
        # column_data = column_data.strip().lower().replace(" ", "_")
        column_data = column_data.strip().lower()
        result.append(column_data)

    title_freq = dict(collections.Counter(result))
    return sorted(title_freq.items(), key=lambda x: x[1], reverse=True)


def get_keywords(data):
    logger.info("calculate keywords")
    rows = []

    for job in tqdm(data, desc="Parsing Keywords"):
        word_string = " ".join(
            [job.get("job_description").lower(), job.get("job_highlights").lower()]
        )
        word_list = word_string.split()
        word_list = strip_non_computer_word(word_list)
        rows.extend(set(word_list))

    word_freq = dict(collections.Counter(rows))
    sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return dict(sorted_by_freq)


def _get_keywords(job_dict):
    word_string = " ".join(
        [
            job_dict["job_description"].lower(),
            job_dict["job_highlights"].lower(),
        ]
    )
    word_list = word_string.split()
    return strip_non_computer_word(word_list)


def employer_keywords(data):
    pass


def job_title_keywords(data):
    # per job title, what are the top N number of skills associated with that title.

    def _get_job_title(job_dict) -> str:
        return job_dict["title"].strip().lower().replace(" ", "_")

    def _get_keywords(job_dict):
        word_string = " ".join(
            [
                job_dict["job_description"].lower(),
                job_dict["job_highlights"].lower(),
            ]
        )
        word_list = word_string.split()
        return strip_non_computer_word(word_list)

    result = {}

    for job_dict in tqdm(data, desc="Parsing Job Title Keywords"):
        title = _get_job_title(job_dict)
        keywords = _get_keywords(job_dict)

        # if the  title  key is already present, it will update the existing set with the new keywords.
        # If the  title  key is not present, it will create a new key-value pair with the
        # title  as the key and the  keywords  set as the value.
        result.setdefault(title, []).extend(keywords)

    # count the number of keyword occurrences
    for key, value in result.items():
        word_freq = dict(collections.Counter(value))
        sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        result[key] = sorted_by_freq

    return result


def employer_keywords(data):
    result = {}

    for job_dict in tqdm(data, desc="Parsing Employer Keywords"):
        employer_name = job_dict["employer"].strip().lower()
        keywords = _get_keywords(job_dict)
        result.setdefault(employer_name, []).extend(keywords)

    # count the number of keyword occurrences
    for key, value in result.items():
        word_freq = dict(collections.Counter(value))
        sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        result[key] = sorted_by_freq

    return result



def main(): 
    path = "results/Software Engineer in 2023-09-23/google_jobs_Software Engineer in All Cities_2023-09-23.json"
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    # job_titles = occurrence(data, "title")
    # jt_keywords = job_title_keywords(data)
    # result = {title: jt_keywords[title] for (title, count) in job_titles}
    # with open('All_Job_Title_Keyword_Occurrences.json', 'w', encoding='utf-8') as file:
    #     json.dump(result, file, indent=4)

    # employers = occurrence(data, "employer")
    # emp_keywords = employer_keywords(data)
    
    # result = {}
    # for (emp_name, count) in employers:
    #     result[emp_name] = emp_keywords[emp_name]
    
    # result = {emp_name: emp_keywords[emp_name] for (emp_name, count) in employers}
    
    # with open("All_Employer_Keyword_Occurrences.json", "w", encoding="utf-8") as file:
    #     json.dump(result, file, indent=4)


if __name__ == "__main__":
    main()
