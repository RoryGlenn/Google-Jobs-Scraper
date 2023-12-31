"""parse_data.py is meant for parsing raw data that has already been scraped using the google_jobs_scraper.py script."""

import collections
import json
import logging

from tqdm import tqdm
from keyword_const import COMPUTER_SCIENCE_TERMS

logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_raw_data(path: str) -> list[dict]:
    """The function `get_raw_data` reads and returns the contents of a JSON file as a list of dictionaries.

    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path of the JSON file that you want to
    read.

    Returns
    -------
        a list of dictionaries.

    """
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def write_data(data: list or dict, path: str) -> None:
    """The `write_data` function writes a list or dictionary of data to a file in JSON format with
    indentation.

    Parameters
    ----------
    data : list or dict
        The `data` parameter can be either a list or a dictionary. It represents the data that you want to
    write to a file.
    path : str
        The `path` parameter is a string that represents the file path where the data will be written to.
    It should include the file name and extension. For example, if you want to write the data to a file
    named "data.json" in the current directory, you can pass the path as "

    """
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def strip_non_computer_word(tokens: list[str]):
    """
    Filters out non-computer science terms from a list of tokens.

    Args:
        tokens (list): A list of tokens to filter.

    Returns:
        list: A list of tokens that are computer science terms.
    """
    return [token for token in tokens if token in COMPUTER_SCIENCE_TERMS]


def get_occurrences(data: list[dict], col_name: str) -> list:
    """
    Returns a list of tuples containing the frequency of occurrence of each unique value in the specified column of the input data.

    Args:
        data (list): A list of dictionaries representing job data.
        col_name (str): The name of the column to analyze.

    Returns:
        list: A list of tuples, where each tuple contains a unique value from the specified column and its frequency of occurrence in the input data.

    """
    result = []
    for job_dict in data:
        column_data = job_dict[col_name]
        column_data = column_data.strip().lower()
        result.append(column_data)

    title_freq = dict(collections.Counter(result))
    return sorted(title_freq.items(), key=lambda x: x[1], reverse=True)


def get_keywords(job_dict: dict) -> list:
    """
    Extracts keywords from the job description and highlights of a job listing.

    Args:
        job_dict (dict): A dictionary containing job listing information.

    Returns:
        list: A list of keywords extracted from the job description and highlights.
    """
    word_string = " ".join(
        [
            job_dict["job_description"].lower(),
            job_dict["job_highlights"].lower(),
        ]
    )
    word_list = word_string.split()
    return strip_non_computer_word(word_list)


def job_title_keywords(data: list[dict]) -> dict[str, list[tuple]]:
    """
    Given a list of job dictionaries, returns a dictionary where each key is a job title and the value is a list of tuples
    containing the top N number of skills associated with that title, sorted by frequency of occurrence.

    Args:
        data (list[dict]): A list of job dictionaries containing job titles, descriptions, and highlights.

    Returns:
        dict[str, list[tuple]]: A dictionary where each key is a job title and the value is a list of tuples containing
        the top N number of skills associated with that title, sorted by frequency of occurrence.
    """

    def _get_job_title(job_dict) -> str:
        return job_dict["title"].strip().lower()

    result = {}

    for job_dict in tqdm(data, desc="Parsing Job Title Keywords"):
        title = _get_job_title(job_dict)
        keywords = get_keywords(job_dict)

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


def employer_keywords(data: list[dict]) -> dict:
    """
    Extracts the keywords from the job descriptions in the given data and groups them by employer.

    Args:
        data (list): A list of dictionaries, where each dictionary represents a job posting and contains
            the following keys: "title", "location", "employer", "description", "date_posted".

    Returns:
        dict: A dictionary where the keys are the names of the employers (lowercase and stripped of leading/trailing
            whitespace) and the values are lists of tuples, where each tuple contains a keyword and its frequency
            in the job descriptions of that employer. The lists are sorted in descending order of frequency.
    """
    result = {}

    for job_dict in tqdm(data, desc="Parsing Employer Keywords"):
        employer_name = job_dict["employer"].strip().lower()
        keywords = get_keywords(job_dict)
        result.setdefault(employer_name, []).extend(keywords)

    # count the number of keyword occurrences
    for key, value in result.items():
        word_freq = dict(collections.Counter(value))
        sorted_by_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        result[key] = sorted_by_freq

    return result


def all_job_title_occurrences(data: list[dict], path: str) -> None:
    """The function `all_job_title_occurrences` takes a list of dictionaries `data` and a string `path` as
    input, counts the occurrences of job titles in the `data` and writes the result to a file specified
    by `path`.

    Parameters
    ----------
    data : list[dict]
        The `data` parameter is a list of dictionaries. Each dictionary represents a job and contains
    information about the job, such as the job title, company, and location.
    path : str
        The `path` parameter is a string that represents the file path where the result will be written to.
    It should include the file name and extension. For example, if you want to write the result to a
    file named "job_titles.txt" in the current directory, you can set `path`

    """
    job_titles = get_occurrences(data, "title")
    result = {elem[0]: elem[1] for elem in job_titles}
    write_data(result, path)


def all_employer_keyword_occurrences(data: list[dict], path: str) -> None:
    """The function `all_employer_keyword_occurrences` extracts the occurrences of keywords for each
    employer from a given dataset, and writes the results to a specified file path.

    Parameters
    ----------
    data : list[dict]
        The `data` parameter is a list of dictionaries. Each dictionary represents a data entry and
    contains information about an employer. The dictionaries have various keys, but one of the keys is
    "employer" which contains the name of the employer.
    path : str
        The `path` parameter is a string that represents the file path where the result will be written to.
    It should include the file name and extension. For example, "output.txt" or "results.csv".

    """
    employers = get_occurrences(data, "employer")
    emp_keywords = employer_keywords(data)
    result = {emp_name: dict(emp_keywords[emp_name]) for (emp_name, count) in employers}
    write_data(result, path)


def all_job_title_keyword_occurrences(data: list[dict], path: str) -> None:
    """The function `all_job_title_keyword_occurrences` takes in a list of dictionaries representing job
    data and a file path, and it calculates the occurrences of keywords in job titles and writes the
    results to a file.

    Parameters
    ----------
    data : list[dict]
        The `data` parameter is a list of dictionaries. Each dictionary represents a job and contains
    information such as the job title, description, and other relevant details.
    path : str
        The `path` parameter is a string that represents the file path where the result will be written to.

    """
    job_titles = get_occurrences(data, "title")
    jt_keywords = job_title_keywords(data)
    result = {title: dict(jt_keywords[title]) for (title, count) in job_titles}

    write_data(result, path)


def all_employer_occurrences(data: list[dict], path: str) -> None:
    """The function `all_employer_occurrences` takes a list of dictionaries `data` and a string `path` as
    input, counts the occurrences of each employer in the data, and writes the result to a file
    specified by the `path`.

    Parameters
    ----------
    data : list[dict]
        The `data` parameter is a list of dictionaries. Each dictionary represents a data entry and
    contains information about an individual. The dictionaries have various keys, but one of the keys is
    "employer" which stores the name of the employer for that individual.
    path : str
        The `path` parameter is a string that represents the file path where the result will be written to.
    It should include the file name and extension. For example, if you want to write the result to a
    file named "output.txt" in the current directory, you can pass the value "./output

    """
    employers = get_occurrences(data, "employer")
    result = {elem[0]: elem[1] for elem in employers}

    write_data(result, path)


def write_all_occurrences(data: list[dict], path: str) -> None:
    # sourcery skip: use-fstring-for-concatenation
    """The function writes all occurrences of job titles, employer keywords, job title keywords, and
    employers from a given data list to a specified file path.

    Parameters
    ----------
    data : list[dict]
        A list of dictionaries representing job data. Each dictionary contains information about a job,
    such as job title, employer, and keywords.
    path : str
        The path parameter is a string that represents the file path where the occurrences will be written to.

    """

    all_job_title_occurrences(data, path + "/job_title_occurrences.json")
    all_employer_keyword_occurrences(data, path + "/employer_keyword_occurrences.json")
    all_job_title_keyword_occurrences(
        data, path + "/job_title_keyword_occurrences.json"
    )
    all_employer_occurrences(data, path + "/employer_occurrences.json")
