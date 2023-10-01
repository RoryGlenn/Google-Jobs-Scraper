import json
from pprint import pprint
from keyword_const import COMPUTER_SCIENCE_TERMS
import collections
from tqdm import tqdm


def strip_non_computer_word(tokens):
    return [token for token in tokens if token in COMPUTER_SCIENCE_TERMS]


def process_keywords(data):
    print("calculate keywords")
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


def main():
    path = "results/Software Engineer in 2023-09-23/google_jobs_Software Engineer in All Cities_2023-09-23.json"
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    keywords = process_keywords(data)

    with open("AllKeywords.json", "w", encoding="utf-8") as file:
        json.dump(keywords, file, indent=4)



if __name__ == "__main__":
    main()
