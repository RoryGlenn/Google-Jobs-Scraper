from parse_data import get_raw_data, write_all_occurrences


def main() -> None:
    """The main function reads data from a JSON file, extracts the path, and writes all occurrences of the
    data to a file in the same directory.

    """
    path = "results/Software Engineer in 2023-10-11/google_jobs_data_us_cities.json"
    data = get_raw_data(path)

    path = path.split("/")
    path = "/".join(path[:-1])
    write_all_occurrences(data, path)


if __name__ == "__main__":
    main()
