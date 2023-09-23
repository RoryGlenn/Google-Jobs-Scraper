# Google Jobs Scraper using Playwright Python

Step 1: cd into the playwright_python directory.

Step 2: Create virtual environment

    python -m venv venv
    
    .\venv\scripts\Activate.ps1

Step 3: Install the requirements.txt using

     pip install -r requirements.txt

Step 4: Install the necessary browsers required for playwright

    playwright install firefox

Step 5: Run the scraper code using

     python .\googlejobs_scraper.py "Backend engineer" "Singapore"

- Modify max scroll:

    python .\googlejobs_scraper.py "Backend engineer" "Singapore" --max_scroll=200