from collections import Counter
import os
import sys
from threading import Event, Thread, Timer

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
import re
from urllib.parse import urlparse, urljoin, urldefrag
import ast

import signal
import scraper

def print_output():
    outputs = [scraper.longest_page_url, scraper.longest_page_word_count, scraper.word_counts]
    unique_pages = set()
    subdomains = Counter()
    with open('Logs/Worker.log', 'r') as file:
        for line in file:
            # Extract the URL and the status
            match = re.search(r'Downloaded (\S+), status <(\d+)>', line)
            if match:
                url, status = match.groups()
                # Remove fragment and query to get the unique URL
                url = urldefrag(url)[0]
                # print(status)
                if status == "200":
                    unique_pages.add(url)
                # print(len(unique_pages))
                            # Process subdomains for ics.uci.edu
                    parsed_url = urlparse(url)
                    if parsed_url.netloc.endswith('.ics.uci.edu'):
                        subdomain = parsed_url.netloc
                        subdomains[subdomain] += 1
    with open("output.txt", 'a') as f:
        # Assuming outputs is properly defined and available here
        f.write(f"Unique pages: {len(unique_pages)}\n")
        f.write(f"Longest page so far: {outputs[0]} with word count: {outputs[1]}\n")
        f.write(f"Top 50 words: {outputs[2]}\n")
        for subdomain, count in sorted(subdomains.items()):
            f.write(f"{subdomain}, {count}\n")
# Define the signal handler
def handle_interrupt(signum, frame):
    print_output()
    print("Process paused. Data written to output.txt.")
    sys.exit(0)


signal.signal(signal.SIGINT, handle_interrupt)
class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        unique_pages = set()
        word_counts = {}
        longest_page_url = ""
        longest_page_word_count = 0
        # Create an Event object for timeout handling
        timeout_event = Event()
        # Read the log file
        # print("test:", os.getcwd())
        with open('Logs/Worker.log', 'r') as file:
            for line in file:
                # Extract the URL and the status
                match = re.search(r'Downloaded (\S+), status <(\d+)>', line)
                if match:
                    url, status = match.groups()
                    # Remove fragment and query to get the unique URL
                    url = urldefrag(url)[0]

                    unique_pages.add(url)
        # extract the info from output.txt
        try:
            with open('output.txt', 'r') as f:
                lines = f.readlines()
                # Find the last word_counts line
                for line in reversed(lines):
                    if line.startswith("Top 50 words:"):
                        # Use ast.literal_eval to safely evaluate the string as a Python literal
                        dict_str = line.replace("Top 50 words: ", "").strip()
                        word_counts = ast.literal_eval(dict_str)
                        break
                # Find the longest page line
                for line in reversed(lines):
                    if line.startswith("Longest page so far:"):
                        longest_page_url = line.split(' ')[4]
                        longest_page_word_count = int(line.strip().split(' ')[-1])
                        break
        except FileNotFoundError:
            # If the file does not exist, initialize to empty dict and zero
            word_counts = {}
            longest_page_word_count = 0
            longest_page_url = ""
        except ValueError as e:
            print(f"Error while initializing from file: {e}")
            # In case of an error during file reading, initialize to default values
            word_counts = {}
            longest_page_word_count = 0
            longest_page_url = ""
        
        try:
            while True:
                # Before each URL fetch, reset the timeout_event and start a Timer
                timeout_event.clear()
                timeout_timer = Timer(10, timeout_event.set)
                timeout_timer.start()

                try:
                    tbd_url = self.frontier.get_tbd_url()  # This method already includes the 500ms delay logic
                    if not tbd_url:
                        self.logger.info("Frontier is empty. Stopping Crawler.")
                        break
                    resp = download(tbd_url, self.config, self.logger)
                    self.logger.info(
                        f"Downloaded {tbd_url}, status <{resp.status}>, "
                        f"using cache {self.config.cache_server}.")
                    if resp.status == 200:
                        scraped_urls = scraper.scraper(tbd_url, resp, unique_pages, word_counts, longest_page_url, longest_page_word_count)
                    # print(scraped_urls)
                        for scraped_url in scraped_urls:
                            self.frontier.add_url(scraped_url)
                        self.frontier.mark_url_complete(tbd_url)
                except Exception as e:
                    self.logger.error(f"An exception occurred: {e}")  # Log the actual exception
                    continue  # Continue with the next iteration of the loop
                finally:
                    timeout_timer.cancel()    

                # Check if the timeout was reached
                if timeout_event.is_set():
                    self.logger.info(f"Timeout reached for URL {tbd_url}. Skipping.")
                    continue  # Skip this URL and continue with the next one
                time.sleep(self.config.time_delay)      
            print_output()   
        except Exception as e:
            print_output()


