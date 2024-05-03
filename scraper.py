from collections import Counter
import re
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag

visited_urls = set()
word_counts = {}
longest_page_url = ""
longest_page_word_count = 0

refresh_count = 0
php_blacklist = Counter()
count_blacklist = Counter()
# Default English stopwords list
stop_words = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", 
    "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", 
    "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", 
    "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", 
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", 
    "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", 
    "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", 
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", 
    "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", 
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", 
    "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", 
    "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", 
    "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", 
    "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", 
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", 
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", 
    "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
])




    
def can_fetch_from_robots_txt(url, user_agent='*'):
    # Define the base domains
    base_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
    robots_parsers = {domain: RobotFileParser(urljoin(f"http://{domain}", '/robots.txt')) for domain in base_domains}
    for parser in robots_parsers.values():
        parser.read()

    rp = RobotFileParser()
    rp.set_url(urljoin(url, '/robots.txt'))
    rp.read()
    return rp.can_fetch(user_agent, url)

def scraper(url, resp, unique_pages, w_counts, longest_url, longest_count):
    global longest_page_word_count, longest_page_url, visited_urls, word_counts
    if len(visited_urls) == 0:
        visited_urls = unique_pages
    if len(word_counts) == 0:
        word_counts = w_counts
    if longest_page_url == "":
        longest_page_url = longest_url
    if longest_page_word_count == 0:
        longest_page_word_count = longest_count

    # Check robots.txt for the specific domains
    if not can_fetch_from_robots_txt(url):
        print(f"Access to {url} is disallowed by robots.txt.")
        return []
    links = extract_next_links(url, resp)
    l = [link for link in links if is_valid(link)]
    # print(a)
    visited_urls.update(l)
    return l

def extract_next_links(url, resp):
    global longest_page_word_count, longest_page_url, visited_urls, word_counts, refresh_count

    links = []
    if resp.status == 200:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')


        text_content = soup.get_text().lower()
        words = [word for word in re.findall(r"\b[a-zA-Z]{2,}\b", text_content) if word not in stop_words and not word.isdigit()]
        
        # If the current page has more words than the previous longest, update
        if len(words) > longest_page_word_count:
            longest_page_word_count = len(words)
            longest_page_url = url

        for word in words:
            if word not in word_counts:
                word_counts[word] = 1
            else:
                word_counts[word] += 1

        if refresh_count == 50:
            word_counts = dict(sorted(word_counts.items(), key=lambda item: item[1], reverse=True))
            # Print the top 50 elements
            # word_counts = dict(list(sorted_data.items()))
            # top_50_elements = whole_elements[:50]
            refresh_count = 0
        else:
            refresh_count += 1

        for anchor in soup.find_all('a', href=True):
            abs_url, _ = urldefrag(urljoin(url, anchor['href']))
            links.append(abs_url)

    return links

def is_valid(url):
    global longest_page_word_count, longest_page_url, visited_urls, php_blacklist

    try:
        parsed = urlparse(url)
        # Only allow certain subdomains from 'uci.edu'
        allowed_subdomains = ["ics", "cs", "informatics", "stat"]
        allowed_domains = ["uci.edu"]

        # Split the netloc into parts
        netloc_parts = parsed.netloc.split('.')

        # Ensure the netloc has at least two parts for domain and TLD
        if len(netloc_parts) < 2:
            return False

        # Create a domain string from the last two parts of netloc
        domain = ".".join(netloc_parts[-2:])

        # Check if the domain is in the allowed domains list
        if domain in allowed_domains:
            # Check if the subdomain is allowed if it exists
            if len(netloc_parts) > 2 and netloc_parts[-3] not in allowed_subdomains:
                return False
        else:
            return False

        # Check for repeating directory patterns
        if is_repeating_path(parsed.path):
            return False          

        # path is too long, possibly a trap
        if len(parsed.path.split("/")) > 5:
            return False
        
        # date is possibly a trap
        date_pattern = r'\d{4}-\d{2}'
        if re.search(date_pattern, url):
            return False
        
        if url in visited_urls:
            return False
        
        if parsed.query.count("%") >= 3 or parsed.query.count("=") >= 3 or parsed.query.count("&") >= 3:
            return False
        
        if parsed.scheme not in set(["http", "https"]):
            return False


        disallowed_extensions = {
            "css", "js", "bmp", "gif", "jpeg", "jpg", "ico", "png", "tiff", "tif", "mid", 
            "mp2", "mp3", "mp4", "wav", "avi", "mov", "mpeg", "ram", "m4v", "mkv", "ogg", 
            "ogv", "pdf", "ps", "eps", "tex", "ppt", "pptx", "doc", "docx", "xls", "xlsx", 
            "names", "data", "dat", "exe", "bz2", "tar", "msi", "bin", "7z", "psd", "dmg", 
            "iso", "epub", "dll", "cnf", "tgz", "sha1", "thmx", "mso", "arff", "rtf", "jar", 
            "csv", "rm", "smil", "wmv", "swf", "wma", "zip", "rar", "gz", "img", "mpg", "ppsx", "apk", "war"
        }

        # Split the path and get the last part, then check if the extension is in the disallowed set
        path_parts = url.lower().split('/')
        file_extension = path_parts[-1].split('.')[-1]
        if file_extension in disallowed_extensions:
            return False

        # if a url with .php, it's possible to be a trap
        php_url = url.strip().split(".php")[0] + ".php"
        if php_blacklist[php_url] > 10:
            return False
        else:
            php_blacklist[php_url] += 1

        # if a url's path appears too much time, it's possible to be a trap
        if count_blacklist[parsed.netloc + parsed.path] > 10:
            return False
        else:
            count_blacklist[parsed.netloc + parsed.path] += 1

        return True
    except Exception as e:
        print(f"An exception occurred for {url}: {e}")
        return False

def is_repeating_path(path):
    segments = path.strip("/").split('/')
    # Check for a repeating pattern where a segment is followed by itself
    for i in range(len(segments) - 1):
        if segments[i] == segments[i + 1]:
            return True
    # Use a dictionary to count occurrences of each segment
    segment_counts = {}
    for segment in segments:
        # print(segment)
        if segment not in segment_counts:
            segment_counts[segment] = 1
        else:
            segment_counts[segment] += 1
            # If a segment occurs more than 3 times, it's likely a trap
            if segment_counts[segment] >= 3:
                return True
    return False



