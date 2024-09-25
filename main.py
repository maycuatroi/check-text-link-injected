import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

USER_AGENT_DESKTOP = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
HEADERS = {"User-Agent": USER_AGENT_DESKTOP}
TIMEOUT = 10
MAX_WORKERS = 20
OUTPUT_DIR = Path("output")
OK_DIR = OUTPUT_DIR / "ok"
NG_DIR = OUTPUT_DIR / "ng"
DATA_DIR = Path("data")
DETECTED_DOMAINS_FILE = OUTPUT_DIR / "detected_domains.txt"

def load_domains(csv_file: Path) -> list[str]:
    df = pd.read_csv(csv_file)
    return df["Domain"].unique().tolist()

def load_sensitive_keywords(keyword_file: Path) -> list[str]:
    with keyword_file.open("r", encoding="utf-8") as file:
        content = file.read()
    return [keyword.strip() for keyword in content.split(",")]

def create_output_dirs() -> None:
    OK_DIR.mkdir(parents=True, exist_ok=True)
    NG_DIR.mkdir(parents=True, exist_ok=True)

def process_domain(domain: str, sensitive_keywords: list[str]) -> tuple[str, bool]:
    url = f"https://{domain}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return domain, False

    soup = BeautifulSoup(response.content, "html.parser")
    rendered_text = soup.get_text(separator="\n", strip=True)
    output_folder = OK_DIR
    detected = False

    for keyword in sensitive_keywords:
        if keyword in rendered_text:
            lines = rendered_text.split("\n")
            for i, line in enumerate(lines):
                if keyword in line:
                    start = max(0, i - 3)
                    end = min(len(lines), i + 4)
                    context = "\n".join(lines[start:end])
                    logging.info(f"Sensitive keyword '{keyword}' found in {url}")
                    logging.info(f"Context:\n{context}")
                    break
            output_folder = NG_DIR
            detected = True
            break

    output_file = output_folder / f"{domain}.txt"
    try:
        with output_file.open("w", encoding="utf-8") as file:
            file.write(rendered_text)
    except IOError as e:
        logging.error(f"Error writing to {output_file}: {e}")

    return domain, detected

def main() -> None:
    domains = load_domains(DATA_DIR / "backlink.csv")
    sensitive_keywords = load_sensitive_keywords(DATA_DIR / "keyword.txt")
    create_output_dirs()

    process_func = partial(process_domain, sensitive_keywords=sensitive_keywords)

    detected_domains = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(
            tqdm(
                executor.map(process_func, domains),
                total=len(domains),
                desc="Processing domains",
            )
        )
        detected_domains = [domain for domain, detected in results if detected]

    with DETECTED_DOMAINS_FILE.open("w", encoding="utf-8") as file:
        for domain in detected_domains:
            file.write(f"{domain}\n")

    logging.info(f"Detected domains written to {DETECTED_DOMAINS_FILE}")

if __name__ == "__main__":
    main()
