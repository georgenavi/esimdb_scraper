import logging
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed

BASE_API = "https://esimdb.com/api/client"
DEFAULT_USER_AGENT = "esimdb-scraper/1.0 (+https://github.com/georgenavi/esimdb_scraper)"
REQUEST_TIMEOUT_SECONDS = 30
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 1.0
PLANS_REQUEST_DELAY_SECONDS = 0.2

SCHEMA_VERSION = "1.0"

SESSION: Optional[requests.Session] = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(processName)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_session() -> requests.Session:
    """
    Lazily create and return a process-local requests.Session.

    Each process gets its own Session, which is safe for multiprocessing.
    """
    global SESSION
    if SESSION is None:
        session = requests.Session()
        session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
        SESSION = session
    return SESSION


def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = RETRY_ATTEMPTS,
    delay: float = RETRY_BASE_DELAY_SECONDS,
) -> Any:
    """
    Perform a GET request with simple retry logic and return parsed JSON.

    Uses exponential backoff with jitter between retries.
    """
    session = get_session()

    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == retries - 1:
                logger.error("%s failed permanently: %s", url, e)
                raise

            backoff = delay * (2 ** attempt)
            sleep_for = backoff + random.uniform(0, delay)
            logger.warning(
                "%s failed (%s), retry %d/%d in %.2fs...",
                url,
                e,
                attempt + 1,
                retries,
                sleep_for,
            )
            time.sleep(sleep_for)


def fetch_countries(locale: str = "en") -> List[Dict[str, Any]]:
    """
    Fetch all countries from eSIMDB.

    Expected endpoint:
        GET /api/client/countries?locale=en

    This function tries to be defensive about the exact shape of the response.
    """
    url = f"{BASE_API}/countries"
    data = get_json(url, params={"locale": locale})

    if isinstance(data, list):
        raw_countries = data
    else:
        raise ValueError("Unexpected response format from /countries")

    countries: List[Dict[str, Any]] = []
    for c in raw_countries:
        slug = c.get("slug")
        name = c.get("name")
        region = c.get("region")

        if not slug or not name:
            continue

        if isinstance(region, str):
            region_human = region.strip().title()
        else:
            region_human = ""

        countries.append(
            {
                "slug": slug,
                "name": name,
                "region": region_human,
            }
        )

    return countries


def iter_country_plans(
        country_slug: str,
        locale: str = "en",
) -> Iterable[Tuple[Dict[str, Any], str]]:
    """
    Iterate over all plans for a country, across all pages.

    Returns tuples of (plan_dict, provider_name).

    Example endpoint:
        /api/client/countries/france/data-plans?page=1&locale=en

    Deduplicates plans across all pages based on plan ID.
    """
    page = 1
    number_of_pages: Optional[int] = None

    seen_plan_ids: set = set()

    while number_of_pages is None or page <= number_of_pages:
        url = f"{BASE_API}/countries/{country_slug}/data-plans"
        data = get_json(url, params={"page": page, "locale": locale})

        if number_of_pages is None:
            try:
                number_of_pages = int(data.get("numberOfPages", 1))
            except (TypeError, ValueError):
                logger.warning(
                    "Unexpected 'numberOfPages' value for %s, defaulting to 1",
                    country_slug,
                )
                number_of_pages = 1

        providers_map: Dict[str, Dict[str, Any]] = data.get("providers", {})

        plans_main = data.get("plans", []) or []
        plans_featured = data.get("featured", []) or []

        combined_plans = plans_main + plans_featured

        # Deduplicate based on plan ID across all pages
        for plan in combined_plans:
            plan_id = plan.get("id")

            # If plan has an ID, use it for deduplication
            if plan_id is not None:
                if plan_id in seen_plan_ids:
                    continue
                seen_plan_ids.add(plan_id)
            # If no ID, we can't deduplicate reliably, so yield it anyway

            provider_id = plan.get("provider")
            provider_name = ""
            if provider_id and provider_id in providers_map:
                provider_name = providers_map[provider_id].get("name", "") or ""

            yield plan, provider_name

        page += 1
        time.sleep(PLANS_REQUEST_DELAY_SECONDS)


def validate_plan(
    plan: Dict[str, Any],
    provider_name: str,
    country_name: str,
    country_region: str,
) -> Dict[str, Any]:
    """
    Validate and normalize fields:

    - Country
    - Region
    - Price (in USD)
    - Data allowance (in GB)
    - Validity (in days)
    - Provider/Operator name
    """
    usd_price = plan.get("usdPromoPrice")
    if usd_price is None:
        usd_price = plan.get("usdPrice")

    if usd_price is None:
        prices = plan.get("prices") or {}
        usd_price = prices.get("USD")

    capacity = plan.get("capacity")
    capacity_unit = plan.get("capacityUnit")  # if the API happens to provide it
    data_gb: Optional[float] = None

    if isinstance(capacity, (int, float)) and capacity > 0:
        if isinstance(capacity_unit, str):
            unit = capacity_unit.lower()
            if unit in ("mb", "mib"):
                data_gb = float(capacity) / 1000.0
            elif unit in ("gb", "gib"):
                data_gb = float(capacity)
            else:
                # Unknown unit, fall back to heuristic
                data_gb = float(capacity) / 1000.0
                logger.warning(
                    "Unknown capacityUnit '%s' for plan in %s; assuming MB.",
                    capacity_unit,
                    country_name,
                )
        else:
            # Heuristic: small numbers probably GB, large numbers MB
            if capacity <= 25:
                data_gb = float(capacity)
            else:
                data_gb = float(capacity) / 1000.0

        if data_gb is not None and data_gb > 25:
            logger.warning(
                "Suspiciously large data capacity (%.2f GB) for plan in %s",
                data_gb,
                country_name,
            )

    validity_raw = plan.get("period")
    validity_days: Optional[int]
    try:
        validity_days = int(validity_raw) if validity_raw is not None else None
    except (TypeError, ValueError):
        logger.warning(
            "Invalid validity period '%s' for plan in %s; setting to None",
            validity_raw,
            country_name,
        )
        validity_days = None

    plan_name = plan.get("enName") or plan.get("name") or ""

    return {
        "country": country_name,
        "region": country_region,
        "provider": provider_name,
        "plan_name": plan_name,
        "price_usd": usd_price,
        "data_gb": data_gb,
        "validity_days": validity_days,
    }


def sanitize_filename(name: str) -> str:
    """
    Convert a country name (or similar string) into a filesystem-safe filename.

    - Replaces slashes and whitespace with underscores.
    - Removes or replaces other potentially problematic characters.
    """
    safe_name = name.replace("/", "_").replace("\\", "_").replace(" ", "_")
    safe_chars = "".join(c if c.isalnum() or c in "_-" else "_" for c in safe_name)
    return safe_chars.lower()


def scrape_country(
    country: Dict[str, Any],
    output_path: Path,
    locale: str,
    scrape_date: str,
) -> Tuple[bool, int]:
    """
    Scrape all plans for a single country and write them to a Parquet file.

    Returns (success_flag, num_plans_written).
    Applies per-plan error handling so bad plans don't kill the country.
    """
    slug = country["slug"]
    country_name = country["name"]
    region = country["region"]

    logger.info("Processing country: %s (slug=%s)", country_name, slug)

    filename = sanitize_filename(country_name)
    output_file = output_path / f"{filename}.parquet"

    plans_data: List[Dict[str, Any]] = []
    for raw_plan, provider_name in iter_country_plans(slug, locale=locale):
        try:
            row = validate_plan(raw_plan, provider_name, country_name, region)
            plans_data.append(row)
        except Exception as e:
            logger.warning(
                "Skipping problematic plan in %s (slug=%s): %s", country_name, slug, e
            )

    if not plans_data:
        logger.warning("No plans found for %s (slug=%s)", country_name, slug)
        return True, 0

    df = pd.DataFrame(plans_data)

    df["scrape_date"] = scrape_date
    df["schema_version"] = SCHEMA_VERSION

    string_cols = ["country", "region", "provider", "plan_name", "scrape_date", "schema_version"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    numeric_float_cols = ["price_usd", "data_gb"]
    for col in numeric_float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float32")

    if "validity_days" in df.columns:
        df["validity_days"] = (
            pd.to_numeric(df["validity_days"], errors="coerce").astype("Int32")
        )

    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    logger.info("Saved %d plans for %s to %s", len(df), country_name, output_file.name)
    return True, len(df)


def main(output_dir: str = "esimdb_data", locale: str = "en") -> None:
    """
    Orchestrate the full scraping process.

    - Fetches list of countries.
    - Iterates through each country (in parallel via multiprocessing) and scrapes data plans.
    - Writes a Parquet file per country into a date-based subdirectory.
    """
    logger.info("Starting eSIMDB scraper")

    today = datetime.now().strftime("%Y%m%d")
    logger.info("Run date: %s", today)
    logger.info("Fetching countries...")

    countries = fetch_countries(locale=locale)
    logger.info("Found %d countries", len(countries))

    # Create output directory structure
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    output_path = project_root / output_dir / today
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", output_path.absolute())

    successful = 0
    failed = 0
    total_plans = 0

    max_workers = min(5, len(countries))
    logger.info("Processing countries in parallel with %d workers", max_workers)

    # Use ProcessPoolExecutor for multiprocessing
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_country = {
            executor.submit(scrape_country, country, output_path, locale, today): country
            for country in countries
        }

        for future in as_completed(future_to_country):
            country = future_to_country[future]
            name = country["name"]
            slug = country["slug"]

            try:
                success, num_plans = future.result()
                if success:
                    successful += 1
                else:
                    failed += 1
                total_plans += num_plans
                logger.info(
                    "Finished %s (slug=%s), plans=%d, success=%s",name, slug, num_plans, success)
            except Exception as e:
                logger.error("Failed to process %s (slug=%s) in worker: %s",name, slug, e, exc_info=True)
                failed += 1

    logger.info("=" * 60)
    logger.info("Scraping complete!")
    logger.info("Countries successful: %d/%d", successful, len(countries))
    logger.info("Countries failed: %d/%d", failed, len(countries))
    logger.info("Total plans scraped: %d", total_plans)

if __name__ == "__main__":
    main()
