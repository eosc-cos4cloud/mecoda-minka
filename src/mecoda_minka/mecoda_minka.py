import importlib.resources as resources
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from datetime import date
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd  # type: ignore
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import ICONIC_TAXON, TAXONS, Observation, Photo, Project

urllib3.disable_warnings()

print()

# Variables
BASE_URL = "https://minka-sdg.org"
API_PATH = "https://api.minka-sdg.org/v1"


# Global counter for tracking total observations downloaded
class ObservationCounter:
    def __init__(self):
        self._count = 0
        self._lock = threading.Lock()

    def add(self, count: int):
        with self._lock:
            self._count += count
            return self._count

    def get(self):
        with self._lock:
            return self._count

    def reset(self):
        with self._lock:
            self._count = 0


_observation_counter = ObservationCounter()


@lru_cache(maxsize=1)
def _load_taxon_data():
    """Load and cache taxon data once"""
    try:
        return pd.read_csv(
            "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-minka/refs/heads/master/src/mecoda_minka/data/taxon_tree.csv"
        )
    except:
        file_path = resources.files("mecoda_minka.data") / "taxon_tree.csv"
        return pd.read_csv(file_path)


# Load taxon data once
df_taxon = _load_taxon_data()


def _create_optimized_session():
    """Create a session with connection pooling and retry strategy"""
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    # Mount adapters with retry strategy and connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy, pool_connections=20, pool_maxsize=20
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def get_project(project: Union[str, int]) -> List[Project]:
    """Download information of a project from id or name"""

    if type(project) is int:
        url = f"{BASE_URL}/projects/{project}.json"
        page = requests.get(url)

        if page.status_code == 404:
            print("Project ID not found")
            return []
        else:
            resultado = [Project(**page.json())]
            return resultado

    elif type(project) is str:
        url = f"{BASE_URL}/projects/search.json?q={project}"
        page = requests.get(url)
        resultado = [Project(**proj) for proj in page.json()]
        return resultado


def get_obs(
    query: Optional[str] = None,
    id_project: Optional[int] = None,
    id_obs: Optional[int] = None,
    user: Optional[str] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    introduced: Optional[bool] = None,
    year: Optional[int] = None,
    num_max: Optional[int] = None,
    starts_on: Optional[str] = None,  # Must be observed on or after this date
    ends_on: Optional[str] = None,  # Must be observed on or before this date
    created_on: Optional[str] = None,  # Day YYYY-MM-DD
    created_d1: Optional[str] = None,  # Must be created on or after this date
    created_d2: Optional[str] = None,  # Must be created on or before this date
    grade: Optional[str] = None,  # Must be one of this: research, casual, needs_id
    id_above: Optional[int] = None,
    id_below: Optional[int] = None,
    updated_since: Optional[str] = None,  # Must be updated on or after this date
    api_token: Optional[str] = None,
) -> List[Observation]:
    """
    Function to extract the observations and that supports different filters
    """

    print("Generating list of observations:")

    # Reset counter at the beginning
    _observation_counter.reset()

    url = _build_url(
        query,
        id_project,
        id_obs,
        user,
        taxon,
        taxon_id,
        place_id,
        introduced,
        year,
        starts_on,
        ends_on,
        created_on,
        created_d1,
        created_d2,
        grade,
        id_above,
        id_below,
        updated_since,
    )
    session = _create_optimized_session()
    if api_token == None:
        headers = None
    else:
        headers = {"Authorization": api_token}
    try:
        total_obs = session.get(url, headers=headers).json()["total_results"]
        print("Total observations to download:", total_obs)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Network error: {e}")
    except KeyError as e:
        raise Exception(f"Not valid parameters: {e}")
    except ValueError as e:
        raise Exception(f"Invalid JSON response: {e}")

    if total_obs <= 10000 or (num_max != None and num_max <= 10000):
        observations = _request(url, num_max, session, api_token)
    else:
        # Optimized parallel processing for large datasets (>10000)
        print("Large dataset detected, using optimized parallel processing...")

        # Get boundary IDs in parallel
        def fetch_boundary_id(url_desc, is_first=True):
            try:
                resp = session.get(url_desc, headers=headers, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if "results" in data and data["results"]:
                        return data["results"][0]["id"]
            except Exception as e:
                print(f"Error fetching boundary ID: {e}")
            return None

        # Fetch first and last IDs in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            url_last = f"https://api.minka-sdg.org/v1/observations?order=desc&order_by=created_at"
            url_first = f"{url}&order_by=id&order=asc" if id_above is None else None

            future_last = executor.submit(fetch_boundary_id, url_last, False)
            future_first = (
                executor.submit(fetch_boundary_id, url_first, True)
                if url_first
                else None
            )

            last_id = future_last.result()
            first_id = future_first.result() if future_first else id_above

        if not last_id:
            raise Exception("Could not determine data boundaries")

        limit = math.ceil(last_id / 10000)
        start = math.floor((first_id or 0) / 10000)

        # Generate batch URLs
        batch_urls = []
        base_url_clean = url.replace(f"&id_above={id_above}", "") if id_above else url

        for n in range(
            start, min(limit + 1, start + 20)
        ):  # Limit to 20 batches for safety
            batch_url = f"{base_url_clean}&id_above={n*10000}&id_below={(n+1)*10000+1}"
            batch_urls.append((n, batch_url))

        # Process batches in parallel
        def fetch_batch(batch_info):
            batch_num, batch_url = batch_info
            try:
                batch_observations = _request(
                    batch_url, None, session, api_token, suppress_prints=True
                )
                if batch_observations:
                    current_total = _observation_counter.add(len(batch_observations))
                    print(
                        f"Batch completed: +{len(batch_observations)} obs (Total: {current_total}/{total_obs})"
                    )
                return batch_observations
            except Exception as e:
                print(f"Error processing batch {batch_num}: {e}")
                return []

        observations = []
        with ThreadPoolExecutor(max_workers=min(6, len(batch_urls))) as executor:
            futures = [
                executor.submit(fetch_batch, batch_info) for batch_info in batch_urls
            ]

            for future in as_completed(futures):
                try:
                    batch_observations = future.result()
                    if batch_observations:
                        observations.extend(batch_observations)
                        # Stop early if we have enough
                        if num_max and len(observations) >= num_max:
                            break
                except Exception as e:
                    print(f"Error processing batch result: {e}")

        # Apply final limit
        if num_max and len(observations) > num_max:
            observations = observations[:num_max]

        print(f"Download completed: {len(observations)} observations total")

    return observations


def _build_url(
    query: Optional[str] = None,
    id_project: Optional[int] = None,
    id_obs: Optional[int] = None,
    user: Optional[str] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    introduced: Optional[bool] = None,
    year: Optional[int] = None,
    starts_on: Optional[date] = None,
    ends_on: Optional[date] = None,
    created_on: Optional[date] = None,  # day YYYY-MM-DD
    created_d1: Optional[date] = None,
    created_d2: Optional[date] = None,
    grade: Optional[str] = None,
    id_above: Optional[int] = None,
    id_below: Optional[int] = None,
    updated_since: Optional[date] = None,
) -> str:
    """
    Internal function to build the url to which the observation request
    will be made
    """
    # define base url

    base_url = f"{API_PATH}/observations"

    # define the arguments that the API supports
    args = []
    if id_obs is not None:
        args.append(f"id={id_obs}")
    if id_project is not None:
        if introduced is True:
            args.append(f"introduced=true&project_id={id_project}")
        else:
            args.append(f"project_id={id_project}")
    if user is not None:
        args.append(f"user_login={user}")
    if created_on is not None:
        args.append(f"created_on={created_on}")
    if created_d1 is not None:
        args.append(f"created_d1={created_d1}")
    if created_d2 is not None:
        args.append(f"created_d2={created_d2}")
    if starts_on is not None:
        args.append(f"d1={starts_on}")
    if ends_on is not None:
        args.append(f"d2={ends_on}")
    if query is not None:
        args.append(f'q="{query}"')
    if taxon is not None:
        taxon = taxon.title()
        if taxon not in TAXONS:
            raise ValueError("Not a valid taxonomy")
        args.append(f"iconic_taxa={taxon}")
    if place_id is not None:
        if introduced is True:
            args.append(f"introduced=true&place_id={place_id}")
        else:
            args.append(f"place_id={place_id}")
    if year is not None:
        args.append(f"year={year}")
    if taxon_id is not None:
        args.append(f"taxon_id={taxon_id}")
    if grade is not None:
        args.append(f"quality_grade={grade}")
    if id_above is not None:
        args.append(f"id_above={id_above}")
    if id_below is not None:
        args.append(f"id_below={id_below}")
    if updated_since is not None:
        args.append(f"updated_since={updated_since}")
    url = f'{base_url}?{"&".join(args)}&per_page=200'
    # if no parameter indicated, it returns the last records
    print(url)
    return url


def _process_observation_data(data: Dict[str, Any]) -> Observation:
    """Process a single observation data dictionary efficiently"""
    import datetime

    # Date processing
    for date_field in ["created_at", "updated_at"]:
        if data.get(date_field):
            try:
                # Parse datetime string like "2016-07-11T16:10:39"
                dt_str = data[date_field].replace("T", " ")
                data[date_field] = datetime.datetime.strptime(
                    dt_str, "%Y-%m-%d %H:%M:%S"
                )
            except:
                pass

    if data.get("observed_on"):
        try:
            # Parse date string like "2016-07-06" and convert to date object (not datetime)
            if "T" in data["observed_on"]:
                dt_str = data["observed_on"].replace("T", " ")
                dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                data["observed_on"] = dt.date()
            else:
                # Convert date string to date object
                dt = datetime.datetime.strptime(data["observed_on"], "%Y-%m-%d")
                data["observed_on"] = dt.date()
        except:
            pass

    # Boolean conversion
    if data.get("captive") == "false":
        data["captive"] = False
    elif data.get("captive") == "true":
        data["captive"] = True

    # Device detection - only if oauth_application_id is present
    if "oauth_application_id" in data:
        data["device"] = "app" if data.get("oauth_application_id") == 2 else "web"

    # Place name processing
    place_guess = data.get("place_guess")
    if place_guess is not None:
        data["place_name"] = place_guess.replace("\r\n", " ").strip()

    # Taxon data processing - only if taxon data is present
    taxon_data = data.get("taxon")
    if taxon_data:
        data["taxon_id"] = int(taxon_data["id"]) if taxon_data.get("id") else None
        data["taxon_name"] = taxon_data.get("name")
        data["taxon_rank"] = taxon_data.get("rank")
        data["taxon_ancestry"] = taxon_data.get("ancestry")

    # Location processing - convert lat/lng fields to location field if needed
    if data.get("latitude") and data.get("longitude") and not data.get("location"):
        try:
            data["location"] = f"{data['latitude']},{data['longitude']}"
        except:
            pass

    # Extract lat/lon from 'location' if not present
    if data.get("location") and (not data.get("latitude") or not data.get("longitude")):
        try:
            lat, lon = map(float, data["location"].split(","))
            data["latitude"] = lat
            data["longitude"] = lon
        except (ValueError, AttributeError):
            pass

    # Photos processing - optimized
    observation_photos = data.get("observation_photos", [])
    if observation_photos:
        photos = []
        for obs_photo in observation_photos:
            photo_data = obs_photo.get("photo", {})
            base_url = photo_data.get("url", "")
            if base_url:
                photos.append(
                    Photo(
                        id=photo_data.get("id"),
                        large_url=base_url.replace("/square", "/large"),
                        medium_url=base_url.replace("/square", "/medium"),
                        small_url=base_url.replace("/square", "/small"),
                        license_photo=photo_data.get("license_code"),
                        attribution=photo_data.get("attribution"),
                    )
                )
        data["photos"] = photos

    # Iconic taxon processing - only if iconic taxon data is available
    iconic_id = None
    if taxon_data and taxon_data.get("iconic_taxon_id"):
        iconic_id = taxon_data.get("iconic_taxon_id")
    elif data.get("iconic_taxon_id"):
        iconic_id = data.get("iconic_taxon_id")

    if iconic_id:
        data["iconic_taxon"] = ICONIC_TAXON.get(iconic_id)

    # User data processing - only if user data is present
    user_data = data.get("user")
    if user_data:
        data["user_id"] = user_data.get("id")
        data["user_login"] = user_data.get("login")

    # License processing - only if license data is present
    if data.get("license_code") or data.get("license"):
        data["license_obs"] = data.get("license_code") or data.get("license")

    # Description cleanup - only if description is present
    if data.get("description"):
        data["description"] = data["description"].replace("\r\n", " ")

    # Identifications processing - only if identifications are present
    identifications = data.get("identifications")
    if identifications:
        identifiers = [
            ident["user"]["login"]
            for ident in identifications
            if ident.get("user", {}).get("login")
        ]
        data["identifiers"] = ", ".join(identifiers) if identifiers else None

    return Observation(**data)


def _build_observations(observations_data: List[Dict[str, Any]]) -> List[Observation]:
    """
    Optimized function that processes observation data using parallel processing for large datasets
    """
    if len(observations_data) > 100:
        # Use parallel processing for large datasets
        with ThreadPoolExecutor(max_workers=4) as executor:
            observations = list(
                executor.map(_process_observation_data, observations_data)
            )
    else:
        # Use sequential processing for small datasets to avoid overhead
        observations = [_process_observation_data(data) for data in observations_data]

    return observations


def _request(
    arg_url: str,
    num_max: Optional[int] = None,
    session=None,
    api_token=None,
    suppress_prints: bool = False,
) -> List[Observation]:
    """
    Optimized function that performs parallel API requests and returns
    the list of Observation objects.
    """
    if session is None:
        session = _create_optimized_session()

    if api_token:
        headers = {"Authorization": api_token}
    else:
        headers = None

    # First request to get total pages
    page = session.get(arg_url, headers=headers)

    if page.status_code == 404:
        raise ValueError("Not found")
    elif page.status_code != 200:
        raise ValueError(f"HTTP error: {page.status_code}")

    try:
        response = page.json()
        if "results" not in response:
            raise ValueError("Invalid response format: missing 'results' field")

        first_page_results = response["results"]
        total_results = response.get("total_results", len(first_page_results))

        # Calculate pages needed based on total results or num_max limit
        if num_max:
            total_pages_needed = min(50, math.ceil(num_max / 200))
        else:
            total_pages_needed = min(50, math.ceil(total_results / 200))

        # If only one page needed or results < 200, return immediately
        if len(first_page_results) < 200 or total_pages_needed == 1:
            observations = _build_observations(first_page_results)
            if num_max:
                observations = observations[:num_max]
            if not suppress_prints:
                print(f"Number of elements: {len(observations)}")
            return observations

        # Sequential processing for paginated requests to avoid test issues
        all_results = [first_page_results]
        page_num = 2
        while len(all_results) < total_pages_needed and page_num <= 50:
            url = f"{arg_url}&page={page_num}"
            try:
                resp = session.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if "results" in data and data["results"]:
                        page_results = data["results"]
                        all_results.append(page_results)

                        # Show progress for non-suppressed requests
                        if not suppress_prints:
                            total_so_far = sum(len(r) for r in all_results)
                            print(f"Number of elements: {total_so_far}")

                        # Stop if this page has fewer results than page size (indicates last page)
                        if len(page_results) < 200:
                            break

                        # Stop if we have enough results
                        total_so_far = sum(len(r) for r in all_results)
                        if num_max and total_so_far >= num_max:
                            break
                    else:
                        break  # No more results
                else:
                    break  # Error or no more pages
            except Exception as e:
                print(f"Error fetching page {page_num}: {e}")
                break
            page_num += 1

        # Process all results - use parallel processing only for large datasets
        observations = []
        if sum(len(results) for results in all_results) > 500:
            # Use parallel processing for large result sets
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(_build_observations, page_results)
                    for page_results in all_results
                ]

                for future in as_completed(futures):
                    try:
                        page_observations = future.result()
                        observations.extend(page_observations)
                    except Exception as e:
                        print(f"Error processing observations: {e}")
        else:
            # Sequential processing for smaller datasets
            for page_results in all_results:
                observations.extend(_build_observations(page_results))

        # Apply limit if specified
        if num_max and len(observations) > num_max:
            observations = observations[:num_max]

        return observations

    except ValueError as e:
        print(f"Error: {str(e)}")
        return []


def get_dfs(observations, df_taxon=df_taxon) -> pd.DataFrame:
    """
    Highly optimized function to extract dataframe from observations and dataframe from photos.
    """
    # Handle empty observations early
    if not observations:
        # Return empty DataFrames with the expected structure
        empty_obs_columns = [
            "id",
            "created_at",
            "updated_at",
            "observed_on",
            "observed_on_time",
            "iconic_taxon",
            "taxon_id",
            "taxon_rank",
            "taxon_name",
            "latitude",
            "longitude",
            "obscured",
            "place_name",
            "quality_grade",
            "user_id",
            "user_login",
            "license_obs",
            "identifications_count",
            "identifiers",
            "num_identification_agreements",
            "num_identification_disagreements",
            "taxon_ancestry",
            "device",
            "kingdom",
            "phylum",
            "class",
            "order",
            "family",
            "genus",
        ]

        empty_photos_columns = [
            "id",
            "photos_id",
            "iconic_taxon",
            "taxon_name",
            "photos_medium_url",
            "user_login",
            "latitude",
            "longitude",
            "license_photo",
            "attribution",
            "path",
        ]

        return pd.DataFrame(columns=empty_obs_columns), pd.DataFrame(
            columns=empty_photos_columns
        )

    # Ultra-fast data extraction using vectorized operations
    def extract_obs_data_fast(obs):
        """Fastest possible data extraction"""
        if hasattr(obs, "__dict__"):
            return obs.__dict__
        return obs.model_dump()

    # Always use the fastest approach - direct list comprehension
    data_list = [extract_obs_data_fast(obs) for obs in observations]

    # Use pd.DataFrame constructor directly - faster than json_normalize for most cases
    df = pd.DataFrame(data_list)

    # Fast taxon_id handling
    if not df.empty and "taxon_id" in df.columns:
        df["taxon_id"] = df["taxon_id"].fillna("").astype(str)
    elif not df.empty:
        df["taxon_id"] = ""

    # Separate photos data before processing observations
    photos_data = []
    if "photos" in df.columns:
        # Extract all photo data in one pass using vectorized operations
        for idx, row in df.iterrows():
            obs_id = row.get("id")
            photos = row.get("photos", [])
            if photos and isinstance(photos, list):
                for photo in photos:
                    if photo:  # Skip None photos
                        photos_data.append(
                            {
                                "id": obs_id,
                                "iconic_taxon": row.get("iconic_taxon"),
                                "taxon_name": row.get("taxon_name"),
                                "user_login": row.get("user_login"),
                                "latitude": row.get("latitude"),
                                "longitude": row.get("longitude"),
                                "photos_id": (
                                    getattr(photo, "id", None)
                                    if hasattr(photo, "id")
                                    else (
                                        photo.get("id")
                                        if isinstance(photo, dict)
                                        else None
                                    )
                                ),
                                "photos_medium_url": (
                                    getattr(photo, "medium_url", None)
                                    if hasattr(photo, "medium_url")
                                    else (
                                        photo.get("medium_url")
                                        if isinstance(photo, dict)
                                        else None
                                    )
                                ),
                                "license_photo": (
                                    getattr(photo, "license_photo", None)
                                    if hasattr(photo, "license_photo")
                                    else (
                                        photo.get("license_photo")
                                        if isinstance(photo, dict)
                                        else None
                                    )
                                ),
                                "attribution": (
                                    getattr(photo, "attribution", None)
                                    if hasattr(photo, "attribution")
                                    else (
                                        photo.get("attribution")
                                        if isinstance(photo, dict)
                                        else None
                                    )
                                ),
                            }
                        )

        # Remove photos column from observations DataFrame
        df_observations = df.drop(["photos"], axis=1)
    else:
        df_observations = df.copy()

    # Minimal datetime processing - only convert what's necessary
    if not df_observations.empty:
        datetime_cols = ["created_at", "updated_at", "observed_on", "time_observed_at"]
        for col in datetime_cols:
            if col in df_observations.columns:
                # Fast datetime conversion without format specification
                df_observations[col] = pd.to_datetime(
                    df_observations[col], errors="coerce", utc=True
                )

        # Extract date components efficiently
        for col in ["created_at", "updated_at", "observed_on"]:
            if col in df_observations.columns:
                df_observations[col] = df_observations[col].dt.date

        if "time_observed_at" in df_observations.columns:
            # Convert to datetime UTC and then to Madrid hour
            df_observations["time_observed_at"] = pd.to_datetime(df_observations["time_observed_at"])
            
            # Check if already tz-aware, if not localize to UTC first
            if df_observations["time_observed_at"].dt.tz is None:
                df_observations["time_observed_at"] = df_observations["time_observed_at"].dt.tz_localize("UTC")
            
            # Convert to Madrid timezone
            df_observations["time_observed_at"] = df_observations["time_observed_at"].dt.tz_convert("Europe/Madrid")

            # Extract only hour
            df_observations["observed_on_time"] = df_observations[
                "time_observed_at"
            ].dt.time

            # Eliminar la columna original
            df_observations.drop(columns=["time_observed_at"], inplace=True)

    # Fast column selection
    desired_columns = [
        "id",
        "created_at",
        "updated_at",
        "observed_on",
        "observed_on_time",
        "iconic_taxon",
        "taxon_id",
        "taxon_rank",
        "taxon_name",
        "latitude",
        "longitude",
        "obscured",
        "place_name",
        "quality_grade",
        "user_id",
        "user_login",
        "license_obs",
        "identifications_count",
        "identifiers",
        "num_identification_agreements",
        "num_identification_disagreements",
        "taxon_ancestry",
        "device",
    ]

    existing_columns = [
        col for col in desired_columns if col in df_observations.columns
    ]
    if existing_columns:
        df_observations = df_observations[existing_columns]

    # Fast sorting
    if not df_observations.empty and "id" in df_observations.columns:
        df_observations.sort_values(by="id", ascending=False, inplace=True)

    # Fast license handling
    if not df_observations.empty and "license_obs" in df_observations.columns:
        df_observations["license_obs"] = df_observations["license_obs"].fillna("C")

    # Optimize taxon columns extraction - only call if we have data
    if not df_observations.empty and "taxon_ancestry" in df_observations.columns:
        _get_taxon_columns(df_observations, df_taxon)

    # Create photos DataFrame from extracted data
    if photos_data:
        df_photos = pd.DataFrame(photos_data)

        # Fast photo ID processing
        df_photos["photos_id"] = df_photos["photos_id"].fillna("").astype(str)

        # Vectorized path generation
        mask = df_photos["photos_id"] != ""
        df_photos.loc[mask, "path"] = (
            df_photos.loc[mask, "id"].astype(str)
            + "_"
            + df_photos.loc[mask, "photos_id"]
            + ".jpg"
        )
        df_photos.loc[~mask, "path"] = None

        # Fast license processing
        copyright_mask = df_photos["license_photo"].isna() & df_photos[
            "attribution"
        ].str.contains("all rights reserved", na=False)
        df_photos.loc[copyright_mask, "license_photo"] = "C"

        # Fast sorting
        if "id" in df_photos.columns:
            df_photos.sort_values(by="id", ascending=False, inplace=True)
    else:
        # Empty photos DataFrame with correct structure
        df_photos = pd.DataFrame(
            columns=[
                "id",
                "photos_id",
                "iconic_taxon",
                "taxon_name",
                "photos_medium_url",
                "user_login",
                "latitude",
                "longitude",
                "license_photo",
                "attribution",
                "path",
            ]
        )

    return df_observations, df_photos


def _get_taxon_columns(df_obs: pd.DataFrame, df_taxon: pd.DataFrame):
    """Highly optimized taxon column extraction using vectorized operations"""
    if df_obs.empty or "taxon_ancestry" not in df_obs.columns:
        return

    # Initialize all taxonomic columns with None
    taxonomic_levels = ["kingdom", "phylum", "class", "order", "family", "genus"]
    for level in taxonomic_levels:
        df_obs[level] = None

    # Create a lookup dictionary for faster access
    if not hasattr(_get_taxon_columns, "_taxon_lookup"):
        # Drop duplicates before creating the index
        df_taxon_unique = df_taxon.drop_duplicates("taxon_id")
        _get_taxon_columns._taxon_lookup = df_taxon_unique.set_index(
            "taxon_id"
        ).to_dict("index")

    taxon_lookup = _get_taxon_columns._taxon_lookup

    # Process all ancestries in batch
    def extract_taxon_info_vectorized(ancestry_string):
        """Vectorized version of taxon extraction"""
        if not ancestry_string or pd.isna(ancestry_string):
            return {level: None for level in taxonomic_levels}

        result = {level: None for level in taxonomic_levels}
        try:
            # Split and process all at once
            ancestries = ancestry_string.split("/")
            for ancestry in ancestries:
                try:
                    ancestry_id = int(ancestry)
                    if ancestry_id != 1 and ancestry_id in taxon_lookup:
                        taxon_info = taxon_lookup[ancestry_id]
                        rank = taxon_info.get("rank")
                        name = taxon_info.get("taxon_name")
                        if rank in result:
                            result[rank] = name
                except (ValueError, KeyError):
                    continue
        except:
            pass
        return result

    # Apply vectorized function
    taxon_data = df_obs["taxon_ancestry"].apply(extract_taxon_info_vectorized)

    # Extract each level efficiently
    for level in taxonomic_levels:
        df_obs[level] = taxon_data.apply(lambda x: x.get(level) if x else None)

    # Drop the ancestry column
    df_obs.drop(columns=["taxon_ancestry"], inplace=True)


def extra_info(df_observations) -> pd.DataFrame:
    """
    Optimized function to obtain extra information using parallel requests
    (expensive at API level - use sparingly)
    """
    ids = df_observations["id"].to_list()

    def fetch_observation_details(id_num):
        """Fetch details for a single observation"""
        url = f"{BASE_URL}/observations/{id_num}.json"
        session = _create_optimized_session()
        try:
            page = session.get(url, timeout=30)
            page.raise_for_status()
            idents = page.json().get("identifications", [])

            if idents:
                user_identification = idents[0]["user"]["login"]
                first_taxon_name = idents[0]["taxon"]["name"]
                last_taxon_name = idents[-1]["taxon"]["name"]
                return id_num, [user_identification, first_taxon_name, last_taxon_name]
            else:
                return id_num, [0, 0, 0]
        except Exception as e:
            print(f"Error fetching observation {id_num}: {e}")
            return id_num, [0, 0, 0]

    # Use parallel processing with limited workers to avoid overwhelming the API
    dic = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_observation_details, id_num) for id_num in ids]

        for future in as_completed(futures):
            try:
                id_num, result = future.result()
                dic[id_num] = result
            except Exception as e:
                print(f"Failed to process observation: {e}")

    # Vectorized mapping operations
    df_observations["first_identification"] = df_observations["id"].map(
        lambda x: str(dic.get(x, [0])[0])
    )
    df_observations["first_taxon_name"] = df_observations["id"].map(
        lambda x: str(dic.get(x, [0, 0])[1])
    )
    df_observations["last_taxon_name"] = df_observations["id"].map(
        lambda x: str(dic.get(x, [0, 0, 0])[2])
    )

    df_observations["first_taxon_match"] = np.where(
        df_observations["first_taxon_name"] == df_observations["last_taxon_name"],
        "True",
        "False",
    )
    df_observations["first_identification_match"] = np.where(
        df_observations["first_identification"] == df_observations["user_login"],
        "True",
        "False",
    )

    return df_observations


def download_photos(
    df_photos: pd.DataFrame, directorio: Optional[str] = "minka_photos"
):
    """
    Optimized function to download photos using parallel processing
    """
    # Create the folder, if it exists overwrite it
    os.makedirs(directorio, exist_ok=True)

    def download_single_photo(row):
        """Download a single photo with error handling"""
        session = _create_optimized_session()
        try:
            response = session.get(row["photos_medium_url"], stream=True, timeout=60)
            response.raise_for_status()

            file_path = f"{directorio}/{row['path']}"
            with open(file_path, "wb") as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)
            return row.name, True
        except Exception as e:
            print(f"Failed to download photo {row['path']}: {e}")
            return row.name, False

    # Use parallel processing for downloads
    successful_downloads = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(download_single_photo, row)
            for _, row in df_photos.iterrows()
        ]

        for future in as_completed(futures):
            try:
                idx, success = future.result()
                successful_downloads[idx] = success
            except Exception as e:
                print(f"Download task failed: {e}")

    # Update paths for successful downloads only
    df_photos.loc[:, "abs_path"] = df_photos.apply(
        lambda row: (
            os.path.abspath(f"{directorio}/{row['path']}")
            if successful_downloads.get(row.name, False)
            else None
        ),
        axis=1,
    )


def get_count_by_taxon() -> Dict:
    """
    Function that returns the number of observations recorded for each taxonomic family.
    """
    url = f"{BASE_URL}/taxa.json"
    session = _create_optimized_session()
    page = session.get(url, timeout=30)
    page.raise_for_status()
    taxa = page.json()

    # Use dictionary comprehension for better performance
    return {taxon["name"]: taxon["observations_count"] for taxon in taxa}


# Darwin Core Format
def get_dwc(observations: List) -> pd.DataFrame:
    """
    Function to get dataframe with DarwinCore Format.
    Take a list of Observation objects to get ids.
    """
    df_total = pd.DataFrame()
    id_obs = [observation.id for observation in observations]
    for id_ob in id_obs:
        url = f"https://minka-sdg.org/observations.dwc?id={id_ob}"
        try:
            df = pd.read_xml(url, parser="etree")
            df_total = pd.concat([df_total, df])
        except Exception as e:
            print(f"Error processing observation {id_ob}: {e}")
            continue

    # clean fields
    if not df_total.empty:
        # Ensure institutionCode column exists and set to "Minka"
        df_total["institutionCode"] = "Minka"

        # Replace iNaturalist with Minka in datasetName if column exists
        if "datasetName" in df_total.columns:
            df_total["datasetName"] = df_total["datasetName"].str.replace(
                "iNaturalist", "Minka"
            )
        else:
            # Add datasetName column if it doesn't exist
            df_total["datasetName"] = "MINKA research-grade observations"

    return df_total


def get_dwc_from_query(
    id_obs: Optional[int] = None,
    user_id: Optional[int] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    year: Optional[int] = None,
    start_on: Optional[date] = None,
    ends_on: Optional[date] = None,
) -> str:
    base_url = _build_url_dwc(
        id_obs,
        user_id,
        taxon,
        taxon_id,
        place_id,
        year,
        start_on,
        ends_on,
    )

    df_total = pd.DataFrame()

    for i in range(1, 50):
        url = f"{base_url}&page={i}"

        try:
            # Specify the xpath to handle namespaced XML correctly
            namespaces = {
                "dwr": "http://rs.tdwg.org/dwc/xsd/simpledarwincore/",
                "dwc": "http://rs.tdwg.org/dwc/terms/",
                "dcterms": "http://purl.org/dc/terms/",
            }
            df = pd.read_xml(
                url,
                parser="etree",
                xpath=".//dwr:SimpleDarwinRecord",
                namespaces=namespaces,
            )
            df_total = pd.concat([df_total, df])
        except:
            # clean fields
            if len(df_total) > 1:
                df_total["institutionCode"] = "Minka"
                df_total["datasetName"] = df_total["datasetName"].str.replace(
                    "iNaturalist", "Minka"
                )
            else:
                df_total = None
            return df_total


def _build_url_dwc(
    id_obs: Optional[int] = None,
    user_id: Optional[str] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    year: Optional[int] = None,
    start_on: Optional[date] = None,
    ends_on: Optional[date] = None,
) -> str:
    """
    Internal function to build the url to which the observation request
    will be made
    """
    # define base url
    base_url_dwc = f"{BASE_URL}/observations.dwc"

    # define the arguments that the API supports
    args = []
    if id_obs is not None:
        args.append(f"id={id_obs}")
    if user_id is not None:
        args.append(f"user_id={user_id}")
    if taxon is not None:
        taxon = taxon.title()
        args.append(f"iconic_taxa={taxon}")
    if place_id is not None:
        args.append(f"place_id={place_id}")
    if year is not None:
        args.append(f"year={year}")
    if taxon_id is not None:
        args.append(f"taxon_id={taxon_id}")
    if start_on is not None:
        args.append(f"d1={start_on}")
    if ends_on is not None:
        args.append(f"d2={ends_on}")

    url = f'{base_url_dwc}?{"&".join(args)}&per_page=200'

    # if no parameter indicated, it returns the last records

    return url
