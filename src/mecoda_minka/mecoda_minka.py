import os
import shutil
from contextlib import suppress
from datetime import date
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pkg_resources
import requests
import urllib3

from .models import ICONIC_TAXON, TAXONS, Observation, Photo, Project

urllib3.disable_warnings()

# Variables
API_URL = "https://minka-sdg.org"


def get_project(project: Union[str, int]) -> List[Project]:
    """Download information of a project from id or name"""

    if type(project) is int:
        url = f"{API_URL}/projects/{project}.json"
        page = requests.get(url)

        if page.status_code == 404:
            print("Project ID not found")
            return []
        else:
            resultado = [Project(**page.json())]
            return resultado

    elif type(project) is str:
        url = f"{API_URL}/projects/search.json?q={project}"
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
    year: Optional[int] = None,
    num_max: Optional[int] = None,
    starts_on: Optional[str] = None,  # Must be observed on or after this date
    ends_on: Optional[str] = None,  # Must be observed on or before this date
    created_on: Optional[str] = None,  # Day YYYY-MM-DD
    created_d1: Optional[str] = None,  # Must be created on or after this date
    created_d2: Optional[str] = None,  # Must be created on or before this date
) -> List[Observation]:
    """
    Function to extract the observations and that supports different filters
    """

    print("Generating list of observations:")

    url = _build_url(
        query,
        id_project,
        id_obs,
        user,
        taxon,
        taxon_id,
        place_id,
        year,
        starts_on,
        ends_on,
        created_on,
        created_d1,
        created_d2,
    )

    observations = _request(url, num_max)

    return observations


def _build_url(
    query: Optional[str] = None,
    id_project: Optional[int] = None,
    id_obs: Optional[int] = None,
    user: Optional[str] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    year: Optional[int] = None,
    start_on: Optional[date] = None,
    ends_on: Optional[date] = None,
    created_on: Optional[date] = None,  # day YYYY-MM-DD
    created_d1: Optional[date] = None,
    created_d2: Optional[date] = None,
) -> str:
    """
    Internal function to build the url to which the observation request
    will be made
    """
    # define base url
    if id_obs is not None:
        base_url = f"{API_URL}/observations/{id_obs}.json"
    else:
        # base_url = f"{API_URL}/observations.json"
        base_url = f"{API_URL}:4000/v1/observations"

    # define the arguments that the API supports
    args = []
    if id_project is not None:
        args.append(f"project_id={id_project}")
    if user is not None:
        args.append(f"user_login={user}")
    if created_on is not None:
        args.append(f"created_on={created_on}")
    if created_d1 is not None:
        args.append(f"created_d1={created_d1}")
    if created_d2 is not None:
        args.append(f"created_d2={created_d2}")
    if start_on is not None:
        args.append(f"d1={start_on}")
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
        args.append(f"place_id={place_id}")
    if year is not None:
        args.append(f"year={year}")
    if taxon_id is not None:
        args.append(f"taxon_id={taxon_id}")

    url = f'{base_url}?{"&".join(args)}&per_page=200'

    # if no parameter indicated, it returns the last records

    return url


def _build_observations(observations_data: List[Dict[str, Any]]) -> List[Observation]:
    """
    Inner function that takes a list of dictionaries and returns a list
    of Observation objects.
    """
    observations = []

    for data in observations_data:
        with suppress(KeyError):
            if data["place_guess"] is not None:
                data["place_name"] = data["place_guess"].replace("\r\n", " ").strip()

        with suppress(KeyError):
            try:
                data["taxon_id"] = int(data["taxon"]["id"])
                data["taxon_name"] = data["taxon"]["name"]
                data["taxon_rank"] = data["taxon"]["rank"]
                data["taxon_ancestry"] = data["taxon"]["ancestry"]
            except:
                data["taxon_id"] = None
                data["taxon_name"] = None
                data["taxon_rank"] = None
                data["taxon_ancestry"] = None

        with suppress(KeyError):
            try:
                data["latitude"] = data["location"].split(",")[0]
                data["longitude"] = data["location"].split(",")[1]
            except:
                data["latitude"] = None
                data["longitude"] = None

        with suppress(KeyError):
            lista_fotos = []
            # Caso para las observaciones filtradas por id
            if len(observations_data) == 1:
                for observation_photo in data["observation_photos"]:
                    lista_fotos.append(
                        Photo(
                            id=observation_photo["photo"]["id"],
                            large_url=observation_photo["photo"]["large_url"],
                            medium_url=observation_photo["photo"]["medium_url"],
                            small_url=observation_photo["photo"]["small_url"],
                            license_photo=observation_photo["photo"]["license_code"],
                            attribution=observation_photo["photo"]["attribution"],
                        )
                    )
            # Resto de búsquedas
            else:
                for observation_photo in data["observation_photos"]:
                    lista_fotos.append(
                        Photo(
                            id=observation_photo["photo"]["id"],
                            large_url=observation_photo["photo"]["url"].replace(
                                "/square", "/large"
                            ),
                            medium_url=observation_photo["photo"]["url"].replace(
                                "/square", "/medium"
                            ),
                            small_url=observation_photo["photo"]["url"].replace(
                                "/square", "/small"
                            ),
                            license_photo=observation_photo["photo"]["license_code"],
                            attribution=observation_photo["photo"]["attribution"],
                        )
                    )
            data["photos"] = lista_fotos

        with suppress(KeyError):
            try:
                try:
                    data["iconic_taxon"] = ICONIC_TAXON[
                        data["taxon"]["iconic_taxon_id"]
                    ]
                except:
                    # request de un solo id de observación
                    data["iconic_taxon"] = ICONIC_TAXON[data["iconic_taxon_id"]]
            except:
                data["iconic_taxon"] = None

        with suppress(KeyError):
            data["user_id"] = data["user"]["id"]
            data["user_login"] = data["user"]["login"]

        with suppress(KeyError):
            try:
                data["license_obs"] = data["license_code"]
            except:
                data["license_obs"] = data["license"]

        # removal of line breaks in the description field
        with suppress(KeyError):
            if data["description"] is not None:
                data["description"] = data["description"].replace("\r\n", " ")

        observation = Observation(**data)

        observations.append(observation)

    return observations


def _request(arg_url: str, num_max: Optional[int] = None) -> List[Observation]:
    """
    Internal function that performs the API request and returns
    the list of Observation objects.
    """
    observations = []
    n = 1
    page = requests.get(arg_url)

    if page.status_code == 404:
        raise ValueError("Not found")

    elif page.status_code == 200:
        try:
            while len(page.json()["results"]) == 200:
                observations.extend(_build_observations(page.json()["results"]))
                n += 1
                if n > 49:
                    print("WARNING: Only the first 10,000 results are displayed")
                    break
                if num_max is not None and len(observations) >= num_max:
                    break
                url = f"{arg_url}&page={n}"
                page = requests.get(url)
                print(f"Number of elements: {len(observations)}")

            observations.extend(_build_observations(page.json()["results"]))
            if num_max:
                observations = observations[:num_max]

        except:
            # Caso de los requests con el id de una única observación, no tienen results
            observations.extend(_build_observations([page.json()]))

        print(f"Number of elements: {len(observations)}")

    return observations


def get_dfs(observations) -> pd.DataFrame:
    """
    Function to extract dataframe from observations and dataframe from photos.
    """
    df = pd.DataFrame([obs.model_dump() for obs in observations])
    df["taxon_id"] = df["taxon_id"].astype(float).apply(lambda x: f"{x:.0f}")

    df_observations = df.drop(["photos"], axis=1)
    df_observations["created_at"] = pd.to_datetime(
        df_observations["created_at"], format="%Y-%m-%d %H:%M:%S", utc=True
    ).dt.date
    df_observations["updated_at"] = pd.to_datetime(
        df_observations["updated_at"], format="%Y-%m-%d %H:%M:%S", utc=True
    ).dt.date
    df_observations["observed_on"] = pd.to_datetime(
        df_observations["observed_on"], format="%Y-%m-%d %H:%M:%S", utc=True
    ).dt.date

    # Las observaciones con licencia None son Copyright
    df_observations.loc[df_observations.license_obs.isnull(), "license_obs"] = "C"

    _get_taxon_columns(df_observations)
    df_photos = df[
        [
            "id",
            "photos",
            "iconic_taxon",
            "taxon_name",
            "user_login",
            "latitude",
            "longitude",
        ]
    ]
    df_photos = df_photos.explode("photos").reset_index(drop=True)
    df_photos["photos_id"] = df_photos.photos.str.get("id")
    df_photos["photos_medium_url"] = df_photos.photos.str.get("medium_url")
    df_photos["license_photo"] = df_photos.photos.str.get("license_photo")
    df_photos["attribution"] = df_photos.photos.str.get("attribution")

    df_photos = df_photos[
        [
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
        ]
    ]
    df_photos["photos_id"] = (
        df_photos["photos_id"].astype(float).apply(lambda x: f"{x:.0f}")
    )
    df_photos["path"] = (
        df_photos["id"].astype(str) + "_" + df_photos["photos_id"].astype(str) + ".jpg"
    )
    # El campo queda en blanco en los C
    df_photos.loc[
        (df_photos["license_photo"].isnull())
        & (df_photos.attribution.str.contains("all rights reserved")),
        "license_photo",
    ] = "C"
    return df_observations, df_photos


def _get_taxon_columns(df_obs: pd.DataFrame):
    file_path = pkg_resources.resource_filename("mecoda_minka", "data/taxon_tree.csv")
    df_taxon = pd.read_csv(file_path)
    df_obs["taxon_ancestry"] = df_obs["taxon_ancestry"].apply(
        lambda x: _get_dict_taxon(x, df_taxon)
    )

    for level in ["kingdom", "phylum", "class", "order", "family", "genus"]:
        df_obs[level] = df_obs.taxon_ancestry.str.get(level)

    df_obs.drop(columns=["taxon_ancestry"], inplace=True)


def _get_dict_taxon(ancestry_string, df_taxon):
    try:
        data = {}
        list_ancestries = ancestry_string.split("/")
        for ancestry in list_ancestries:
            if int(ancestry) != 1:
                try:
                    rank = df_taxon[df_taxon["id"] == int(ancestry)]["rank"].item()
                    name = df_taxon[df_taxon["id"] == int(ancestry)]["name"].item()
                    data[rank] = name
                except:
                    # para rangos intermedios, no los incluye
                    continue
    except:
        data = None

    return data


def extra_info(df_observations) -> pd.DataFrame:
    """
    Function to obtain extra information of each observation of a selection
    (very expensive at the API level)
    """
    ids = df_observations["id"].to_list()
    dic = {}

    for id_num in ids:
        url = f"{API_URL}/observations/{id_num}.json"
        page = requests.get(url)

        idents = page.json()["identifications"]
        if len(idents) > 0:
            user_identification = idents[0]["user"]["login"]
            first_taxon_name = idents[0]["taxon"]["name"]
            last_taxon_name = idents[len(idents) - 1]["taxon"]["name"]
            dic[id_num] = [user_identification, first_taxon_name, last_taxon_name]
        else:
            dic[id_num] = [0, 0, 0]

    df_observations["first_identification"] = df_observations["id"].apply(
        lambda x: str(dic[x][0])
    )
    df_observations["first_taxon_name"] = df_observations["id"].apply(
        lambda x: str(dic[x][1])
    )
    df_observations["last_taxon_name"] = df_observations["id"].apply(
        lambda x: str(dic[x][2])
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
    Function to download the photos resulting from the query.
    """
    # Create the folder, if it exists overwrite it
    if os.path.exists(directorio):
        shutil.rmtree(directorio)
    os.makedirs(directorio)

    # Iterate through the df_photos query result and download the photos in medium size
    for i, row in df_photos.iterrows():
        response = requests.get(row["photos.medium_url"], stream=True)
        if response.status_code == 200:
            with open(f"{directorio}/{row['path']}", "wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)
        del response

    # Even using .loc, we get a SettingWithCopyWarning message
    df_photos.loc[:, "abs_path"] = os.path.abspath(f"{directorio}/{df_photos['path']}")


def get_count_by_taxon() -> Dict:
    """
    Function that returns the number of observations recorded for each taxonomic family.
    """
    url = f"{API_URL}/taxa.json"
    page = requests.get(url)
    taxa = page.json()
    count = {}
    for taxon in taxa:
        count[taxon["name"]] = taxon["observations_count"]
    return count


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
        df = pd.read_xml(url, parser="etree")
        df_total = pd.concat([df_total, df])

    # clean fields
    df_total["institutionCode"] = "Minka"
    df_total["datasetName"] = df_total["datasetName"].str.replace(
        "iNaturalist", "Minka"
    )

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
            df = pd.read_xml(url, parser="etree")
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
    base_url = f"{API_URL}/observations.dwc"

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

    url = f'{base_url}?{"&".join(args)}&per_page=200'

    # if no parameter indicated, it returns the last records

    return url
