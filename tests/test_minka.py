#!/usr/bin/env python3
# -*- coding: iso-8859-15 -*-

import datetime

import pandas as pd
import pytest
from pydantic_core._pydantic_core import TzInfo

from mecoda_minka import (
    ICONIC_TAXON,
    TAXONS,
    Observation,
    Photo,
    Project,
    get_count_by_taxon,
    get_dfs,
    get_dwc,
    get_dwc_from_query,
    get_obs,
    get_project,
)

BASE_URL = "https://minka-sdg.org"
API_URL = "https://api.minka-sdg.org/v1"


def test_get_project_from_id_extract_project_data():
    expected_result = Project(
        id=20,
        title="BioMARató 2022 (Catalunya)",
        description="La BioMARató es una manera divertida...",
        created_at=datetime.datetime(
            2022, 4, 19, 14, 24, 30, 290000, tzinfo=datetime.timezone.utc
        ),
        updated_at=datetime.datetime(
            2022, 10, 30, 11, 58, 26, 375000, tzinfo=datetime.timezone.utc
        ),
        latitude=None,
        longitude=None,
        parent_id=None,
        children_id=[],
        user_id=4,
        icon_url="/attachments/projects/icons/20/span2/icon.png?1650378269",
        observed_taxa_count=0,
    )

    result = get_project(20)

    assert result[0].id == 20
    assert result[0].created_at.year == 2022
    assert "Catalunya" in result[0].title


def test_get_project_from_not_found_id_raise_error(requests_mock, capsys):
    requests_mock.get(
        f"{BASE_URL}/projects/11.json",
        json={"error": "No encontrado"},
        status_code=404,
    )
    get_project(11)
    out, err = capsys.readouterr()
    assert "Project ID not found" in out


def test_get_project_from_str_extract_project_data(requests_mock):
    expected_result = [
        Project(
            id=806, title="urbamar", description="Proyecto de búsqueda de lampardos"
        )
    ]
    requests_mock.get(
        f"{BASE_URL}/projects/search.json?q=urbamar",
        json=[
            {
                "id": 806,
                "title": "urbamar",
                "description": "Proyecto de búsqueda de lampardos",
            }
        ],
    )
    result = get_project("urbamar")

    assert result == expected_result


def test_get_project_from_ambiguous_str_extract_project_data(requests_mock):
    expected_result = [
        Project(
            id=806, title="urbamar", description="Proyecto de búsqueda de lampardos"
        ),
        Project(
            id=23, title="ruramar", description="Proyecto de búsqueda de parlamdos"
        ),
    ]
    requests_mock.get(
        f"{BASE_URL}/projects/search.json?q=mar",
        json=[
            {
                "id": 806,
                "title": "urbamar",
                "description": "Proyecto de búsqueda de lampardos",
            },
            {
                "id": 23,
                "title": "ruramar",
                "description": "Proyecto de búsqueda de parlamdos",
            },
        ],
    )
    result = get_project("mar")

    assert result == expected_result


def test_get_obs_by_id_returns_observations_data(
    requests_mock,
):
    expected_result = [
        Observation(
            id=2084,
            captive=False,
            created_at=datetime.datetime(
                2016,
                7,
                11,
                16,
                10,
                39,
            ),
            updated_at=datetime.datetime(
                2016,
                7,
                28,
                10,
                44,
                44,
            ),
            observed_on=datetime.datetime(2016, 7, 6, 0, 0),
            description="",
            iconic_taxon="chromista",
            taxon_id=2850,
            taxon_name="Rissoella verruculosa",
            taxon_rank="species",
            taxon_ancestry=None,
            location="41.773743,3.021853",
            # latitude="41.773743",
            # longitude="3.021853",
            quality_grade="research",
            user_id=626,
            user_login="amxatrac",
            photos=[
                Photo(
                    id=1975,
                    large_url=f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                    medium_url=f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                    small_url=f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242",
                ),
                Photo(
                    id=2075,
                    large_url=f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                    medium_url=f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                    small_url=f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242",
                ),
            ],
            num_identification_agreements=3,
            num_identification_disagreements=0,
        )
    ]
    requests_mock.get(
        f"{API_URL}/observations?id=2084&per_page=200",
        json={
            "total_results": 1,
            "page": 1,
            "per_page": 30,
            "results": [
                {
                    "id": 2084,
                    "captive": "false",
                    "created_at": "2016-07-11T16:10:39",
                    "updated_at": "2016-07-28T10:44:44",
                    "observed_on": "2016-07-06",
                    "description": "",
                    "iconic_taxon_id": 16,
                    "taxon": {
                        "id": 2850,
                        "name": "Rissoella verruculosa",
                        "rank": "species",
                        "ancestry": None,
                    },
                    "latitude": "41.773743",
                    "longitude": "3.021853",
                    "quality_grade": "research",
                    "user_id": 626,
                    "user_login": "amxatrac",
                    "project_observations": [
                        {"project_id": 104, "id": 1079, "observation_id": 2084},
                        {"project_id": 121, "id": 1329, "observation_id": 2084},
                    ],
                    "photos": [
                        {
                            "id": 1975,
                            "large_url": f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                            "medium_url": f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                            "small_url": f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242",
                        },
                        {
                            "id": 2075,
                            "large_url": f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                            "medium_url": f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                            "small_url": f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242",
                        },
                    ],
                    "num_identification_agreements": 3,
                    "num_identification_disagreements": 0,
                }
            ],
        },
    )

    result = get_obs(id_obs=2084)

    assert result == expected_result


def test_get_obs_from_query_returns_observations_data_when_less_than_pagination(
    requests_mock,
):
    expected_result = [
        Observation(
            id=id,
            iconic_taxon="animalia",
            created_at=datetime.datetime(2021, 3, 15, 16, 10, 39, tzinfo=TzInfo(7200)),
        )
        for id in range(3)
    ]
    requests_mock.get(
        f"{API_URL}/observations?q=%22quercus%20quercus%22&per_page=200",
        json={
            "total_results": 147,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": id,
                    "taxon": {"iconic_taxon_id": 2},
                    "created_at": "2021-03-15T16:10:39+02:00",
                }
                for id in range(3)
            ],
        },
    )

    result = get_obs(query="quercus quercus")

    assert result == expected_result
    assert len(result) == 3


def test_get_obs_returns_observations_data_when_more_than_pagination(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            id=id,
            description="Pavo real en su hábitat natural",
            observed_on=datetime.date(2021, 3, 15),
        )
        for id in range(250)
    ]

    requests_mock.get(
        f'{API_URL}/observations?q="quercus quercus"&per_page=200',
        json={
            "total_results": 250,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": id_,
                    "description": "Pavo real en su hábitat natural",
                    "observed_on": "2021-03-15",
                }
                for id_ in range(200)
            ],
        },
    )
    requests_mock.get(
        f'{API_URL}/observations?q="quercus quercus"&per_page=200&page=2',
        json={
            "total_results": 250,
            "page": 2,
            "per_page": 200,
            "results": [
                {
                    "id": id_,
                    "description": "Pavo real en su hábitat natural",
                    "observed_on": "2021-03-15",
                }
                for id_ in range(200, 250)
            ],
        },
    )
    requests_mock.get(
        f'{API_URL}/observations?q="quercus quercus"&per_page=200&page=100',
        json=[],
    )

    result = get_obs(query="quercus quercus")

    assert result == expected_result
    assert len(result) == 250


def test_get_obs_from_user_returns_observations_data(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            id=id_,
            user_id=425,
        )
        for id_ in range(260)
    ]
    requests_mock.get(
        f"{API_URL}/observations?user_login=zolople&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "user_id": 425,
                    "id": id_,
                }
                for id_ in range(200)
            ],
        },
    )
    requests_mock.get(
        f"{API_URL}/observations?user_login=zolople&per_page=200&page=2",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "user_id": 425,
                    "id": id_,
                }
                for id_ in range(200, 260)
            ],
        },
    )

    result = get_obs(user="zolople")

    assert result == expected_result
    assert len(result) == 260


def test_get_obs_project_returns_observations_data(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            id=1,
            user_id=id_,
            iconic_taxon="amphibia",
            taxon_id=481,
            taxon_rank="genus",
            taxon_name="Hedera",
            taxon_ancestry=None,
        )
        for id_ in range(37)
    ]

    requests_mock.get(
        f"{API_URL}/observations?project_id=20&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": 1,
                    "user_id": id_,
                    "iconic_taxon_id": 7,
                    "taxon": {
                        "id": 481,
                        "rank": "genus",
                        "name": "Hedera",
                        "ancestry": None,
                    },
                }
                for id_ in range(37)
            ],
        },
    )
    result = get_obs(id_project=20)

    assert result == expected_result
    assert len(result) == 37


def test_get_project_from_name_returns_observations_data(
    requests_mock,
) -> None:
    expected_result = [
        Project(
            id=1191,
            latitude=41.403373,
            longitude=2.216873,
            updated_at=datetime.datetime(
                2020,
                9,
                26,
                17,
                7,
                36,
                tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)),
            ),
            description="Urbamar és un projecte de ciència ciutadana.",
            title="URBAMAR",
            icon_url=f"{BASE_URL}/attachments/projects/icons/1191/span2/Ilustracio%CC%81n-sin-ti%CC%81tulo.png?1595350663",
            observed_taxa_count=0,
        )
    ]

    requests_mock.get(
        f"{BASE_URL}/projects/search.json?q=urbamar",
        json=[
            {
                "id": 1191,
                "latitude": "41.403373",
                "longitude": "2.216873",
                "updated_at": "2020-09-26T17:07:36+02:00",
                "title": "URBAMAR",
                "description": "Urbamar és un projecte de ciència ciutadana.",
                "icon_url": f"{BASE_URL}/attachments/projects/icons/1191/span2/Ilustracio%CC%81n-sin-ti%CC%81tulo.png?1595350663",
                "observed_taxa_count": 0,
            }
        ],
    )

    result = get_project("urbamar")
    assert result == expected_result


def test_get_obs_from_taxon_returns_info_with_pagination(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            iconic_taxon="fungi",
            id=313430,
            taxon_id=39432,
            taxon_rank="species",
            taxon_name="Cheilymenia theleboloides",
            taxon_ancestry=None,
            updated_at=datetime.datetime(
                2021,
                7,
                12,
                23,
                36,
                48,
                tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)),
            ),
        )
        for i in range(456)
    ]

    requests_mock.get(
        f"{API_URL}/observations?iconic_taxa=Fungi&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "iconic_taxon_id": 13,
                    "id": 313430,
                    "taxon": {
                        "id": 39432,
                        "rank": "species",
                        "name": "Cheilymenia theleboloides",
                        "ancestry": None,
                    },
                    "updated_at": "2021-07-12T23:36:48+02:00",
                }
                for id_ in range(200)
            ],
        },
    )
    requests_mock.get(
        f"{API_URL}/observations?iconic_taxa=Fungi&per_page=200&page=2",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "iconic_taxon_id": 13,
                    "id": 313430,
                    "taxon": {
                        "id": 39432,
                        "rank": "species",
                        "name": "Cheilymenia theleboloides",
                        "ancestry": None,
                    },
                    "updated_at": "2021-07-12T23:36:48+02:00",
                }
                for id_ in range(200)
            ],
        },
    )
    requests_mock.get(
        f"{API_URL}/observations?iconic_taxa=Fungi&per_page=200&page=3",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "iconic_taxon_id": 13,
                    "id": 313430,
                    "taxon": {
                        "id": 39432,
                        "rank": "species",
                        "name": "Cheilymenia theleboloides",
                        "ancestry": None,
                    },
                    "updated_at": "2021-07-12T23:36:48+02:00",
                }
                for id_ in range(56)
            ],
        },
    )
    result = get_obs(taxon="Fungi")

    assert result == expected_result
    assert len(result) == 456


def test_get_obs_from_place_id_returns_obs(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            iconic_taxon="actinopterygii",
            id=1645,
            user_login="andrea",
            taxon_id=2948,
            taxon_name="Holothuria",
            taxon_rank="genus",
            taxon_ancestry=None,
            created_at=datetime.datetime(
                2021,
                8,
                15,
                19,
                43,
                43,
            ),
        )
        for i in range(456)
    ]

    requests_mock.get(
        f"{API_URL}/observations?place_id=1011&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "taxon": {
                        "id": 2948,
                        "name": "Holothuria",
                        "rank": "genus",
                        "ancestry": None,
                    },
                    "id": 1645,
                    "iconic_taxon_id": 3,
                    "user_login": "andrea",
                    "created_at": "2021-08-15T19:43:43",
                }
                for id_ in range(200)
            ],
        },
    )
    requests_mock.get(
        f"{API_URL}/observations?place_id=1011&per_page=200&page=2",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "taxon": {
                        "id": 2948,
                        "name": "Holothuria",
                        "rank": "genus",
                        "ancestry": None,
                    },
                    "id": 1645,
                    "iconic_taxon_id": 3,
                    "user_login": "andrea",
                    "created_at": "2021-08-15T19:43:43",
                }
                for id_ in range(200)
            ],
        },
    )
    requests_mock.get(
        f"{API_URL}/observations?place_id=1011&per_page=200&page=3",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "taxon": {
                        "id": 2948,
                        "name": "Holothuria",
                        "rank": "genus",
                        "ancestry": None,
                    },
                    "id": 1645,
                    "iconic_taxon_id": 3,
                    "user_login": "andrea",
                    "created_at": "2021-08-15T19:43:43",
                }
                for id_ in range(56)
            ],
        },
    )
    result = get_obs(place_id=1011)
    # __import__("pdb").set_trace()
    assert result == expected_result
    assert len(result) == 456


# test de uso de la función con taxon en minúsculas
def test_get_obs_from_taxon_min_returns_info(
    requests_mock,
) -> None:
    requests_mock.get(
        f"{API_URL}/observations?iconic_taxa=Fungi&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": 1645,
                    "iconic_taxon_id": 13,
                }
                for id_ in range(57)
            ],
        },
    )

    result = get_obs(taxon="fungi")

    assert len(result) == 57


# test de usos combinados
def test_get_obs_from_combined_arguments(
    requests_mock,
) -> None:
    requests_mock.get(
        f"{API_URL}/observations?user_login=zolople&iconic_taxa=Mollusca&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": id_,
                }
                for id_ in range(5)
            ],
        },
    )
    result = get_obs(taxon="Mollusca", user="zolople")

    assert len(result) == 5


# test combinado id_project, place_id, query
def test_get_obs_from_three_combined_arguments(
    requests_mock,
) -> None:
    requests_mock.get(
        f'{API_URL}/observations?project_id=45&place_id=3&q="quercus quercus"&per_page=200',
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {"id": 4586, "project": 45, "place": 3, "species": "quercus quercus"},
                {"id": 4588, "project": 45, "place": 3, "species": "quercus quercus"},
            ],
        },
    )
    result = get_obs(id_project=45, place_id=3, query="quercus quercus")

    assert len(result) == 2


def test_get_obs_from_fake_taxon() -> None:
    with pytest.raises(ValueError):
        get_obs(taxon="inexistente")


def test_get_count_by_taxon_returns_info(
    requests_mock,
) -> None:
    requests_mock.get(
        f"{BASE_URL}/taxa.json",
        json=[
            {
                "name": "Fungi",
                "observations_count": 7883,
            },
            {
                "name": "Protozoa",
                "observations_count": 123,
            },
            {
                "name": "Chromista",
                "observations_count": 1375,
            },
            {
                "name": "Animalia",
                "observations_count": 107108,
            },
        ],
    )
    result = get_count_by_taxon()

    assert len(result) == 4
    assert result["Chromista"] == 1375


def test_get_obs_from_year_returns_obs(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            id=id_,
        )
        for id_ in range(150)
    ]
    requests_mock.get(
        f"{API_URL}/observations?year=2018&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": id_,
                }
                for id_ in range(150)
            ],
        },
    )
    result = get_obs(year=2018)

    assert result == expected_result
    assert len(result) == 150


def test_get_obs_with_num_max(
    requests_mock,
) -> None:
    expected_result = [
        Observation(
            id=id_,
        )
        for id_ in range(10)
    ]
    requests_mock.get(
        f"{API_URL}/observations?iconic_taxa=Fungi&per_page=200",
        json={
            "total_results": 900,
            "page": 1,
            "per_page": 200,
            "results": [
                {
                    "id": id_,
                }
                for id_ in range(200)
            ],
        },
    )
    result = get_obs(taxon="fungi", num_max=10)

    assert result == expected_result
    assert len(result) == 10


def test_get_dfs_extrae_dfs() -> None:
    observations = [
        Observation(
            id=98441,
            captive=False,
            created_at=datetime.datetime(
                2022, 10, 27, 15, 21, 52, 503000, tzinfo=datetime.timezone.utc
            ),
            updated_at=datetime.datetime(
                2022, 10, 28, 8, 37, 38, 904000, tzinfo=datetime.timezone.utc
            ),
            observed_on=datetime.date(2020, 12, 24),
            description=None,
            iconic_taxon="animalia",
            taxon_id=372,
            taxon_name="Amphipoda",
            taxon_ancestry="1/2/10/118",
            latitude=42.0138450901,
            longitude=3.2169726918,
            place_name="Spain",
            quality_grade="needs_id",
            user_id=4,
            user_login="xasalva",
            photos=[
                Photo(
                    id=119257,
                    large_url=f"{BASE_URL}/attachments/local_photos/files/119257/large/D72_7339.jpeg?1666884089",
                    medium_url=f"{BASE_URL}/attachments/local_photos/files/119257/medium/D72_7339.jpeg?1666884089",
                    small_url=f"{BASE_URL}/attachments/local_photos/files/119257/small/D72_7339.jpeg?1666884089",
                )
            ],
            num_identification_agreements=0,
            num_identification_disagreements=0,
            identifications_count=2,
            id_please=False,
        )
    ]

    expected_result_obs = pd.DataFrame(
        [
            {
                "id": 1,
                "captive": None,
                "created_at": None,
                "updated_at": None,
                "observed_on": None,
                "description": None,
                "iconic_taxon": "animalia",
                "ancestry": "1/2/4/3/343/1409/35511",
                "taxon_id": None,
                "taxon_name": "Thalassoma pavo",
                "taxon_ancestry": None,
                "latitude": 40.1,
                "longitude": -7.5,
                "place_name": None,
                "quality_grade": None,
                "user_id": None,
                "user_login": "joselu_00",
                "num_identification_agreements": None,
                "num_identification_disagreements": None,
            }
        ]
    )

    result_obs, result_photo = get_dfs(observations)
    assert type(result_obs) == pd.DataFrame
    assert (len(result_obs)) == len(observations)
    assert result_obs["id"].values != None


def test_get_taxon_columns() -> None:
    observations = [
        Observation(
            id=98441,
            captive=False,
            created_at=datetime.datetime(
                2022, 10, 27, 15, 21, 52, 503000, tzinfo=datetime.timezone.utc
            ),
            updated_at=datetime.datetime(
                2022, 10, 28, 8, 37, 38, 904000, tzinfo=datetime.timezone.utc
            ),
            observed_on=datetime.date(2020, 12, 24),
            description=None,
            iconic_taxon="animalia",
            taxon_id=372,
            taxon_name="Amphipoda",
            taxon_ancestry="1/2/10/118",
            latitude=42.0138450901,
            longitude=3.2169726918,
            place_name="Spain",
            quality_grade="needs_id",
            user_id=4,
            user_login="xasalva",
            photos=[
                Photo(
                    id=119257,
                    large_url=f"{BASE_URL}/attachments/local_photos/files/119257/large/D72_7339.jpeg?1666884089",
                    medium_url=f"{BASE_URL}/attachments/local_photos/files/119257/medium/D72_7339.jpeg?1666884089",
                    small_url=f"{BASE_URL}/attachments/local_photos/files/119257/small/D72_7339.jpeg?1666884089",
                )
            ],
            num_identification_agreements=0,
            num_identification_disagreements=0,
            identifications_count=2,
            id_please=False,
        )
    ]

    df_obs, df_photos = get_dfs(observations)
    assert type(df_obs) == pd.DataFrame
    assert df_obs["class"].item() == "Malacostraca"


def test_get_dwc() -> None:
    observations = [
        Observation(id=100),
        Observation(id=101),
        Observation(id=102),
    ]
    result = get_dwc(observations)

    assert len(result) == 3
    assert type(result) == pd.DataFrame

    assert len(result.columns) == 35
    assert result["institutionCode"].iloc[0] == "Minka"


def test_get_dwc_from_query() -> None:
    result = get_dwc_from_query(
        user_id=4,  # xasalva
        taxon_id=4,  # filo chordata
        place_id=55,  # spain
        start_on="2022-10-01",
        ends_on="2022-10-10",
    )
    assert len(result) > 70
    assert type(result) == pd.DataFrame
    assert len(result.columns) >= 33
    assert result["institutionCode"].iloc[0] == "Minka"


# correctly converts observations to DataFrame
def test_correctly_converts_observations_to_dataframe():
    observations = [
        Observation(
            id=98441,
            captive=False,
            created_at=datetime.datetime(
                2022, 10, 27, 15, 21, 52, 503000, tzinfo=datetime.timezone.utc
            ),
            updated_at=datetime.datetime(
                2022, 10, 28, 8, 37, 38, 904000, tzinfo=datetime.timezone.utc
            ),
            observed_on=datetime.date(2020, 12, 24),
            description=None,
            iconic_taxon="animalia",
            taxon_id=372,
            taxon_name="Amphipoda",
            taxon_ancestry="1/2/10/118",
            latitude=42.0138450901,
            longitude=3.2169726918,
            place_name="Spain",
            quality_grade="needs_id",
            user_id=4,
            user_login="xasalva",
            photos=[
                Photo(
                    id=119257,
                    large_url=f"{BASE_URL}/attachments/local_photos/files/119257/large/D72_7339.jpeg?1666884089",
                    medium_url=f"{BASE_URL}/attachments/local_photos/files/119257/medium/D72_7339.jpeg?1666884089",
                    small_url=f"{BASE_URL}/attachments/local_photos/files/119257/small/D72_7339.jpeg?1666884089",
                )
            ],
            num_identification_agreements=0,
            num_identification_disagreements=0,
            identifications_count=2,
            id_please=False,
        )
    ]

    result_obs, result_photo = get_dfs(observations)

    assert type(result_obs) == pd.DataFrame
    assert len(result_obs) == len(observations)
    assert "id" in result_obs.columns
