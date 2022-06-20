#!/usr/bin/env python3

import datetime
import pytest
import pandas as pd
from mecoda_minka import (
    get_project,
    get_obs,
    get_count_by_taxon,
    get_dfs,
    get_taxon_columns,
    Project,
    Observation,
    Photo,
    TAXONS,
    ICONIC_TAXON
)
import requests

API_URL = "https://minka-sdg.org"

def test_get_project_from_id_extract_project_data(requests_mock):
    expected_result = Project(
        id=806, 
        title="título", 
        description='Proyecto de búsqueda de lampardos'
        )
    requests_mock.get(
        f'{API_URL}/projects/806.json',
        json = expected_result.dict()
    )
    result = get_project(806)

    assert result == [expected_result]

# Meter request_mock
def test_get_project_from_not_found_id_raise_error(requests_mock, capsys):
    requests_mock.get(
        f'{API_URL}/projects/11.json',
        json = {"error":"No encontrado"},
        status_code = 404
    )
    get_project(11)
    out, err = capsys.readouterr()
    assert "ID No encontrado" in out

def test_get_project_from_str_extract_project_data(requests_mock):
    expected_result = [Project(id=806, title="urbamar", description='Proyecto de búsqueda de lampardos')]
    requests_mock.get(
        f'{API_URL}/projects/search.json?q=urbamar',
        json = [{
            "id": 806,
            "title": "urbamar",
            "description": "Proyecto de búsqueda de lampardos"
        }]
    )
    result = get_project("urbamar")

    assert result == expected_result

def test_get_project_from_ambiguous_str_extract_project_data(requests_mock):
    expected_result = [
        Project(
            id=806, 
            title="urbamar", 
            description='Proyecto de búsqueda de lampardos'),
        Project(
            id=23, 
            title="ruramar", 
            description='Proyecto de búsqueda de parlamdos'),
    ]
    requests_mock.get(
        f'{API_URL}/projects/search.json?q=mar',
        json = [{
            "id": 806,
            "title": "urbamar",
            "description": "Proyecto de búsqueda de lampardos"
        },
        {
            "id": 23,
            "title": "ruramar",
            "description": "Proyecto de búsqueda de parlamdos"
        }]
    )
    result = get_project("mar")

    assert result == expected_result


def test_get_obs_by_id_returns_observations_data(requests_mock,):
    expected_result = [Observation(
        id=2084,
        captive=False,
        created_at=datetime.datetime(2016, 7, 11, 16, 10, 39, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200))),
        updated_at=datetime.datetime(2016, 7, 28, 10, 44, 44, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200))),
        observed_on=datetime.datetime(2016, 7, 6, 0, 0),
        description="",
        iconic_taxon="chromista",
        taxon_id=2850, 
        taxon_name="Rissoella verruculosa",
        taxon_ancestry=None,
        latitude="41.773743",
        longitude="3.021853",
        quality_grade="research",
        user_id=626,
        user_login="amxatrac",
        photos=[
            Photo(
                id=1975,
                large_url=f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                medium_url=f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                small_url=f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242"
                ),
            Photo(
                id=2075,
                large_url=f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                medium_url=f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                small_url=f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242"
                ),
            ],
        num_identification_agreements=3,
        num_identification_disagreements=0,
    )]
    requests_mock.get(
        f"{API_URL}/observations/2084.json",
        json = {
            "id": 2084,
            "captive": "false",
            "created_at": "2016-07-11T16:10:39+02:00",
            "updated_at": "2016-07-28T10:44:44+02:00",
            "observed_on": "2016-07-06",
            "description": "",
            "iconic_taxon_id": 16,
            "taxon": {"id": 2850, "name": "Rissoella verruculosa", "ancestry": None},
            "latitude": "41.773743",
            "longitude": "3.021853",
            "quality_grade": "research",
            "user_id": 626,
            "user_login": "amxatrac", 
            "project_observations": [
                {"project_id": 104, "id": 1079, "observation_id": 2084},
                {"project_id": 121, "id": 1329, "observation_id": 2084}
                ],
            "photos": [
                {
                    "id": 1975, 
                    "large_url": f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                    "medium_url": f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                    "small_url": f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242"},
                {
                    "id": 2075, 
                    "large_url": f"{API_URL}/attachments/local_photos/files/2947/large/rissoella_verruculosa.JPG?1468246242",
                    "medium_url": f"{API_URL}/attachments/local_photos/files/2947/medium/rissoella_verruculosa.JPG?1468246242",
                    "small_url": f"{API_URL}/attachments/local_photos/files/2947/small/rissoella_verruculosa.JPG?1468246242"},
                ],
            "num_identification_agreements": 3,
            "num_identification_disagreements": 0
            }
        )

    result = get_obs(id_obs=2084)
    
    assert result == expected_result
    


def test_get_obs_from_query_returns_observations_data_when_less_than_pagination(requests_mock):
    expected_result = [Observation(
        id=id, 
        iconic_taxon="animalia",
        created_at=datetime.datetime(2021, 3, 15, 16, 10, 39, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))
        ) for id in range(3)]
    requests_mock.get(
        f'{API_URL}/observations.json?q="quercus quercus"&per_page=200',
        json=[{
            "id": id, 
            "iconic_taxon_id": 2, 
            "created_at": "2021-03-15T16:10:39+02:00"} for id in range(3)],
    )

    result = get_obs(query="quercus quercus")
    
    assert result == expected_result
    assert len(result) == 3
    
    
def test_get_obs_returns_observations_data_when_more_than_pagination(
    requests_mock,
    ) -> None:

    expected_result = [Observation(
        id=id,
        description="Pavo real en su hábitat natural",
        observed_on=datetime.date(2021, 3, 15)) for id in range(250)
    ]

    requests_mock.get(
        f'{API_URL}/observations.json?q="quercus quercus"&per_page=200',
        json=[{
            "id": id_, 
            "description": "Pavo real en su hábitat natural",
            "observed_on": "2021-03-15"} for id_ in range(200)],
    )
    requests_mock.get(
        f'{API_URL}/observations.json?q="quercus quercus"&per_page=200&page=2',
        json=[{
            "id": id_, 
            "description": "Pavo real en su hábitat natural",
            "observed_on": "2021-03-15"} for id_ in range(200, 250)],
    )
    requests_mock.get(
        f'{API_URL}/observations.json?q="quercus quercus"&per_page=200&page=100',
        json=[],
    )

    result = get_obs(query="quercus quercus")

    assert result == expected_result
    assert len(result) == 250


def test_get_obs_returns_error_when_more_than_20000_results(
    requests_mock,
    capsys,
    ) -> None:
    """The API will return an error."""
    requests_mock.get(
        f'{API_URL}/observations.json?q="quercus quercus"&per_page=200',
        json=[
            {"id": id_, "iconic_taxon_id": 3}
            for id_ in range(0, 200)
        ],
    )
    for page in range(2, 100):
        requests_mock.get(
            f'{API_URL}/observations.json?q="quercus quercus"&per_page=200&page={page}',
            json=[
                {"id": id_, "iconic_taxon_id": 3}
                for id_ in range(200 * (page - 1), 200 * page)
            ],
        )
    requests_mock.get(
        f'{API_URL}/observations.json?q="quercus quercus"&per_page=200&page=101',
        json={"message": "You reach 20,000 items limit"},
    )

    result = get_obs("quercus quercus")

    # captura el mensaje de error que aparece en el output
    out, err = capsys.readouterr()
    
    assert len(result) == 20000
    assert "WARNING: Only the first 20,000 results are displayed" in out
    
    
def test_get_obs_from_user_returns_observations_data(requests_mock,) -> None:
    expected_result = [Observation(
        id=id_,
        user_id=425,) for id_ in range(260)
    ]
    requests_mock.get(
        f"{API_URL}/observations/zolople.json?per_page=200",
        json=[
            {"user_id": 425, 
            "id": id_,} for id_ in range(200)],
    )
    requests_mock.get(
        f"{API_URL}/observations/zolople.json?per_page=200&page=2",
        json=[{
            "user_id": 425,
            "id": id_, } for id_ in range(200, 260)],
    )
    
    result = get_obs(user="zolople")
    
    assert result == expected_result
    assert len(result) == 260


def test_get_obs_project_returns_observations_data(requests_mock,) -> None:
    expected_result = [Observation(
        id=1,
        user_id=id_,
        iconic_taxon="amphibia",
        taxon_id=481,
        taxon_name="Hedera",
        taxon_ancestry=None
        ) for id_ in range(37)
    ]
    
    requests_mock.get(
        f"{API_URL}/observations/project/806.json?per_page=200",
        json=[
            {"id": 1,
            "user_id": id_, 
            "iconic_taxon_id": 7,
            "taxon": {
                "id": 481, 
                "name": "Hedera",
                "ancestry": None}
            } for id_ in range(37)
        ],
    )
    result = get_obs(id_project=806)

    assert result == expected_result
    assert len(result) == 37

def test_get_project_from_name_returns_observations_data(requests_mock,) -> None:
    expected_result = [Project(
        id=1191,
        latitude=41.403373,
        longitude=2.216873,
        updated_at=datetime.datetime(2020, 9, 26, 17, 7, 36, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200))),
        description="Urbamar és un projecte de ciència ciutadana.",
        title="URBAMAR",
        icon_url=f"{API_URL}/attachments/projects/icons/1191/span2/Ilustracio%CC%81n-sin-ti%CC%81tulo.png?1595350663",
        observed_taxa_count=0
        )]
    

    requests_mock.get(
        f"{API_URL}/projects/search.json?q=urbamar",
        json=[{
            'id': 1191,
            'latitude': '41.403373',
            'longitude': '2.216873',
            'updated_at': "2020-09-26T17:07:36+02:00",
            'title': 'URBAMAR',
            'description': "Urbamar és un projecte de ciència ciutadana.",
            'icon_url': f"{API_URL}/attachments/projects/icons/1191/span2/Ilustracio%CC%81n-sin-ti%CC%81tulo.png?1595350663",
            'observed_taxa_count': 0
        }]
    )

    result = get_project('urbamar')
    assert result == expected_result

def test_get_obs_from_taxon_returns_info_with_pagination(requests_mock,) -> None:
    expected_result = [Observation(
        iconic_taxon="fungi",
        id=313430,
        taxon_id=39432, 
        taxon_name="Cheilymenia theleboloides", 
        taxon_ancestry=None,
        updated_at=datetime.datetime(2021, 7, 12, 23, 36, 48, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))
    ) for i in range(456)]

    requests_mock.get(
        f"{API_URL}/observations.json?iconic_taxa=Fungi&per_page=200",
        json=[{
            "iconic_taxon_id": 13, 
            "id": 313430, 
            "taxon": {"id": 39432, "name": "Cheilymenia theleboloides", "ancestry": None},
            'updated_at': "2021-07-12T23:36:48+02:00",} for id_ in range(200)
        ]
    )
    requests_mock.get(
        f"{API_URL}/observations.json?iconic_taxa=Fungi&per_page=200&page=2",
        json=[{
            "iconic_taxon_id": 13, 
            "id": 313430, 
            "taxon": {"id": 39432, "name": "Cheilymenia theleboloides", "ancestry": None},
            'updated_at': "2021-07-12T23:36:48+02:00",} for id_ in range(200)
        ]
    )
    requests_mock.get(
        f"{API_URL}/observations.json?iconic_taxa=Fungi&per_page=200&page=3",
        json=[{
            "iconic_taxon_id": 13, 
            "id": 313430, 
            "taxon": {"id": 39432, "name": "Cheilymenia theleboloides", "ancestry": None},
            'updated_at': "2021-07-12T23:36:48+02:00",} for id_ in range(56)
        ]
    )
    result = get_obs(taxon='Fungi')
    
    assert result == expected_result
    assert len(result) == 456


def test_get_obs_from_place_id_returns_obs(requests_mock,) -> None:
    expected_result = [Observation(
        iconic_taxon="actinopterygii",
        id=1645,
        user_login="andrea",
        taxon_id=2948, 
        taxon_name="Holothuria", 
        taxon_ancestry=None,
        created_at=datetime.datetime(2021, 8, 15, 19, 43, 43, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))
    ) for i in range(456)]

    requests_mock.get(
        f"{API_URL}/observations.json?place_id=1011&per_page=200",
        json=[{
            "taxon": {"id": 2948, "name": "Holothuria", "ancestry": None}, 
            "id": 1645, 
            "iconic_taxon_id": 3,
            "user_login": "andrea",
            'created_at': "2021-08-15T19:43:43+02:00",} for id_ in range(200)
            ]
    )
    requests_mock.get(
        f"{API_URL}/observations.json?place_id=1011&per_page=200&page=2",
        json=[{
            "taxon": {"id": 2948, "name": "Holothuria", "ancestry": None}, 
            "id": 1645, 
            "iconic_taxon_id": 3,
            "user_login": "andrea",
            'created_at': "2021-08-15T19:43:43+02:00",} for id_ in range(200)
            ]
    )
    requests_mock.get(
        f"{API_URL}/observations.json?place_id=1011&per_page=200&page=3",
        json=[{
            "taxon": {"id": 2948, "name": "Holothuria", "ancestry": None}, 
            "id": 1645, 
            "iconic_taxon_id": 3,
            "user_login": "andrea",
            'created_at': "2021-08-15T19:43:43+02:00",} for id_ in range(56)
            ]
    )
    result = get_obs(place_id=1011)
    
    assert result == expected_result
    assert len(result) == 456

def test_get_obs_from_place_name_returns_obs(requests_mock,) -> None:
    requests_mock.get(
        f"{API_URL}/places.json?q=Barcelona",
        json=[{'id': 20}, {'id': 67}, {'id': 1024}]
        )
    requests_mock.get(
        f"{API_URL}/observations.json?place_id=20&per_page=200",
        json=[{
            "id": id_, 
            'updated_at': '2020-09-26T05:07:36-10:00',} for id_ in range(200)]
        )
    requests_mock.get(
        f"{API_URL}/observations.json?place_id=20&per_page=200&page=2",
        json=[{
            "id": id_, 
            'updated_at': '2020-09-26T05:07:36-10:00',} for id_ in range(56)]
        )
    requests_mock.get(
        f"{API_URL}/observations.json?place_id=67&per_page=200",
        json=[]
        )
    requests_mock.get(
        f"{API_URL}/observations.json?place_id=1024&per_page=200",
        json=[{
            "id": id_, 
            'updated_at': '2020-09-26T05:07:36-10:00',} for id_ in range(5)]
        )
    
    result = get_obs(place_name="Barcelona")
    
    assert len(result) == 261

    place_ids = []
    for observation in result:
        place_ids.append(observation.place_id)
    assert len(place_ids) > 1

    assert type(result[0].updated_at) == datetime.datetime

# test para nombre de place_name que no devuelve nada
def test_get_obs_from_place_name_returns_no_obs(requests_mock,) -> None:
    requests_mock.get(
        f"{API_URL}/places.json?q=Cuntis",
        json=[]
        )
    
    result = get_obs(place_name="Cuntis")
    
    assert len(result) == 0

# test de uso de la función con taxon en minúsculas
def test_get_obs_from_taxon_min_returns_info(requests_mock,) -> None:
    requests_mock.get(
        f"{API_URL}/observations.json?iconic_taxa=Fungi&per_page=200",
        json=[{
            "id": 1645, 
            "iconic_taxon_id": 13,} for id_ in range(57)
            ]
    )

    result = get_obs(taxon='fungi')

    assert len(result) == 57

# test de usos combinados
def test_get_obs_from_combined_arguments(requests_mock,) -> None:
    requests_mock.get(
        f"{API_URL}/observations/zolople.json?iconic_taxa=Mollusca&per_page=200",
        json=[{
            "id": id_,} for id_ in range(5)
            ]
    )
    result = get_obs(taxon="Mollusca", user="zolople")

    assert len(result) == 5

# test combinado id_project, place_id, query
def test_get_obs_from_three_combined_arguments(requests_mock,) -> None:
    requests_mock.get(
        f'{API_URL}/observations/project/45.json?place_id=3&q="quercus quercus"&per_page=200',
        json=[
            {"id": 4586, "project": 45, "place": 3, "species": "quercus quercus"},
            {"id": 4588, "project": 45, "place": 3, "species": "quercus quercus"},
        ]
    )
    result = get_obs(id_project=45, place_id=3, query="quercus quercus")

    assert len(result) == 2


def test_get_obs_from_fake_taxon() -> None:
    with pytest.raises(ValueError):
        get_obs(taxon="inexistente")


def test_get_count_by_taxon_returns_info(requests_mock,) -> None:
    requests_mock.get(
        f"{API_URL}/taxa.json",
        json=[{
                'name': 'Fungi',
                'observations_count': 7883,
            },
            {
                'name': 'Protozoa',
                'observations_count': 123,
            },
            {
                'name': 'Chromista',
                'observations_count': 1375,
            },
            {
                'name': 'Animalia',
                'observations_count': 107108,
            }
            ]
            )
    result = get_count_by_taxon()
    
    assert len(result) == 4
    assert result['Chromista'] == 1375


def test_get_obs_from_year_returns_obs(requests_mock,) -> None:
    expected_result = [Observation(
        id=id_,
        ) for id_ in range(150)
    ]
    requests_mock.get(
        f"{API_URL}/observations.json?year=2018&per_page=200",
        json=[{
            "id": id_,} for id_ in range(150)
            ]
    )
    result = get_obs(year=2018)
    
    assert result == expected_result
    assert len(result) == 150


def test_get_obs_with_num_max(requests_mock,) -> None:
    expected_result = [Observation(
        id=id_,
        ) for id_ in range(10)
    ]
    requests_mock.get(
        f"{API_URL}/observations.json?iconic_taxa=Fungi&per_page=200",
        json=[{"id": id_,} for id_ in range(200)]
        )

    result = get_obs(taxon="fungi", num_max=10)
    assert result == expected_result
    assert len(result) == 10

def test_get_dfs_extrae_dfs() -> None:
    observations = [
        Observation(
            id=1,
            photos=[
                Photo(
                    id=1,
                    medium_url="http://a.jpg")],
            iconic_taxon="animalia",
            taxon_name="Thalassoma pavo",
            user_login="joselu_00",
            latitude=40.1,
            longitude=-7.5,
            observed_on=datetime.datetime(2021, 9, 16),
            created_at=datetime.datetime(2021, 9, 2, 19, 43, 43, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200))),
            updated_at=datetime.datetime(2021, 9, 2, 19, 43, 43, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200))),
            )]

    expected_result_obs = pd.DataFrame([{
        "id":1, 
        'captive': None,
        "created_at": None,
        "updated_at": None,
        "observed_on": None,
        'description': None, 
        "iconic_taxon": "animalia",
        "ancestry": "1/2/4/3/343/1409/35511",
        'taxon_id': None, 
        "taxon_name": "Thalassoma pavo",
        'taxon_ancestry': None, 
        "latitude": 40.1,
        "longitude":-7.5,
        'place_name': None, 
        'place_id': None,
        'quality_grade': None, 
        'user_id': None, 
        'user_login': "joselu_00",
        'num_identification_agreements': None, 
        'num_identification_disagreements': None,
         }])
    
    result_obs, result_photo = get_dfs(observations)
    assert type(result_obs) == pd.DataFrame    
    assert(len(result_obs)) == len(observations)
    assert result_obs['id'].values != None  
    
def test_get_taxon_columns() -> None:
    df_obs = pd.DataFrame([{
        "id":1, 
        'captive': None,
        "created_at": None,
        "updated_at": None,
        "observed_on": None,
        'description': None, 
        "iconic_taxon": "animalia",
        'taxon_ancestry': "1/2/4/3/343/1409/35511", 
        'taxon_id': None, 
        "taxon_name": "Thalassoma pavo",
        "latitude": 40.1,
        "longitude":-7.5,
        'place_name': None, 
        'place_id': None,
        'quality_grade': None, 
        'user_id': None, 
        'user_login': "joselu_00",
        'num_identification_agreements': None, 
        'num_identification_disagreements': None,
         }])
    expected_result_obs = pd.DataFrame([{
        "id":1, 
        'captive': None,
        "created_at": None,
        "updated_at": None,
        "observed_on": None,
        'description': None, 
        "iconic_taxon": "animalia",
        'taxon_id': None, 
        "taxon_name": "Thalassoma pavo",
        'taxon_ancestry': "1/2/4/3/343/1409/35511", 
        "latitude": 40.1,
        "longitude":-7.5,
        'place_name': None, 
        'place_id': None,
        'quality_grade': None, 
        'user_id': None, 
        'user_login': "joselu_00",
        'num_identification_agreements': None, 
        'num_identification_disagreements': None,
        "kingdom": "Animalia",
        "phylum": "Chordata",
        "class": "Actinopterygii",
        "order": "Perciformes",
        "superfamily": None,
        "family": "Labridae",
        "genus": "Thalassoma"
         }])
    df_result = get_taxon_columns(df_obs)
    assert type(df_result) == pd.DataFrame
    assert df_result['genus'].item() == "Thalassoma"
    assert len(df_result.columns) > len(df_obs.columns)