#!/usr/bin/env python3
from .models import Project, Observation, TAXONS, ICONIC_TAXON, Photo
from typing import List, Dict, Any, Union, Optional
import requests
from contextlib import suppress
import urllib3
import pandas as pd
import flat_table
import os
import shutil
import re
import numpy as np
import pkg_resources

urllib3.disable_warnings()

# Definición de variables
API_URL = "https://minka-sdg.org"

file_path = pkg_resources.resource_filename("mecoda_minka", "data/taxon_tree.csv")
df_taxon = pd.read_csv(file_path)

# Función para extraer los datos de un proyecto a partir de su nombre o id
def get_project(p: Union[str, int]) -> List[Project]:
    """Download information of a project from id or name"""  

    if type(p) is int:
        url = f"{API_URL}/projects/{p}.json"
        page = requests.get(url, verify=False)
        
        if page.status_code == 404:
            print("ID No encontrado")
            exit
        else:
            resultado = [Project(**page.json())]
            return resultado

    elif type(p) is str:
        url = f"{API_URL}/projects/search.json?q={p}"
        page = requests.get(url, verify=False)
        resultado = [Project(**proj) for proj in page.json()]
        return resultado

# Función interna para extraer los posibles ids de un lugar a partir de una palabra
def _get_ids_from_place(place:str) -> list:
    place_ids = []
    url = f"{API_URL}/places.json?q={place}"
    page = requests.get(url, verify=False)

    for dct in page.json():
        place_id = dct['id']
        place_ids.append(place_id)
        
    return place_ids


# Función interna para construir la url a la que se hará la petición de observaciones
def _build_url(
    query: Optional[str] = None, 
    id_project: Optional[int] = None,
    project_name: Optional[str] = None,
    id_obs: Optional[int] = None,
    user: Optional[str] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    year: Optional[int] = None,
    ) -> str:
    
    #import pdb; pdb.set_trace()

    if project_name is not None:
        id_project = get_project(project_name)[0].id
    
    # definir la base url
    if id_project is not None:
        base_url = f"{API_URL}/observations/project/{id_project}.json"
    elif id_obs is not None:
        base_url = f"{API_URL}/observations/{id_obs}.json"
    elif user is not None:
        base_url = f"{API_URL}/observations/{user}.json"
    else:
        base_url = f"{API_URL}/observations.json"
    
    # definir los argumentos que admite la API
    args = []
    if query is not None:
        args.append(f'q="{query}"')
    if taxon is not None:
        taxon = taxon.title()
        if taxon in TAXONS:
            args.append(f"iconic_taxa={taxon}")
        else:
            raise ValueError("No es una taxonomía válida")
    if place_id is not None:
        args.append(f"place_id={place_id}")
    if year is not None:
        args.append(f"year={year}")
    if taxon_id is not None:
        args.append(f"taxon_id={taxon_id}")

    url = f'{base_url}?{"&".join(args)}&per_page=200'
    
    #cuando no indicamos ningún parámetro, devuelve los últimos registros

    return url

# Función interna que toma una lista de diccionarios y devuelve una lista de objetos Observation
def _build_observations(observations_data: List[Dict[str, Any]]) -> List[Observation]:
    '''
    Construye objetos Observation a partir del JSON de observaciones de la API.

    Args:
        observations_data: lista de diccionarios. Cada uno contiene la información de una observación.
    
    '''
    observations = []
    
    for data in observations_data:

        with suppress(KeyError):
            if data['place_guess'] is not None:
                data['place_name'] = data['place_guess'].replace("\r\n", ' ').strip()

        with suppress(KeyError):
            try:
                data["taxon_id"]=int(data['taxon']['id'])
                data["taxon_name"]=data['taxon']['name']
                data['taxon_ancestry']=data['taxon']['ancestry']
            except:
                data["taxon_id"]=None
                data["taxon_name"]=None
                data['taxon_ancestry']=None
    
        with suppress(KeyError):
            lista_fotos = []
            for observation_photo in data['photos']:
                lista_fotos.append(Photo(
                    id=observation_photo['id'],
                    large_url=observation_photo['large_url'],
                    medium_url=observation_photo['medium_url'],
                    small_url=observation_photo['small_url'],
                ))
            data['photos'] = lista_fotos

        with suppress(KeyError):
            lista_fotos = []
            for observation_photo in data['observation_photos']:
                lista_fotos.append(Photo(
                    id=observation_photo['id'],
                    large_url=observation_photo['photo']['large_url'],
                    medium_url=observation_photo['photo']['medium_url'],
                    small_url=observation_photo['photo']['small_url'],
                ))
            data['photos'] = lista_fotos

        with suppress(KeyError):
            data['iconic_taxon'] = ICONIC_TAXON[data['iconic_taxon_id']]
        
        # eliminación de saltos de línea en el campo description
        with suppress(KeyError):
            if data['description'] is not None:
                data['description'] = data['description'].replace("\r\n", ' ')

        observation = Observation(**data)

        observations.append(observation)
    
    return observations

# Función interna que realiza la petición de la API y devuelve la lista de objetos Observation
def _request(arg_url: str, num_max: Optional[int] = None) -> List[Observation]:
    observations = []
    n = 1
    page = requests.get(arg_url, verify=False)

    if page.status_code == 404:
        raise ValueError("Not found")

    elif page.status_code == 200:
        if type(page.json()) is dict:
            observations.extend(_build_observations([page.json()]))
        
        else:
            while len(page.json()) == 200:
                observations.extend(_build_observations(page.json()))
                n += 1
                if n > 49:
                    print("WARNING: Only the first 10,000 results are displayed")
                    break
                if num_max is not None and len(observations) >= num_max:
                    break
                url = f"{arg_url}&page={n}"
                page = requests.get(url, verify=False)
                print(f"Número de elementos: {len(observations)}")
                
            observations.extend(_build_observations(page.json()))
            
            if num_max:
                observations = observations[:num_max]

        print(f"Número de elementos: {len(observations)}")
    return observations

# Función para extraer las observaciones y que admite distintos filtros
def get_obs(
    query: Optional[str] = None, 
    id_project: Optional[int] = None,
    project_name: Optional[str] = None,
    id_obs: Optional[int] = None,
    user: Optional[str] = None,
    taxon: Optional[str] = None,
    taxon_id: Optional[int] = None,
    place_id: Optional[int] = None,
    place_name: Optional[str] = None,
    year: Optional[int] = None,
    num_max: Optional[int] = None,
    ) -> List[Observation]:
    #import pdb; pdb. set_trace()
    print("Generando lista de observaciones:")

    # procesamos el string del nombre del proyecto
    if project_name is not None:
        project_name = re.sub(r"^\s+|\s+$", "", project_name)
        project_name = re.sub(r"\s", "-", project_name)

    if place_name is not None:
        place_ids = _get_ids_from_place(place_name)
        observations = []
        
        for place_id in place_ids:
            url = _build_url(
                query, 
                id_project,
                project_name,
                id_obs,
                user,
                taxon,
                taxon_id,
                place_id,
                year,
                )
            observations.extend(_request(url, num_max))
    else:
        url = _build_url(
            query, 
            id_project,
            project_name,
            id_obs,
            user,
            taxon,
            taxon_id,
            place_id,
            year,
            )
        
        observations = _request(url, num_max)

    return observations

# Función que devuelve el número de observaciones registrado de cada familia taxonómica
def get_count_by_taxon() -> Dict:
    url = f"{API_URL}/taxa.json"
    page = requests.get(url, verify=False)
    taxa = page.json()
    count = {}
    for taxon in taxa:
        count[taxon['name']] = taxon['observations_count']
    return count


# Función para extraer dataframe de observaciones y dataframe de photos
def get_dfs(observations) -> pd.DataFrame:
    df = pd.DataFrame([obs.dict() for obs in observations])
    df2 = df.drop(['photos'], axis=1)
    df["taxon_id"] = df["taxon_id"].astype("Int64", errors="ignore") 

    df_observations = flat_table.normalize(df2).drop(['index'], axis=1)
    df_observations['created_at'] = df_observations['created_at'].apply(lambda x: x.date()).astype('datetime64[ns]')
    df_observations['updated_at'] = df_observations['updated_at'].apply(lambda x: x.date()).astype('datetime64[ns]')
    df_observations['observed_on'] = df_observations['observed_on'].astype('datetime64[ns]')

    df_photos = flat_table.normalize(df[['id', 'photos', 'iconic_taxon', 'taxon_id', 'taxon_name', 'taxon_ancestry', 'user_login', 'latitude', 'longitude']]).drop(['index'], axis=1)
    df_photos = df_photos[['id', 'photos.id', 'iconic_taxon', 'taxon_name', 'photos.medium_url', 'user_login', 'latitude', 'longitude']]
    df_photos['photos.id'] = df_photos['photos.id'].astype("Int64", errors='ignore')
    df_photos['path'] = df_photos['id'].astype(str) + "_" + df_photos['photos.id'].astype(str) + ".jpg"
    
    return df_observations, df_photos

# Función para crear columnas de los distintos rangos taxonómicos
def _get_name_from_id(number_list):
    name_list = []
    for number in number_list:
        if number != 1:
            try:
                rank = df_taxon[df_taxon['id'] == int(number)]['rank'].item()
                name = df_taxon[df_taxon['id'] == int(number)]['name'].item()
                name_list.append(f"{rank} {name}")
            except:
                try:
                    rank = requests.get(f"https://minka-sdg.org/taxa/{number}.json").json()['rank']
                    name = requests.get(f"https://minka-sdg.org/taxa/{number}.json").json()['name']
                    name_list.append(f"{rank} {name}")
                except:
                    continue
    return name_list 

def _get_level(x, level):
    result = None
    for elem in x:
        if elem.startswith(level):
            result = elem.replace(f"{level} ", "")
    return result

def get_taxon_columns(df_obs: pd.DataFrame) -> pd.DataFrame:
    df_life = df_obs[df_obs['taxon_ancestry'].isnull()]
    df_obs = df_obs[df_obs['taxon_ancestry'].notnull()]
    
    # set copy to avoid warning
    df = df_obs.copy()
    df.taxon_ancestry = df_obs.taxon_ancestry.str.split("/")
    df_obs = df

    df_obs['ancestry_names'] = df_obs.taxon_ancestry.apply(lambda x: _get_name_from_id(x))
    
    # set copy to avoid warning
    df = df_obs.copy()
    for level in ['kingdom', 'phylum', 'class', 'order', 'superfamily', 'family', 'genus']:
        df[level] = df_obs['ancestry_names'].apply(lambda x: _get_level(x, level))
    df_obs = df
    
    df_obs = df_obs.drop(columns=['ancestry_names'])
    
    df_obs = pd.concat([df_obs, df_life], ignore_index = True, axis = 0)
    
    return df_obs


# Función para obtener información extra de cada observación de una selección (muy costoso a nivel de API)
def extra_info(df_observations) -> pd.DataFrame:
    ids = df_observations['id'].to_list()
    dic = {}

    for id_num in ids:
        url = f"{API_URL}/observations/{id_num}.json"
        page = requests.get(url, verify=False)

        idents = page.json()['identifications']
        if len(idents) > 0:
            user_identification = idents[0]['user']['login']
            first_taxon_name = idents[0]['taxon']['name']
            last_taxon_name = idents[len(idents) - 1]['taxon']['name']
            dic[id_num] = [user_identification, first_taxon_name, last_taxon_name]
        else:
            dic[id_num] = [0, 0, 0]

    df_observations['first_identification'] = df_observations['id'].apply(lambda x: str(dic[x][0]))
    df_observations['first_taxon_name'] = df_observations['id'].apply(lambda x: str(dic[x][1]))
    df_observations['last_taxon_name'] = df_observations['id'].apply(lambda x: str(dic[x][2]))

    df_observations['first_taxon_match'] = np.where(df_observations['first_taxon_name'] == df_observations['last_taxon_name'], 'True', 'False')
    df_observations['first_identification_match'] = np.where(df_observations['first_identification'] == df_observations['user_login'], 'True', 'False')

    return df_observations


# Función para descargar las fotos resultado de la consulta
def download_photos(df_photos: pd.DataFrame, directorio: Optional[str] = "minka_photos"):
    
    # Crea la carpeta, si existe la sobreescribre
    if os.path.exists(directorio):
        shutil.rmtree(directorio)
    os.makedirs(directorio)

    # Itera por el df_photos resultado de la consulta y descarga las fotos en tamaño medio
    for n in range(len(df_photos)):
        row = df_photos.iloc[[n]]
        response = requests.get(row['photos.medium_url'][n], verify=False, stream=True)
        if response.status_code == 200:
            with open(f"{directorio}/{row['path'][n]}", 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
        del response
        
    df_photos['path'] = df_photos['path'].apply(lambda x: os.path.abspath(f"{directorio}/{x}"))
