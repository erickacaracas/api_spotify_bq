from flask import Flask, request
import base64
import os
import time
import json
import requests
from google.cloud import bigquery, secretmanager, storage

app = Flask(__name__)

# Função para obter segredo do Secret Manager
def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'data-engineering-3254')
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    return json.loads(response.payload.data.decode("UTF-8"))

# Função para controle de limite de requisições
def make_request_with_limit_control(url, headers):
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            retry_after = response.headers.get('Retry-After', 60)
            print(f"Limite de requisições atingido. Esperando {retry_after} segundos.")
            time.sleep(int(retry_after))
        else:
            print(f"Erro inesperado: {response.status_code} - {response.text}")
            break

# Função para baixar chave da service account do Cloud Storage
def service_account_key(bucket_name, file_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)
    print(f"Chave de service account baixada para {destination_file_name}")

# Função para obter o token de acesso do Spotify
def access_token_spotify(client_id, client_secret):
    auth_url = "https://accounts.spotify.com/api/token"
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}

    response = requests.post(auth_url, headers=headers, data=data)
    if response.status_code != 200:
        print(f"Erro ao obter token: {response.text}")
        return None
    return response.json().get("access_token")

# Função principal para ingestão de dados
@app.route('/', methods=['POST'])
def handle_request(data, context):
    try:
        pubsub_message = request.get_json()
        print(f"Recebido: {json.dumps(pubsub_message)}")

        # Caminho e dados de configuração
        dataset_id = "spotify_podcasts_gcp"
        table_id_podcasts = "tb_datahackers_limit"
        table_id_episodios = "tb_datahackers_eps_total"
        table_id_eps_grupo = "tb_datahackers_eps_grupo_boticario"

        bucket_name = "spotify-case-podcasts"
        credentials_file_name = "chave_sa.json"
        local_credentials_path = "/tmp/chave_sa.json"

        # Baixar chave da service account
        print("Baixando chave da service account...")
        service_account_key(bucket_name, credentials_file_name, local_credentials_path)

        # Obter credenciais do Spotify
        print("Obtendo credenciais do Spotify...")
        spotify_credentials = get_secret("spotify-credentials")
        client_id = spotify_credentials['client_id']
        client_secret = spotify_credentials['client_secret']

        print("Obtendo token de acesso do Spotify...")
        token = access_token_spotify(client_id, client_secret)
        if not token:
            print("Erro ao obter token de acesso do Spotify.")
            return "Erro ao obter token", 500

        # Buscar podcasts
        print("Buscando podcasts...")
        podcasts_url = "https://api.spotify.com/v1/search?q=data%20hackers&type=show&market=BR&limit=50"
        headers = {"Authorization": f"Bearer {token}"}
        response = make_request_with_limit_control(podcasts_url, headers)

        if response.status_code == 200:
            podcasts = response.json().get('shows', {}).get('items', [])
            print(f"Podcasts recebidos: {len(podcasts)}")

            # Conexão ao BigQuery
            print("Conectando ao BigQuery...")
            client = bigquery.Client.from_service_account_json(local_credentials_path)

            # Criar tabela de podcasts e inserir dados
            print(f"Verificando se a tabela {table_id_podcasts} existe...")
            if not table_exists(client, dataset_id, table_id_podcasts):
                print(f"Criando tabela {table_id_podcasts}...")
                create_table(client, dataset_id, table_id_podcasts)
                print("Inserindo dados na tabela de podcasts...")
                insert_podcasts(client, dataset_id, table_id_podcasts, podcasts)

            # Processar episódios para cada podcast
            for podcast in podcasts:
                show_id = podcast["id"]
                print(f"Buscando episódios para o podcast {show_id}...")
                episodios = busca_episodios(token, show_id)
                if episodios:
                    print(f"Processando {len(episodios)} episódios para o podcast {show_id}...")
                    # Criar tabela de episódios e inserir dados
                    if not table_exists(client, dataset_id, table_id_episodios):
                        print(f"Criando tabela {table_id_episodios}...")
                        create_table(client, dataset_id, table_id_episodios)
                    
                    print(f"Inserindo dados na tabela de episódios...")
                    insert_episodes(client, dataset_id, table_id_episodios, episodios)

                    # Inserir episódios do Grupo Boticário
                    grupo_boticario_rows = [e for e in episodios if "Grupo Boticário" in e["description"]]
                    if grupo_boticario_rows:
                        print(f"Encontrados {len(grupo_boticario_rows)} episódios do Grupo Boticário...")
                        if not table_exists(client, dataset_id, table_id_eps_grupo):
                            print(f"Criando tabela {table_id_eps_grupo}...")
                            create_table(client, dataset_id, table_id_eps_grupo)
                        print(f"Inserindo episódios do Grupo Boticário na tabela {table_id_eps_grupo}...")
                        insert_episodes(client, dataset_id, table_id_eps_grupo, grupo_boticario_rows)

        print("Dados processados com sucesso.")
        return "Dados processados com sucesso", 200

    except Exception as e:
        print(f"Erro ao processar a mensagem: {str(e)}")
        return "Erro ao processar a mensagem", 500

def busca_episodios(token, show_id):
    episodes_url = f"https://api.spotify.com/v1/shows/{show_id}/episodes"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(episodes_url, headers=headers)
    return response.json().get("items", []) if response.status_code == 200 else None

def table_exists(client, dataset_id, table_id):
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False

def create_table(client, dataset_id, table_id):
    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("total_episodes", "INTEGER")
    ] if table_id == "tb_datahackers_limit" else [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("release_date", "STRING"),
        bigquery.SchemaField("duration_ms", "INTEGER"),
        bigquery.SchemaField("language", "STRING"),
        bigquery.SchemaField("explicit", "BOOLEAN"),
        bigquery.SchemaField("type", "STRING")
    ]
    
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Tabela {table_id} criada.")

def insert_podcasts(client, dataset_id, table_id, podcasts):
    podcast_rows = [{"name": p.get("name"), "description": p.get("description"), "id": p.get("id"), "total_episodes": p.get("total_episodes", 0)} for p in podcasts]
    client.insert_rows_json(f"{dataset_id}.{table_id}", podcast_rows)
    print(f"{len(podcast_rows)} podcasts inseridos na tabela {table_id}.")

def insert_episodes(client, dataset_id, table_id, episodes):
    episode_rows = []
    for e in episodes:
        episode_rows.append({
            "id": e.get("id"),
            "name": e.get("name"),
            "description": e.get("description"),
            "release_date": e.get("release_date"),
            "duration_ms": e.get("duration_ms"),
            "language": e.get("language"),
            "explicit": e.get("explicit"),
            "type": e.get("type")
        })
    
    # Inserir episódios em lote
    if episode_rows:
        client.insert_rows_json(f"{dataset_id}.{table_id}", episode_rows)
        print(f"{len(episode_rows)} episódios inseridos na tabela {table_id}.")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
