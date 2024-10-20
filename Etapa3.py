from azure.cosmos import CosmosClient, PartitionKey, exceptions
import requests
import urllib3
import json


# Desabilitar os avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Função para extrair o mês anterior com base na data de atualização
def extrair_mes_anterior(data_atualizacao):
    ano, mes, _ = data_atualizacao.split('-')
    ano = int(ano)
    mes = int(mes) - 1
    if mes == 0:
        mes = 12
        ano -= 1
    return f"{ano:04d}-{mes:02d}"

# Inicialização da variável mes_anterior
mes_anterior = None

# Consulta à API para obter a última data de atualização
url_update = "https://api-comexstat.mdic.gov.br/general/dates/updated"
try:
    response_update = requests.get(url_update, verify=False)
    if response_update.status_code == 200:
        response_json = response_update.json()
        print("Resposta da API de atualização:", json.dumps(response_json, indent=4))  # Impressão da resposta
        if 'data' in response_json and 'updated' in response_json['data']:
            ultima_atualizacao = response_json['data']['updated']  # Mudança aqui
            print(f"Última atualização: {ultima_atualizacao}")
            mes_anterior = extrair_mes_anterior(ultima_atualizacao)
            print(f"Consultando dados do mês anterior: {mes_anterior}")
        else:
            print("Chave 'updated' não encontrada na resposta da API.")
    else:
        print(f"Erro ao consultar a última atualização: {response_update.status_code}")
except requests.exceptions.RequestException as e:
    print(f"Erro ao consultar a API de atualização: {e}")

# Verifique se mes_anterior foi definido
if mes_anterior:
    # Definir o URL e o payload da API para dados de importação do mês anterior
    url_api = "https://api-comexstat.mdic.gov.br/general"
    payload = {
        "flow": "import",
        "monthDetail": True,
        "period": {
            "from": mes_anterior,
            "to": mes_anterior
        },
        "filters": [
            {
                "filter": "country",
                "values": [160]  # China
            }
        ],
        "details": [
            "country",
            "state",
            "ncm",
            "via",
            "urf",
            "section"
        ],
        "metrics": [
            "metricFOB",
            "metricKG",
            "metricStatistic",
            "metricFreight",
            "metricInsurance",
            "metricCIF"
        ]
    }

    # Exibir o payload enviado
    print(f"Payload enviado para a API: {json.dumps(payload, indent=4)}")

    # Definir o cabeçalho da requisição
    headers = {
        "Content-Type": "application/json"
    }

    # Realizar a consulta à API
    try:
        response = requests.post(url_api, headers=headers, json=payload, verify=False)
        if response.status_code == 200:
            data = response.json()  # Dados recebidos da API
            if 'data' in data and 'list' in data['data']:
                print("Dados recebidos da API")

                # Configuração do Cosmos DB
                endpoint = 'https://projetodb.documents.azure.com:443/'
                key = 'ff6Z3bABI60eIFUPz0peVii6574UGGi3uG3hzivuREUizsnhr93httKuCRRx5doSCRtXGa3XjcikACDbHawkpA=='
                database_name = 'ToDoList'
                container_name = f'items_{mes_anterior.replace("-", "_")}'  # Nome do contêiner com base no mês anterior

                # Inicializar o cliente do Cosmos DB
                client = CosmosClient(endpoint, key)
                database = client.get_database_client(database_name)

                try:
                    # Tenta ler o contêiner
                    container = database.get_container_client(container_name)
                    container.read()
                    print(f"Contêiner '{container_name}' já existe.")
                except exceptions.CosmosResourceNotFoundError:
                    # Se o contêiner não existir, ele será criado com uma chave de partição padrão
                    container = database.create_container_if_not_exists(
                        id=container_name,
                        partition_key=PartitionKey(path="/coNcm")  # Usando 'coNcm' como chave de partição
                    )
                    print(f"Contêiner '{container_name}' criado.")

                # Inserir os dados em lotes no Cosmos DB
                for item in data['data']['list']:
                    try:
                        item['id'] = item['coNcm']  # Definir 'id' como 'coNcm'
                        container.upsert_item(item)  # Método correto é upsert_item
                        print(f"Item inserido/atualizado: {item}")
                    except exceptions.CosmosHttpResponseError as e:
                        print(f"Erro ao inserir o item no Cosmos DB: {e.message}")
        else:
            print(f"Erro ao consultar a API: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao consultar a API: {e}")
else:
    print("Não foi possível obter o mês anterior. Verifique a API de atualização.")
