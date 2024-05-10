from datetime import datetime, timedelta
import requests
import json
import os

def get_data(url, token, after=None):
    """
    Faz uma requisição HTTP para a URL especificada, usando o token fornecido.

    Args:
        url: a URL para a qual a requisição será feita.
        token: o token de autenticação.
        after: o cursor para a próxima página de resultados.

    Returns:
        Os dados JSON da resposta, se a requisição for bem-sucedida e "None" se a requisição falhar.
    """

    headers = {
        'Authorization': f'Bearer {token}'
    }

    params = {'after': after} if after else {}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f'Erro ao fazer requisição. Código de status: {response.status_code}')

def extract_fully_qualified_names(json_data):
    """
    Extrai os FQNs do JSON especificado.

    Args:
        json_data: os dados a serem analisados.

    Returns:
        Uma lista dos FQNs.
    """

    return [item['fullyQualifiedName'] for item in json_data['data']]

def save_to_json(data, file_path):
    """
    Salva os dados no arquivo especificado.

    Args:
        data: os dados JSON a serem salvos.
        file_path: o caminho do arquivo para salvar os dados.
    """

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

def fetch_schemas():
    """
    Obtém os schemas por meio da API do OpenMetadata, paginando os resultados e salvando-os em um arquivo JSON local.
    """

    url = 'https://sandbox.open-metadata.org/api/v1/databaseSchemas?database=Ecommerce_Datawarehouse.dev'
    # Insira seu token aqui
    token = "token-openmetadata"

    result = []
    after = None

    while True:
        response_data = get_data(url, token, after)
        current_data = extract_fully_qualified_names(response_data)
        result.extend(current_data)

        paging_info = response_data['paging']
        if 'after' in paging_info:
            after = paging_info['after']
        else:
            break

    output_data = {'data': result}
    temp_file_path = 'schemas.json'
    save_to_json(output_data, temp_file_path)

    print(f"schemas.json foi salvo localmente")
    print("Fase 1 completa")

def create_table_files():
    """
    Obtém informações como ID, FQN e URL sobre as tabelas associadas aos schemas obtidos.
    Essas informações são formatadas em arquivos JSON individuais e salvadas localmente.
    """

    temp_file_path = 'schemas.json'

    with open(temp_file_path, 'r') as file:
        databaseSchema_names = json.load(file)['data']

    base_url = 'https://sandbox.open-metadata.org/api/v1/tables?databaseSchema='
    # Insira seu token aqui
    token = "token-openmetadata"  

    for name in databaseSchema_names:
        url = f'{base_url}{name}'
        after = None
        response_data = []

        while True:
            json_data = get_data(url, token, after)

            if not json_data:
                break

            filtered_data = []
            for obj in json_data['data']:
                filtered_object = {
                    'id': obj['id'],
                    'fullyQualifiedName': obj['fullyQualifiedName'],
                    'href': obj['href'],
                    'tableType': obj['tableType'],
                }
                filtered_data.append(filtered_object)

            response_data.extend(filtered_data)

            if 'paging' in json_data and 'after' in json_data['paging']:
                after = json_data['paging']['after']
            else:
                break

        output_file_name = f'{name}.json'
        output_data = {'data': response_data}

        with open(output_file_name, 'w') as outfile:
            json.dump(output_data, outfile, indent=4)
        
        print(f"Arquivo '{output_file_name}' criado")
        
    print("Arquivos criados localmente")
    print("Fase 2 completa")


def get_lineage_by_id(id):
    """
    Obtém a linhagem de uma tabela específica por meio de uma chamada à API OpenMetadata.

    Args:
        id (str): o identificador da tabela para a qual a linhagem será obtida.

    Returns:
        dict or None: a linhagem se a requisição for bem-sucedida, "None" se a requisição falhar.
    """

    url = f' https://sandbox.open-metadata.org/api/v1/lineage/table/{id}'
    # Insira seu token aqui
    token = "token-openmetadata"  

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            lineage = response.json()
            return lineage
        else:
            print(f'Erro ao obter a linhagem para o ID {id}. Status code: {response.status_code}')
            return None
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return None

def extract_and_save_lineage():
    """
    Extrai a linhagem para cada tabela a partir dos arquivos JSON obtidos anteriormente.
    Os resultados são salvos em um arquivo JSON local.
    """

    lineage_file_path = 'resultados_linhagem.json'

    with open('schemas.json', 'r') as f:
        databaseSchema_names = json.load(f)['data']

    resultados = []

    for name in databaseSchema_names:
        with open(f'{name}.json', 'r') as file:
            data = json.load(file)['data']

            for item in data:
                if item.get('tableType') == 'Regular' or item.get('tableType') == 'Partitioned' or item.get('tableType') == 'External':
                    id = item['id']
                    lineage = get_lineage_by_id(id)
                    if lineage is not None and 'nodes' in lineage and len(lineage['nodes']) > 0:
                        for node in lineage['nodes']:
                            fully_qualified_name = node.get('fullyQualifiedName', '')
                            if 'vw' not in fully_qualified_name.lower():
                                if 'downstreamEdges' not in lineage or not lineage['downstreamEdges'] or 'lineageDetails' not in lineage['downstreamEdges'][0]:
                                    if 'upstreamEdges' not in lineage or not lineage['upstreamEdges'] or 'lineageDetails' not in lineage['upstreamEdges'][0]:
                                        resultados.append({
                                            'id': id,
                                            'linhagem': lineage
                                        })

    with open(lineage_file_path, 'w') as outfile:
        json.dump(resultados, outfile, indent=4)

    print(f"Resultados salvos localmente")
    print("Fase 3 completa")

def exclude_lineage(from_id, to_id):
    """
    Deleta a linhagem entre duas tabelas específicas por meio da API OpenMetadata.

    Args:
        from_id (str): ID da tabela de origem.
        to_id (str): ID da tabela de destino.

    Returns:
        dict: o status da operação (erro ou sucesso).
    """

    url = f' https://sandbox.open-metadata.org/api/v1/lineage/table/{from_id}/table/{to_id}'
    # Insira seu token aqui
    token = "token-openmetadata" 

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.delete(url, headers=headers)

        if response.status_code == 200:
            print(f'Linhagem entre {from_id} e {to_id} foi deletada com sucesso.')
            return {'from_id': from_id, 'to_id': to_id, 'status': 'Deletada com sucesso'}
        else:
            print(f'Erro ao deletar a linhagem entre {from_id} e {to_id}. Status code: {response.status_code}')
            return {'from_id': from_id, 'to_id': to_id, 'status': 'Erro ao deletar'}
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return {'from_id': from_id, 'to_id': to_id, 'status': 'Erro ao deletar'}

def delete_lineage():
    """
    Processa os resultados da linhagem para identificar as tabelas relacionadas.
    A linhagem entre essas tabelas é deletada utilizando a API OpenMetadata.
    """

    resultados = []

    with open('resultados_linhagem.json', 'r') as f:
        data = json.load(f)

        for item in data:
            from_id = item['id']
            downstream_edges = item['linhagem'].get('downstreamEdges', [])
            upstream_edges = item['linhagem'].get('upstreamEdges', [])

            for edge in downstream_edges:
                to_id = edge.get('toEntity')
                if from_id and to_id:
                    resultados.append({
                        'from_id': from_id,
                        'to_id': to_id,
                        'status': exclude_lineage(from_id, to_id)['status']
                    })

            for edge in upstream_edges:
                to_id = edge.get('fromEntity')
                if from_id and to_id:
                    resultados.append({
                        'from_id': from_id,
                        'to_id': to_id,
                        'status': exclude_lineage(to_id, from_id)['status']
                    })

    with open('resultados_delecao.json', 'w') as outfile:
        json.dump(resultados, outfile, indent=4)

    print("Deleção completa, os resultados foram salvos localmente")
    print("Fase 4 completa")

fetch_schemas()
create_table_files()
extract_and_save_lineage()
delete_lineage()
