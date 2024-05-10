from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from concurrent.futures import ThreadPoolExecutor
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import requests
import json
import os

default_args = {
    'owner': 'julia_costa',
    'start_date': datetime(2024, 5, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'limpeza_linhagem',
    default_args=default_args,
    description='DAG que executa a limpeza da linhagem no OpenMetadata',
    schedule_interval=None,
)

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

    response = requests.get(url, headers=headers, params=params, verify=False)

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

def fetch_schemas(**kwargs):
    """
    Obtém os schemas por meio da API do OpenMetadata, paginando os resultados e salvando-os em um arquivo JSON local. 
    Depois, o arquivo é carregado no Google Cloud Storage (GCS).

    Returns:
        str: o caminho do arquivo JSON temporário.
    """    
    url = 'https://sandbox.open-metadata.org/api/v1/databaseSchemas?database=Ecommerce_Datawarehouse.dev'
    token = Variable.get("token_om")

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
    temp_file_path = '/tmp/schemas.json'
    save_to_json(output_data, temp_file_path)

    bucket_name = "bucket-om"
    hook = GCSHook(gcp_conn_id="conexao_gcp")
    hook.upload(bucket_name=bucket_name, object_name='arquivos/schemas.json', filename=temp_file_path)

    print(f"schemas.json está no bucket '{bucket_name}'")
    print("Fase 1 completa")

    return temp_file_path

def create_table_files(**kwargs):
    """
    Obtém informações como ID, FQN e URL sobre as tabelas associadas aos schemas obtidos.
    Essas informações são formatadas em arquivos JSON individuais e carregadas no GCS.

    Args:
        **kwargs: argumentos do Airflow.
    """    
    bucket_name = "bucket-om"
    hook = GCSHook(gcp_conn_id="conexao_gcp")

    temp_file_path = '/tmp/schemas.json'
    hook.download(bucket_name=bucket_name, object_name='arquivos/schemas.json', filename=temp_file_path)

    with open(temp_file_path, 'r') as file:
        databaseSchema_names = json.load(file)['data']

    base_url = ' https://sandbox.open-metadata.org/api/v1/tables?databaseSchema='
    token = Variable.get("token_om")
    
    gcs_folder_path = 'arquivos/tabelas/'

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
        output_json = json.dumps(output_data, indent=4)
        
        gcs_object_name = f'{gcs_folder_path}{output_file_name}'
        hook.upload(bucket_name=bucket_name, object_name=gcs_object_name, filename=None, data=output_json)

    print(f"Arquivos criados em '{gcs_folder_path}'")
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
    token = Variable.get("token_om")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers, verify=False)

        if response.status_code == 200:
            linhagem = response.json()
            return linhagem
        else:
            print(f'Erro ao obter a linhagem para o ID {id}. Status code: {response.status_code}')
            return None
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return None

def extract_and_save_lineage(**kwargs):
    """
    Extrai a linhagem para cada tabela a partir dos arquivos JSON obtidos anteriormente.
    Os resultados são salvos em um arquivo JSON local, que também é carregado no GCS.

    Args:
        **kwargs: argumentos do Airflow.
    """

    folder_path = "arquivos/tabelas/"
    lineage_file_path = '/tmp/resultados_linhagem.json'
    gcs_object_name = 'arquivos/resultados_linhagem.json'
    bucket_name = "bucket-om"

    resultados = []

    hook = GCSHook(gcp_conn_id="conexao_gcp")
    files = hook.list(bucket_name, prefix=folder_path)

    for file_name in files:
        if file_name.endswith('.json'):
            json_data = hook.download(bucket_name=bucket_name, object_name=file_name)
            dados_json = json.loads(json_data)['data']

            def processar_item(item):
                if item.get('tableType') == 'Regular' or item.get('tableType') == 'Partitioned' or item.get('tableType') == 'External':
                    id = item['id']
                    linhagem = get_lineage_by_id(id)
                    if linhagem is not None and 'nodes' in linhagem and len(linhagem['nodes']) > 0:
                        for node in linhagem['nodes']:
                            fully_qualified_name = node.get('fullyQualifiedName', '')
                            if 'vw' not in fully_qualified_name.lower():
                                if 'downstreamEdges' not in linhagem or not linhagem['downstreamEdges'] or 'lineageDetails' not in linhagem['downstreamEdges'][0]:
                                    if 'upstreamEdges' not in linhagem or not linhagem['upstreamEdges'] or 'lineageDetails' not in linhagem['upstreamEdges'][0]:
                                        resultados.append({
                                            'id': id,
                                            'linhagem': linhagem
                                        })

            for item in dados_json:
                processar_item(item)

    resultados_formatados = json.dumps(resultados, indent=4)

    with open(lineage_file_path, 'w') as outfile:
        outfile.write(resultados_formatados)

    hook.upload(bucket_name=bucket_name, object_name=gcs_object_name, filename=lineage_file_path)

    print(f"Resultados salvos em '{gcs_object_name}'")
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
    token = Variable.get("token_om")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.delete(url, headers=headers, verify=False)

        if response.status_code == 200:
            print(f'Linhagem entre {from_id} e {to_id} foi deletada com sucesso.')
            return {'from_id': from_id, 'to_id': to_id, 'status': 'Deletada com sucesso'}
        else:
            print(f'Erro ao deletar a linhagem entre {from_id} e {to_id}. Status code: {response.status_code}')
            return {'from_id': from_id, 'to_id': to_id, 'status': 'Erro ao deletar'}
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return {'from_id': from_id, 'to_id': to_id, 'status': 'Erro ao deletar'}

processed_lineage_pairs = set()

def delete_lineage(**kwargs):
    """
    Processa os resultados da linhagem para identificar as tabelas relacionadas.
    A linhagem entre essas tabelas é deletada utilizando a API OpenMetadata.

    Args:
        **kwargs: argumentos do Airflow.
    """    
    resultados = []

    with open('/tmp/resultados_linhagem.json', 'r') as f:
        data = json.load(f)

        for item in data:
            from_id = item['id']
            downstream_edges = item['linhagem'].get('downstreamEdges', [])
            upstream_edges = item['linhagem'].get('upstreamEdges', [])

            for edge in downstream_edges:
                to_id = edge.get('toEntity')
                if from_id and to_id and ((from_id, to_id) not in processed_lineage_pairs and (to_id, from_id) not in processed_lineage_pairs):
                    from_fully_qualified_name = item['linhagem']['entity']['fullyQualifiedName']
                    to_fully_qualified_name = edge.get('toEntityFullyQualifiedName', '')

                    resultados.append({
                        'from_id': from_id,
                        'from_fully_qualified_name': from_fully_qualified_name,
                        'to_id': to_id,
                        'to_fully_qualified_name': to_fully_qualified_name,
                        'status': exclude_lineage(from_id, to_id)['status']
                    })

                    processed_lineage_pairs.add((from_id, to_id))

            for edge in upstream_edges:
                to_id = edge.get('fromEntity')
                if from_id and to_id and ((from_id, to_id) not in processed_lineage_pairs and (to_id, from_id) not in processed_lineage_pairs):
                    from_fully_qualified_name = item['linhagem']['entity']['fullyQualifiedName']
                    to_fully_qualified_name = edge.get('fromEntityFullyQualifiedName', '')

                    resultados.append({
                        'from_id': from_id,
                        'from_fully_qualified_name': from_fully_qualified_name,
                        'to_id': to_id,
                        'to_fully_qualified_name': to_fully_qualified_name,
                        'status': exclude_lineage(to_id, from_id)['status']
                    })

                    processed_lineage_pairs.add((to_id, from_id))

    with open('/tmp/resultados_delecao.json', 'w') as outfile:
        json.dump(resultados, outfile, indent=4)

    bucket_name = "bucket-om"
    gcs_object_name = 'arquivos/resultados_delecao.json'
    hook = GCSHook(gcp_conn_id="conexao_gcp")
    hook.upload(bucket_name=bucket_name, object_name=gcs_object_name, filename='/tmp/resultados_delecao.json')

    print("Deleção completa, os resultados foram salvos na GCS")
    print("Fase 4 completa")

fetch_schemas_task = PythonOperator(
    task_id="fetch_schemas",
    python_callable=fetch_schemas,
    provide_context=True,
    dag=dag,
)

create_table_files_task = PythonOperator(
    task_id="create_table_files",
    python_callable=create_table_files,
    provide_context=True,
    dag=dag,
)

get_lineage_and_save_to_gcs_task = PythonOperator(
    task_id="get_lineage_and_save_to_gcs",
    python_callable=extract_and_save_lineage,
    provide_context=True,
    dag=dag,
)

delete_lineage_task = PythonOperator(
    task_id="delete_lineage",
    python_callable=delete_lineage,
    provide_context=True,
    dag=dag,
)

fetch_schemas_task >> create_table_files_task >> get_lineage_and_save_to_gcs_task >> delete_lineage_task