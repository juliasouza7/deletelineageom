# Limpeza de Linhagem no OpenMetadata

Este script em Python fornece um processo automatizado para limpar a linhagem entre tabelas em um schema usando a API do OpenMetadata. Ele consiste em várias funções para buscar schemas, criar arquivos, extrair linhagem e excluir linhagem.

## Pré-requisitos
- Python 3.x instalado
- Biblioteca Requests (`pip install requests`)

## Utilização
1. Insira seu token da API do OpenMetadata nos locais designados.
2. Execute o script.

## Funções
1. **fetch_schemas**: obtém os schemas da API do OpenMetadata e os salva em um arquivo JSON local.
2. **create_table_files**: recupera informações detalhadas sobre as tabelas associadas aos schemas obtidos e as salva como arquivos JSON individuais.
3. **extract_and_save_lineage**: extrai a linhagem para cada tabela a partir dos arquivos JSON obtidos anteriormente e salva os resultados em um arquivo JSON local.
4. **delete_lineage**: processa os resultados de linhagem para identificar tabelas relacionadas e deleta a linhagem entre elas usando a API do OpenMetadata.

## Notas Adicionais
- Personalize as funções de acordo com seus requisitos específicos.
- Utilize o ambiente de sandbox do OpenMetadata para testar requisições de API antes de implementá-las em ambientes de produção.

Para mais detalhes e explicações, consulte os comentários inline dentro do script.