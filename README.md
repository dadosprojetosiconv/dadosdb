# Biblioteca de extração de dados do SICONV

Os dados são extraídos do diretório https://repositorio.dados.gov.br/seges/detru/

## Estrutura do Repositório

- `mapadb.py`: Script principal que orquestra a execução de todos os scripts.
- `libs/`: Diretório que contém diversos scripts nomeados de acordo com o propósito principal de cada um.
- `libs/pipeline.py`: Contém as funções principais para a coleta e processamento dos dados, chamando as funções dos demais scripts em `libs`.
- `libs/download.py`: Contém as funções para download dos dados do repositório SICONV
- `libs/sql.py`: Contém as funções para adicionar elementos de relacionamento e indexação das tabelas: ID e Chaves estrangeiras.
- `libs/tratamentos.py`: Contém as funções para formatação, filtragem e transformação dos dados, garantindo que somente dados relevantes para o MAPA e adequados sejam passados.

## Instalação e Configuração

1. Clone o repositório:
    ```bash
    git clone https://github.com/seu-usuario/mapadb.git
    cd mapadb
    ```

2. Instale as dependências necessárias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso

### Executando o Script Principal

O primeiro passo é editar o script localizado em libs/mapaUFV_database.sh (se windows, a extensão é .bat). Nele, devem ser alterados os parâmetros DBNAME e USER para o nome desejado para a base e o nome de usuário do PostgreSQL no qual ela será criada.

Depois, deve-se abrir o arquivo `sql/mapaUFV_DADOS.sql` e modificar os caminhos para absolutos. Ou seja, ao invés de `output/arquivo.csv`, deve-se usar algo como `C:/MAPA/output/arquivo.csv`. Observe que pode ser necessário dar permissão ao usuário Postgres para acessar a pasta.

Se estiver usando windows, é necessário descomentar a linha 173 do arquivo `libs/pipelines.py` e comentar a linha 172. Caso contrário, deixe a linha 172 descomentada e comente a linha 173.

Ao executar o script principal `mapadb.py`, é necessário ter em mãos as seguintes informações:
- DBNAME = Nome do banco de dados 
- USER   = Nome do usuário PostgreSQL
- PASS   = Senha do usuário do PostgreSQL
- HOST   = Endereço do HOST
- PORT   = Número da Porta usada pelo PostgreSQL

Para executar o script principal `mapadb.py`, basta rodar o seguinte comando:
```bash
python mapadb.py --dbname DBNAME --port PORT --user USER --password PASS --host HOST
