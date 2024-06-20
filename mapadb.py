import argparse
import subprocess
import os

from libs.pipelines import atualizarBanco

def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Script para testar implantação do banco de dados MAPA UFV")
    parser.add_argument('--dbname', required=True, help='Nome do banco de dados')
    parser.add_argument('--user', required=True, help='Nome do usuário')
    parser.add_argument('--password', required=True, help='Senha do usuário')
    parser.add_argument('--host', default='localhost', help='Host do banco de dados')
    parser.add_argument('--port', type=int, default=5432, help='Porta do banco de dados')

    # Parse arguments
    args = parser.parse_args()

    # Populate db_params dictionary
    parametros = {
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port
    }

    atualizarBanco(parametros, remake=True)

if __name__ == "__main__":
    main()