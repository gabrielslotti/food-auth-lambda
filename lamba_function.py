import psycopg2
import os

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DB_NAME = os.getenv('DB_DB_NAME')


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DB_NAME
    )


def lambda_handler(event, context):
    cpf = event.get("cpf")

    if not cpf:
        return {
            "statusCode": 400,
            "message": "CPF não fornecido"
        }

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        sql = "SELECT id, cpf, first_name, last_name, email FROM customer HERE cpf = '%s'"  # noqa
        cursor.execute(sql, (cpf,))
        result = cursor.fetchone()

        if result:
            cols = ['id', 'cpf', 'first_name', 'last_name', 'email']
            data = dict(zip(cols, result))
            return {
                "statusCode": 200,
                "data": data,
                "message": "Autenticação realizada com sucesso!"
            }
        else:
            return {
                "statusCode": 401,
                "message": "CPF não existente!"
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e),
            "message": f"Erro ao acessar o banco de dados: {str(e)}"
        }

    finally:
        if connection:
            cursor.close()
            connection.close()
