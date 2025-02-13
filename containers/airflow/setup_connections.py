import subprocess


def add_dwh_mysql_connection() -> None:
    """
    Add a MySQL data warehouse connection to Airflow using the CLI.

    This function sets up a connection to a MySQL data warehouse by defining
    the necessary connection details and executing the corresponding Airflow CLI command.
    """
    # Define connection details
    connection_id = "dwh-conn"
    connection_type = "mysql"
    host = "mysql-dwh"
    port = "3306"
    login = "root"
    password = "1234"
    schema = "sales_analytics_db"

    # Define the CLI command
    cmd = [
        "airflow",
        "connections",
        "add",
        connection_id,
        "--conn-type",
        connection_type,
        "--conn-host",
        host,
        "--conn-port",
        port,
        "--conn-login",
        login,
        "--conn-password",
        password,
        "--conn-schema",
        schema,
    ]

    # Execute the command
    subprocess.run(cmd, capture_output=True, text=True)


add_dwh_mysql_connection()
