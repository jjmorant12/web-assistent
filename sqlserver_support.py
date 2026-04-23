import logging
import os
import socket

try:
    import winreg
except ImportError:  # Streamlit Community Cloud runs on Linux and has no Windows registry.
    winreg = None

try:
    import pytds
except ImportError:
    pytds = None


if pytds is not None:
    logging.getLogger("pytds").setLevel(logging.WARNING)


DEFAULT_SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "127.0.0.1")
DEFAULT_SQLSERVER_INSTANCE = os.getenv("SQLSERVER_INSTANCE", "SQLEXPRESS")
DEFAULT_SQLSERVER_USER = os.getenv("SQLSERVER_USER", "taidibei_user")
DEFAULT_SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "Tdb@2026SQL!")
DEFAULT_SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "financial_data")


CORE_TABLE_SQL = """
IF OBJECT_ID(N'dbo.core_performance_indicators_sheet', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.core_performance_indicators_sheet (
        serial_number INT IDENTITY(1,1) PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        stock_abbr VARCHAR(50) NOT NULL,
        eps DECIMAL(10,4) NULL,
        total_operating_revenue DECIMAL(20,2) NULL,
        operating_revenue_yoy_growth DECIMAL(10,4) NULL,
        operating_revenue_qoq_growth DECIMAL(10,4) NULL,
        net_profit_10k_yuan DECIMAL(20,2) NULL,
        net_profit_yoy_growth DECIMAL(10,4) NULL,
        net_profit_qoq_growth DECIMAL(10,4) NULL,
        net_asset_per_share DECIMAL(10,4) NULL,
        roe DECIMAL(10,4) NULL,
        operating_cf_per_share DECIMAL(10,4) NULL,
        net_profit_excl_non_recurring DECIMAL(20,2) NULL,
        net_profit_excl_non_recurring_yoy DECIMAL(10,4) NULL,
        gross_profit_margin DECIMAL(10,4) NULL,
        net_profit_margin DECIMAL(10,4) NULL,
        roe_weighted_excl_non_recurring DECIMAL(10,4) NULL,
        report_period VARCHAR(20) NOT NULL,
        report_year INT NOT NULL
    );
END
"""


BALANCE_TABLE_SQL = """
IF OBJECT_ID(N'dbo.balance_sheet', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.balance_sheet (
        serial_number INT IDENTITY(1,1) PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        stock_abbr VARCHAR(50) NOT NULL,
        asset_cash_and_cash_equivalents DECIMAL(20,2) NULL,
        asset_accounts_receivable DECIMAL(20,2) NULL,
        asset_inventory DECIMAL(20,2) NULL,
        asset_trading_financial_assets DECIMAL(20,2) NULL,
        asset_construction_in_progress DECIMAL(20,2) NULL,
        asset_total_assets DECIMAL(20,2) NULL,
        asset_total_assets_yoy_growth DECIMAL(10,4) NULL,
        liability_accounts_payable DECIMAL(20,2) NULL,
        liability_advance_from_customers DECIMAL(20,2) NULL,
        liability_total_liabilities DECIMAL(20,2) NULL,
        liability_total_liabilities_yoy_growth DECIMAL(10,4) NULL,
        liability_contract_liabilities DECIMAL(20,2) NULL,
        liability_short_term_loans DECIMAL(20,2) NULL,
        asset_liability_ratio DECIMAL(10,4) NULL,
        equity_unappropriated_profit DECIMAL(20,2) NULL,
        equity_total_equity DECIMAL(20,2) NULL,
        report_period VARCHAR(20) NOT NULL,
        report_year INT NOT NULL
    );
END
"""


CASH_FLOW_TABLE_SQL = """
IF OBJECT_ID(N'dbo.cash_flow_sheet', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cash_flow_sheet (
        serial_number INT IDENTITY(1,1) PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        stock_abbr VARCHAR(50) NOT NULL,
        net_cash_flow DECIMAL(20,2) NULL,
        net_cash_flow_yoy_growth DECIMAL(10,4) NULL,
        operating_cf_net_amount DECIMAL(20,2) NULL,
        operating_cf_ratio_of_net_cf DECIMAL(10,4) NULL,
        operating_cf_cash_from_sales DECIMAL(20,2) NULL,
        investing_cf_net_amount DECIMAL(20,2) NULL,
        investing_cf_ratio_of_net_cf DECIMAL(10,4) NULL,
        investing_cf_cash_for_investments DECIMAL(20,2) NULL,
        investing_cf_cash_from_investment_recovery DECIMAL(20,2) NULL,
        financing_cf_cash_from_borrowing DECIMAL(20,2) NULL,
        financing_cf_cash_for_debt_repayment DECIMAL(20,2) NULL,
        financing_cf_net_amount DECIMAL(20,2) NULL,
        financing_cf_ratio_of_net_cf DECIMAL(10,4) NULL,
        report_period VARCHAR(20) NOT NULL,
        report_year INT NOT NULL
    );
END
"""


INCOME_TABLE_SQL = """
IF OBJECT_ID(N'dbo.income_sheet', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.income_sheet (
        serial_number INT IDENTITY(1,1) PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        stock_abbr VARCHAR(50) NOT NULL,
        net_profit DECIMAL(20,2) NULL,
        net_profit_yoy_growth DECIMAL(10,4) NULL,
        other_income DECIMAL(20,2) NULL,
        total_operating_revenue DECIMAL(20,2) NULL,
        operating_revenue_yoy_growth DECIMAL(10,4) NULL,
        operating_expense_cost_of_sales DECIMAL(20,2) NULL,
        operating_expense_selling_expenses DECIMAL(20,2) NULL,
        operating_expense_administrative_expenses DECIMAL(20,2) NULL,
        operating_expense_financial_expenses DECIMAL(20,2) NULL,
        operating_expense_rnd_expenses DECIMAL(20,2) NULL,
        operating_expense_taxes_and_surcharges DECIMAL(20,2) NULL,
        total_operating_expenses DECIMAL(20,2) NULL,
        operating_profit DECIMAL(20,2) NULL,
        total_profit DECIMAL(20,2) NULL,
        asset_impairment_loss DECIMAL(20,2) NULL,
        credit_impairment_loss DECIMAL(20,2) NULL,
        report_period VARCHAR(20) NOT NULL,
        report_year INT NOT NULL
    );
END
"""


TABLE_CREATION_SQL = [
    CORE_TABLE_SQL,
    BALANCE_TABLE_SQL,
    CASH_FLOW_TABLE_SQL,
    INCOME_TABLE_SQL,
]


def _read_reg_value(path, value_name):
    if winreg is None:
        raise RuntimeError("当前环境不支持 Windows 注册表，无法自动探测 SQL Server 实例端口")
    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path) as key:
        value, _ = winreg.QueryValueEx(key, value_name)
        return value


def get_instance_registry_id(instance_name):
    path = r"SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL"
    return _read_reg_value(path, instance_name)


def discover_sqlserver_port(instance_name=DEFAULT_SQLSERVER_INSTANCE):
    instance_id = get_instance_registry_id(instance_name)
    ipall_path = (
        rf"SOFTWARE\Microsoft\Microsoft SQL Server\{instance_id}"
        rf"\MSSQLServer\SuperSocketNetLib\Tcp\IPAll"
    )
    tcp_port = _read_reg_value(ipall_path, "TcpPort")
    if tcp_port:
        return int(tcp_port)
    dynamic_port = _read_reg_value(ipall_path, "TcpDynamicPorts")
    if dynamic_port:
        return int(dynamic_port)
    return None


def normalize_sqlserver_config(db_config):
    config = dict(db_config)
    config.setdefault("host", DEFAULT_SQLSERVER_HOST)
    config.setdefault("instance", DEFAULT_SQLSERVER_INSTANCE)
    config.setdefault("user", DEFAULT_SQLSERVER_USER)
    config.setdefault("password", DEFAULT_SQLSERVER_PASSWORD)
    config.setdefault("database", DEFAULT_SQLSERVER_DATABASE)
    port = config.get("port")
    if port in ("", None):
        try:
            port = discover_sqlserver_port(config["instance"])
        except Exception:
            port = None
    config["port"] = int(port) if port else None
    return config


def connect_sqlserver(db_config, database=None):
    if pytds is None:
        raise RuntimeError("未安装 python-tds，无法连接 SQL Server")
    config = normalize_sqlserver_config(db_config)
    return pytds.connect(
        dsn=config["host"],
        port=config["port"],
        database=database or config["database"],
        user=config["user"],
        password=config["password"],
        enc_login_only=True,
        login_timeout=10,
    )


def ensure_database_exists(db_config):
    config = normalize_sqlserver_config(db_config)
    db_name = config["database"].replace("]", "]]")
    conn = connect_sqlserver(config, database="master")
    try:
        cursor = conn.cursor()
        cursor.execute(f"IF DB_ID(N'{db_name}') IS NULL CREATE DATABASE [{db_name}]")
        conn.commit()
    finally:
        conn.close()


def ensure_financial_tables(connection):
    cursor = connection.cursor()
    try:
        for statement in TABLE_CREATION_SQL:
            cursor.execute(statement)
        connection.commit()
    finally:
        cursor.close()


def fetchall_dicts(cursor):
    columns = [column[0] for column in cursor.description] if cursor.description else []
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def default_sqlserver_config():
    return normalize_sqlserver_config(
        {
            "host": DEFAULT_SQLSERVER_HOST,
            "instance": DEFAULT_SQLSERVER_INSTANCE,
            "user": DEFAULT_SQLSERVER_USER,
            "password": DEFAULT_SQLSERVER_PASSWORD,
            "database": DEFAULT_SQLSERVER_DATABASE,
            "port": os.getenv("SQLSERVER_PORT"),
        }
    )


def describe_sqlserver_endpoint(db_config):
    config = normalize_sqlserver_config(db_config)
    host = config["host"]
    port = config["port"]
    instance = config["instance"]
    machine = socket.gethostname()
    return {
        "host": host,
        "port": port,
        "instance": instance,
        "machine": machine,
        "database": config["database"],
        "user": config["user"],
    }
