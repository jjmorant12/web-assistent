import argparse
import math
import re
import logging
import os
import sys
import zipfile
from functools import lru_cache
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET
import fitz  # PyMuPDF

from data_paths import ATTACHMENT1_PATH, ATTACHMENT2_REPORTS_ROOT
from sqlserver_support import (
    DEFAULT_SQLSERVER_DATABASE,
    DEFAULT_SQLSERVER_HOST,
    DEFAULT_SQLSERVER_INSTANCE,
    DEFAULT_SQLSERVER_PASSWORD,
    DEFAULT_SQLSERVER_USER,
    connect_sqlserver,
    ensure_database_exists,
    ensure_financial_tables,
)

# 配置日志
log_handlers = [logging.StreamHandler()]
try:
    log_handlers.insert(0, logging.FileHandler("financial_data_processor_complete.log", encoding="utf-8"))
except PermissionError:
    print("警告：无法写入 financial_data_processor_complete.log，已切换为仅控制台输出日志。")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger("FinancialDataProcessorComplete")

DEFAULT_PDF_PATH = str(ATTACHMENT2_REPORTS_ROOT)
EXCEL_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def parse_args():
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="批量提取 PDF 财报并写入 SQL Server，目录会递归扫描所有 PDF。"
    )
    parser.add_argument(
        "pdf_path",
        nargs="?",
        default=DEFAULT_PDF_PATH,
        help="PDF 文件或目录路径；如果是目录，会递归扫描全部子目录中的 PDF。"
    )
    parser.add_argument("--db-host", default=os.getenv("SQLSERVER_HOST", DEFAULT_SQLSERVER_HOST), help="SQL Server 主机地址")
    parser.add_argument("--db-instance", default=os.getenv("SQLSERVER_INSTANCE", DEFAULT_SQLSERVER_INSTANCE), help="SQL Server 实例名")
    parser.add_argument("--db-port", type=int, default=os.getenv("SQLSERVER_PORT"), help="SQL Server 端口，留空时自动探测 SQLEXPRESS 端口")
    parser.add_argument("--db-user", default=os.getenv("SQLSERVER_USER", DEFAULT_SQLSERVER_USER), help="SQL Server 用户名")
    parser.add_argument(
        "--db-password",
        default=os.getenv("SQLSERVER_PASSWORD", DEFAULT_SQLSERVER_PASSWORD),
        help="SQL Server 密码"
    )
    parser.add_argument(
        "--db-name",
        default=os.getenv("SQLSERVER_DATABASE", DEFAULT_SQLSERVER_DATABASE),
        help="SQL Server 数据库名"
    )
    return parser.parse_args()


def collect_pdf_files(pdf_path):
    """收集单个 PDF 或目录下全部 PDF 文件。"""
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"路径不存在：{pdf_path}")

    if os.path.isfile(pdf_path):
        if not pdf_path.lower().endswith(".pdf"):
            raise ValueError(f"指定文件不是 PDF：{pdf_path}")
        return [os.path.abspath(pdf_path)]

    if os.path.isdir(pdf_path):
        pdf_files = []
        for root, _, files in os.walk(pdf_path):
            for filename in files:
                if filename.lower().endswith(".pdf"):
                    pdf_files.append(os.path.join(root, filename))
        pdf_files.sort()
        return pdf_files

    raise ValueError(f"无效的路径：{pdf_path}")


class FinancialDataProcessorComplete:
    IDENTITY_FIELDS = ('stock_code', 'stock_abbr', 'report_period', 'report_year')
    DUPLICATE_IDENTITY_FIELDS = ('stock_code', 'report_period', 'report_year')
    INTERNAL_META_KEYS = ('_consistency_checks', '_record_quality', '_quality_flags')
    CONSISTENCY_RULES = {
        'balance_equation': {
            'warn_ratio': 0.01,
            'low_quality_ratio': 0.05,
            'absolute_tolerance': 1.0,
        },
        'net_profit_cross_table': {
            'warn_ratio': 0.05,
            'low_quality_ratio': 0.20,
            'absolute_tolerance': 1.0,
        },
        'revenue_cross_table': {
            'warn_ratio': 0.03,
            'low_quality_ratio': 0.10,
            'absolute_tolerance': 1.0,
        },
    }
    QUALITY_SCORE_KEY_FIELD_WEIGHTS = {
        'asset_total_assets': 8,
        'liability_total_liabilities': 8,
        'equity_total_equity': 8,
        'asset_liability_ratio': 5,
        'net_profit': 8,
        'total_profit': 7,
        'total_operating_revenue': 8,
        'operating_revenue_yoy_growth': 5,
        'net_profit_yoy_growth': 5,
        'net_profit_10k_yuan': 8,
        'operating_revenue_qoq_growth': 4,
        'net_profit_qoq_growth': 4,
        'eps': 4,
        'roe': 4,
        'net_cash_flow': 7,
        'net_cash_flow_yoy_growth': 5,
        'operating_cf_net_amount': 7,
        'financing_cf_net_amount': 5,
        'investing_cf_net_amount': 5,
    }
    QUALITY_SCORE_RULES = {
        'non_null_point': 1.0,
        'valid_numeric_point': 0.5,
        'invalid_numeric_penalty': 6.0,
        'core_field_point': 3.0,
        'core_invalid_penalty': 15.0,
        'core_anomaly_penalty': 12.0,
        'warning_penalty': 8.0,
        'low_quality_penalty': 20.0,
        'quality_flag_warning_penalty': 5.0,
        'quality_flag_low_quality_penalty': 12.0,
        'record_low_quality_penalty': 10.0,
    }
    UNIT_NORMALIZATION_RULES = {
        'total_operating_revenue': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 1e8},
        'net_profit': {'yuan_threshold': 1e7, 'max_abs_wanyuan': 2e7},
        'net_profit_10k_yuan': {'yuan_threshold': 1e7, 'max_abs_wanyuan': 2e7},
        'total_profit': {'yuan_threshold': 1e7, 'max_abs_wanyuan': 2e7},
        'operating_profit': {'yuan_threshold': 1e7, 'max_abs_wanyuan': 2e7},
        'net_cash_flow': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e7},
        'operating_cf_net_amount': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e7},
        'investing_cf_net_amount': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e7},
        'financing_cf_net_amount': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e7},
        'asset_total_assets': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e8},
        'liability_total_liabilities': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e8},
        'equity_total_equity': {'yuan_threshold': 1e8, 'max_abs_wanyuan': 5e8},
    }
    CORE_DUPLICATE_PRIORITY_FIELDS = (
        'total_operating_revenue',
        'net_profit',
        'total_profit',
        'net_profit_10k_yuan',
    )
    REPORT_PERIOD_VALUES = ('Q1', 'HY', 'Q3', 'FY')
    YEAR_RANGE = (2000, 2030)
    STOCK_CODE_PRIMARY_PATTERNS = (
        (r'股票代码[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'证券代码[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'公司代码[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'股票代码\s*[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'证券代码\s*[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'公司代码\s*[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'股票代码\s*\n\s*([0-9]{6})', lambda m: m.group(1)),
        (r'证券代码\s*\n\s*([0-9]{6})', lambda m: m.group(1)),
        (r'公司代码\s*\n\s*([0-9]{6})', lambda m: m.group(1)),
        (r'股票代码\s*[（(][^)）]*[)）]\s*[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'证券代码\s*[（(][^)）]*[)）]\s*[:：]\s*([0-9]{6})', lambda m: m.group(1)),
        (r'公司代码\s*[（(][^)）]*[)）]\s*[:：]\s*([0-9]{6})', lambda m: m.group(1)),
    )
    STOCK_CODE_START_PATTERNS = (
        (r'股票代码[:：]?\s*([0-9]{6})', lambda m: m.group(1)),
        (r'证券代码[:：]?\s*([0-9]{6})', lambda m: m.group(1)),
        (r'公司代码[:：]?\s*([0-9]{6})', lambda m: m.group(1)),
        (r'代码[:：]?\s*([0-9]{6})', lambda m: m.group(1)),
    )
    STOCK_CODE_FALLBACK_PATTERN = r'\b([0-9]{6})\b'
    INVALID_STOCK_ABBR_VALUES = {
        '股票代码', '证券代码', '公司代码', '代码', '股票',
        '证券', '简称', '名称', '公司', '股份'
    }
    INVALID_NORMALIZED_STOCK_ABBR_VALUES = {
        'ST', '*ST', '营业收入', '主营业务收入', '营业总收入', '净利润', '利润总额',
        '股票简称', '证券简称', '公司简称', '公司名称', '股票名称', '报告期', '报告年度',
    }
    COMPANY_INFO_SHEET_CANDIDATES = ('基本信息表',)
    COMPANY_INFO_CODE_COLUMNS = ('股票代码', '证券代码', '公司代码')
    COMPANY_INFO_ABBR_COLUMNS = ('A股简称', '股票简称', '证券简称', '公司简称')
    STOCK_ABBR_PRIMARY_PATTERNS = (
        (r'公司简称[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'公司简称\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'股票简称[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'股票简称\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'证券简称[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'证券简称\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'股票简称[\s\S]{0,200}?\n[\s\S]{0,100}?([^\s:：\n]{2,20})[\s\S]{0,100}?\n[\s\S]{0,100}?股票代码', lambda m: m.group(1)),
        (r'证券简称[\s\S]{0,200}?\n[\s\S]{0,100}?([^\s:：\n]{2,20})[\s\S]{0,100}?\n[\s\S]{0,100}?股票代码', lambda m: m.group(1)),
        (r'公司简称[\s\S]{0,200}?\n[\s\S]{0,100}?([^\s:：\n]{2,20})[\s\S]{0,100}?\n[\s\S]{0,100}?股票代码', lambda m: m.group(1)),
        (r'股票简称\s*\n\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'证券简称\s*\n\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'公司简称\s*\n\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'股票简称\s*[（(][^)）]*[)）]\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'证券简称\s*[（(][^)）]*[)）]\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'公司简称\s*[（(][^)）]*[)）]\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'简称\s*[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'证券名称[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'股票名称[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'公司名称[:：]\s*([^\s:：\n]+)', lambda m: m.group(1)),
    )
    STOCK_ABBR_START_PATTERNS = (
        (r'公司简称[:：]?\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'股票简称[:：]?\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'证券简称[:：]?\s*([^\s:：\n]+)', lambda m: m.group(1)),
        (r'简称[:：]?\s*([^\s:：\n]+)', lambda m: m.group(1)),
    )
    STOCK_ABBR_NEARBY_PATTERN = r'([^\s:：\n]{2,10})'
    REPORT_INFO_TITLE_PATTERNS = (
        (r'(20[0-9]{2})\s*年年度报告', lambda m: ('FY', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年半年度报告', lambda m: ('HY', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年第一季度报告', lambda m: ('Q1', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年第三季度报告', lambda m: ('Q3', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年中期报告', lambda m: ('HY', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年年报', lambda m: ('FY', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年半年报', lambda m: ('HY', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年一季报', lambda m: ('Q1', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年三季报', lambda m: ('Q3', int(m.group(1)))),
        (r'(20[0-9]{2})\s*年中报', lambda m: ('HY', int(m.group(1)))),
    )
    REPORT_PERIOD_PATTERNS = (
        (r'年年度报告', lambda m: 'FY'),
        (r'年半年度报告', lambda m: 'HY'),
        (r'第一季度报告', lambda m: 'Q1'),
        (r'第三季度报告', lambda m: 'Q3'),
        (r'中期报告', lambda m: 'HY'),
        (r'年报', lambda m: 'FY'),
        (r'半年报', lambda m: 'HY'),
        (r'一季报', lambda m: 'Q1'),
        (r'三季报', lambda m: 'Q3'),
        (r'中报', lambda m: 'HY'),
        (r'年年度', lambda m: 'FY'),
        (r'年半年度', lambda m: 'HY'),
        (r'一季度报告', lambda m: 'Q1'),
        (r'三季度报告', lambda m: 'Q3'),
        (r'一季度', lambda m: 'Q1'),
        (r'三季度', lambda m: 'Q3'),
        (r'报告期\s*[:：]?\s*(Q1|HY|Q3|FY)', lambda m: m.group(1)),
        (r'期间\s*[:：]?\s*(Q1|HY|Q3|FY)', lambda m: m.group(1)),
        (r'\b(FY|HY|Q1|Q3)\b', lambda m: m.group(1)),
        (r'Q1|HY|Q3|FY', lambda m: m.group(0)),
        (r'20[0-9]{2}年\s*(第一|第三)季度', lambda m: 'Q1' if '第一' in m.group(1) else 'Q3'),
        (r'20[0-9]{2}年\s*年半年度', lambda m: 'HY'),
        (r'20[0-9]{2}年\s*年年度', lambda m: 'FY'),
    )
    REPORT_PERIOD_KEYWORDS = {
        'FY': ('年年度报告', '年年度', '年报'),
        'HY': ('年半年度报告', '半年报', '中期报告', '中报', '年半年度'),
        'Q1': ('第一季度报告', '一季报', '一季度报告', '一季度'),
        'Q3': ('第三季度报告', '三季报', '三季度报告', '三季度'),
    }
    REPORT_YEAR_TITLE_PATTERNS = (
        r'(20[0-9]{2})\s*年年度报告',
        r'(20[0-9]{2})\s*年半年度报告',
        r'(20[0-9]{2})\s*年第一季度报告',
        r'(20[0-9]{2})\s*年第三季度报告',
        r'(20[0-9]{2})\s*年中期报告',
        r'(20[0-9]{2})\s*年年报',
        r'(20[0-9]{2})\s*年半年报',
        r'(20[0-9]{2})\s*年一季报',
        r'(20[0-9]{2})\s*年三季报',
        r'(20[0-9]{2})\s*年中报',
        r'([0-9]{4})\s*年.*?报告',
        r'报告.*?([0-9]{4})\s*年',
        r'([0-9]{4})\s*年第[一二三四]季度报告',
        r'([0-9]{4})\s*年[一二三四]季度报告',
        r'([0-9]{4})\s*年Q[1-4]报告',
        r'([0-9]{4})\s*年半年度报告',
        r'([0-9]{4})\s*年中期报告',
        r'([0-9]{4})\s*年年度报告',
        r'([0-9]{4})\s*年年报',
    )
    REPORT_YEAR_START_PATTERNS = (
        r'报告期\s*[:：]?\s*(20[0-9]{2})',
        r'报告期间\s*[:：]?\s*(20[0-9]{2})',
        r'期间\s*[:：]?\s*(20[0-9]{2})',
        r'年度\s*[:：]?\s*(20[0-9]{2})',
        r'截至\s*[:：]?\s*(20[0-9]{2})',
        r'日期\s*[:：]?\s*(20[0-9]{2})',
        r'发布日期\s*[:：]?\s*(20[0-9]{2})',
        r'公告日期\s*[:：]?\s*(20[0-9]{2})',
        r'合并资产负债表\s*(20[0-9]{2})',
        r'资产负债表\s*(20[0-9]{2})',
        r'利润表\s*(20[0-9]{2})',
        r'现金流量表\s*(20[0-9]{2})',
        r'([0-9]{4})\s*年.*?财务报告',
        r'财务报告.*?([0-9]{4})\s*年',
        r'([0-9]{4})\s*年度.*?报告',
        r'报告.*?([0-9]{4})\s*年度',
    )
    REPORT_YEAR_FALLBACK_PATTERN = r'20[0-9]{2}'
    TABLE_FIELDS = {
        'core_performance_indicators_sheet': [
            'stock_code', 'stock_abbr', 'eps', 'total_operating_revenue',
            'operating_revenue_yoy_growth', 'operating_revenue_qoq_growth',
            'net_profit_10k_yuan', 'net_profit_yoy_growth', 'net_profit_qoq_growth',
            'net_asset_per_share', 'roe', 'operating_cf_per_share',
            'net_profit_excl_non_recurring', 'net_profit_excl_non_recurring_yoy',
            'gross_profit_margin', 'net_profit_margin', 'roe_weighted_excl_non_recurring',
            'report_period', 'report_year'
        ],
        'balance_sheet': [
            'stock_code', 'stock_abbr', 'asset_cash_and_cash_equivalents',
            'asset_accounts_receivable', 'asset_inventory', 'asset_trading_financial_assets',
            'asset_construction_in_progress', 'asset_total_assets', 'asset_total_assets_yoy_growth',
            'liability_accounts_payable', 'liability_advance_from_customers', 'liability_total_liabilities',
            'liability_total_liabilities_yoy_growth', 'liability_contract_liabilities',
            'liability_short_term_loans', 'asset_liability_ratio', 'equity_unappropriated_profit',
            'equity_total_equity', 'report_period', 'report_year'
        ],
        'cash_flow_sheet': [
            'stock_code', 'stock_abbr', 'net_cash_flow', 'net_cash_flow_yoy_growth',
            'operating_cf_net_amount', 'operating_cf_ratio_of_net_cf', 'operating_cf_cash_from_sales',
            'investing_cf_net_amount', 'investing_cf_ratio_of_net_cf', 'investing_cf_cash_for_investments',
            'investing_cf_cash_from_investment_recovery', 'financing_cf_cash_from_borrowing',
            'financing_cf_cash_for_debt_repayment', 'financing_cf_net_amount',
            'financing_cf_ratio_of_net_cf', 'report_period', 'report_year'
        ],
        'income_sheet': [
            'stock_code', 'stock_abbr', 'net_profit', 'net_profit_yoy_growth',
            'other_income', 'total_operating_revenue', 'operating_revenue_yoy_growth',
            'operating_expense_cost_of_sales', 'operating_expense_selling_expenses',
            'operating_expense_administrative_expenses', 'operating_expense_financial_expenses',
            'operating_expense_rnd_expenses', 'operating_expense_taxes_and_surcharges',
            'total_operating_expenses', 'operating_profit', 'total_profit',
            'asset_impairment_loss', 'credit_impairment_loss', 'report_period', 'report_year'
        ],
    }

    def __init__(self, db_config):
        """
        初始化财务数据处理器

        :param db_config: SQL Server 数据库配置字典，包含 host, instance, port, user, password, database
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self._active_text_cache_key = None
        self._regex_findall_cache = {}
        self._data_extract_cache = {}
        self.company_abbr_mapping = self.load_company_abbr_mapping(Path(ATTACHMENT1_PATH))
        self.initialize_db_connection()

    def initialize_db_connection(self):
        """初始化数据库连接"""
        try:
            ensure_database_exists(self.db_config)
            self.connection = connect_sqlserver(self.db_config)
            self.cursor = self.connection.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接错误: {e}")
            raise

    def create_tables(self):
        """创建四个财务报表表结构"""
        try:
            ensure_financial_tables(self.connection)
            self.cursor = self.connection.cursor()
            logger.info("所有表创建成功")
        except Exception as e:
            logger.error(f"创建表时出错: {e}")
            raise

    def _build_text_cache_key(self, text_content):
        """Build a lightweight cache key for the active PDF text."""
        return (id(text_content), len(text_content))

    def _ensure_text_runtime_cache(self, text_content):
        """Reset per-document caches when switching to a new PDF text."""
        cache_key = self._build_text_cache_key(text_content)
        if cache_key != self._active_text_cache_key:
            self._active_text_cache_key = cache_key
            self._regex_findall_cache.clear()
            self._data_extract_cache.clear()
        return cache_key

    def _findall_cached(self, pattern, text_content):
        """Cache expensive regex scans for the current PDF text."""
        self._ensure_text_runtime_cache(text_content)
        if pattern not in self._regex_findall_cache:
            self._regex_findall_cache[pattern] = re.findall(pattern, text_content)
        return self._regex_findall_cache[pattern]

    def _read_xlsx(self, path):
        workbook = {}
        with zipfile.ZipFile(path) as zf:
            shared_strings = []
            if "xl/sharedStrings.xml" in zf.namelist():
                root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                for si in root.findall("a:si", EXCEL_NS):
                    shared_strings.append("".join(t.text or "" for t in si.findall(".//a:t", EXCEL_NS)))

            wb = ET.fromstring(zf.read("xl/workbook.xml"))
            rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
            rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rel_root}

            for sheet in wb.findall("a:sheets/a:sheet", EXCEL_NS):
                name = sheet.attrib["name"]
                rid = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
                target = rel_map[rid]
                xml = ET.fromstring(zf.read("xl/" + target))
                rows = []
                for row in xml.findall("a:sheetData/a:row", EXCEL_NS):
                    cell_map = {}
                    max_col = 0
                    for cell in row.findall("a:c", EXCEL_NS):
                        ref = cell.attrib.get("r", "")
                        col_name = "".join(ch for ch in ref if ch.isalpha())
                        idx = self._excel_col_to_num(col_name)
                        max_col = max(max_col, idx)
                        cell_type = cell.attrib.get("t")
                        v = cell.find("a:v", EXCEL_NS)
                        is_node = cell.find("a:is", EXCEL_NS)
                        value = ""
                        if cell_type == "s" and v is not None:
                            value = shared_strings[int(v.text)]
                        elif cell_type == "inlineStr" and is_node is not None:
                            value = "".join(t.text or "" for t in is_node.findall(".//a:t", EXCEL_NS))
                        elif v is not None:
                            value = v.text or ""
                        cell_map[idx] = value
                    rows.append([cell_map.get(i, "") for i in range(1, max_col + 1)])
                workbook[name] = rows
        return workbook

    def _excel_col_to_num(self, col_name):
        value = 0
        for char in col_name:
            if char.isalpha():
                value = value * 26 + ord(char.upper()) - 64
        return value

    def _rows_to_dicts(self, rows):
        if not rows:
            return []
        headers = rows[0]
        return [
            {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
            for row in rows[1:]
            if any(cell != "" for cell in row)
        ]

    def _format_stock_code(self, value):
        return str(value).strip().zfill(6)

    def load_company_abbr_mapping(self, attachment1_path):
        attachment1_path = Path(attachment1_path)
        mapping = {}
        if not attachment1_path.exists():
            logger.warning(f"附件1不存在，无法加载官方简称映射：{attachment1_path}")
            return mapping
        try:
            workbook = self._read_xlsx(attachment1_path)
            rows = None
            for sheet_name in self.COMPANY_INFO_SHEET_CANDIDATES:
                if sheet_name in workbook:
                    rows = workbook[sheet_name]
                    break
            if rows is None and workbook:
                rows = next(iter(workbook.values()))
            if not rows:
                logger.warning(f"附件1未读取到有效行，无法加载官方简称映射：{attachment1_path}")
                return mapping
            row_dicts = self._rows_to_dicts(rows)
            for row in row_dicts:
                stock_code = None
                for code_col in self.COMPANY_INFO_CODE_COLUMNS:
                    value = row.get(code_col, "").strip()
                    if value:
                        stock_code = self._format_stock_code(value)
                        break
                stock_abbr = None
                for abbr_col in self.COMPANY_INFO_ABBR_COLUMNS:
                    value = row.get(abbr_col, "").strip()
                    if value:
                        stock_abbr = value
                        break
                if stock_code and stock_abbr:
                    mapping[stock_code] = stock_abbr
            logger.info(f"加载附件1官方简称映射成功，共 {len(mapping)} 条：{attachment1_path}")
            return mapping
        except Exception as e:
            logger.warning(f"加载附件1官方简称映射失败：{attachment1_path}，原因：{e}")
            return {}

    @staticmethod
    @lru_cache(maxsize=4096)
    def _clean_number_cached(number_str):
        cleaned = number_str.replace(',', '')
        return float(cleaned)

    def extract_text_from_pdf(self, pdf_path):
        """
        从PDF文件中提取文本内容

        :param pdf_path: PDF文件路径
        :return: 提取的文本内容
        """
        try:
            doc = fitz.open(pdf_path)
            try:
                page_texts = [doc.load_page(page_num).get_text() for page_num in range(len(doc))]
            finally:
                doc.close()
            text = "".join(page_texts)
            self._ensure_text_runtime_cache(text)
            logger.info(f"成功从PDF文件 {pdf_path} 提取文本，长度: {len(text)} 字符")
            return text
        except Exception as e:
            logger.error(f"从PDF文件 {pdf_path} 提取文本时出错: {e}")
            raise

    def _prepare_matching_text(self, text_content, limit=None):
        clean_text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text_content)
        if limit is not None:
            return clean_text[:limit]
        return clean_text

    def _search_pattern_configs(self, text_content, pattern_configs, *,
                                validator=None, logger_debug_prefix=None):
        for pattern, converter in pattern_configs:
            match = re.search(pattern, text_content)
            if not match:
                continue
            try:
                value = converter(match)
            except Exception as e:
                if logger_debug_prefix:
                    logger.debug(f"{logger_debug_prefix}模式匹配失败：{pattern}, 错误：{e}")
                continue
            if validator is None or validator(value):
                return value
        return None

    def _find_all_pattern_values(self, text_content, pattern, *, converter=None, validator=None):
        values = []
        for match in re.findall(pattern, text_content):
            try:
                value = converter(match) if converter else match
            except Exception:
                continue
            if validator is None or validator(value):
                values.append(value)
        return values

    def _most_common_valid_year(self, text_content):
        all_year_matches = self._find_all_pattern_values(
            text_content,
            self.REPORT_YEAR_FALLBACK_PATTERN,
            converter=int,
            validator=self._is_valid_report_year,
        )
        if not all_year_matches:
            return None
        year_counts = {}
        for year in all_year_matches:
            year_counts[year] = year_counts.get(year, 0) + 1
        return max(year_counts, key=year_counts.get)

    def _is_valid_stock_code(self, stock_code):
        return (
            isinstance(stock_code, str)
            and len(stock_code) == 6
            and stock_code.isdigit()
            and stock_code[0] in ['0', '3', '6']
        )

    def _normalize_st_stock_abbr(self, stock_abbr):
        if not isinstance(stock_abbr, str):
            return stock_abbr
        normalized = re.sub(r'\s+', '', stock_abbr.strip())
        upper = normalized.upper()
        if upper == 'ST':
            return 'ST'
        if upper == '*ST':
            return '*ST'
        return normalized

    def _normalize_raw_stock_abbr_text(self, stock_abbr):
        if not isinstance(stock_abbr, str):
            return None
        cleaned = stock_abbr.strip()
        if not cleaned:
            return None
        cleaned = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', cleaned)
        cleaned = re.sub(r'\s+', '', cleaned)
        cleaned = cleaned.replace('（', '(').replace('）', ')')
        cleaned = self._normalize_st_stock_abbr(cleaned)
        return cleaned or None

    def _is_stock_abbr_candidate(self, stock_abbr):
        normalized = self._normalize_raw_stock_abbr_text(stock_abbr)
        return isinstance(normalized, str) and 0 < len(normalized) <= 20

    def _is_invalid_normalized_stock_abbr(self, stock_abbr):
        normalized = self._normalize_raw_stock_abbr_text(stock_abbr)
        if not normalized:
            return True
        if normalized in self.INVALID_STOCK_ABBR_VALUES:
            return True
        if normalized in self.INVALID_NORMALIZED_STOCK_ABBR_VALUES:
            return True
        if re.fullmatch(r'[\u4e00-\u9fff]', normalized):
            return True
        return False

    def _is_valid_stock_abbr(self, stock_abbr):
        normalized = self._normalize_raw_stock_abbr_text(stock_abbr)
        return (
            isinstance(normalized, str)
            and len(normalized) <= 20
            and not self._is_invalid_normalized_stock_abbr(normalized)
        )

    def normalize_stock_abbr(self, raw_abbr, stock_code, company_mapping=None):
        company_mapping = company_mapping or self.company_abbr_mapping
        normalized_raw_abbr = self._normalize_raw_stock_abbr_text(raw_abbr)
        official_abbr = None
        if stock_code:
            official_abbr = company_mapping.get(self._format_stock_code(stock_code))

        raw_is_valid = self._is_valid_stock_abbr(normalized_raw_abbr)
        used_mapping = False

        if official_abbr:
            normalized_stock_abbr = self._normalize_raw_stock_abbr_text(official_abbr)
            used_mapping = True
        elif raw_is_valid:
            normalized_stock_abbr = normalized_raw_abbr
        else:
            normalized_stock_abbr = None

        logger.info(
            "简称标准化: stock_code=%s raw_stock_abbr=%s normalized_stock_abbr=%s used_attachment1_mapping=%s",
            stock_code,
            raw_abbr,
            normalized_stock_abbr,
            used_mapping,
        )
        return normalized_stock_abbr, used_mapping

    def _is_valid_report_period(self, period):
        return period in self.REPORT_PERIOD_VALUES

    def _is_valid_report_year(self, year):
        min_year, max_year = self.YEAR_RANGE
        return isinstance(year, int) and min_year <= year <= max_year

    def extract_stock_code_from_text(self, text_content):
        """
        从PDF文本中提取股票代码

        :param text_content: PDF文本内容
        :return: 提取的股票代码
        """
        try:
            stock_code = self._search_pattern_configs(
                text_content,
                self.STOCK_CODE_PRIMARY_PATTERNS,
                validator=self._is_valid_stock_code,
            )
            if stock_code:
                logger.info(f"从PDF文本中提取到股票代码: {stock_code}")
                return stock_code

            text_start = self._prepare_matching_text(text_content, limit=2000)
            stock_code = self._search_pattern_configs(
                text_start,
                self.STOCK_CODE_START_PATTERNS,
                validator=self._is_valid_stock_code,
            )
            if stock_code:
                logger.info(f"从PDF文本开头提取到股票代码: {stock_code}")
                return stock_code

            all_six_digits = self._find_all_pattern_values(text_content, self.STOCK_CODE_FALLBACK_PATTERN)
            for stock_code in all_six_digits:
                if self._is_valid_stock_code(stock_code):
                    logger.info(f"从PDF文本中提取到股票代码: {stock_code}")
                    return stock_code
            if all_six_digits:
                logger.info(f"从PDF文本中提取到股票代码: {all_six_digits[0]}")
                return all_six_digits[0]

            logger.warning("未从PDF文本中提取到股票代码")
            return None
        except Exception as e:
            logger.error(f"提取股票代码时出错: {e}")
            return None

    def extract_stock_abbr_from_text(self, text_content):
        """
        从PDF文本中提取股票简称

        :param text_content: PDF文本内容
        :return: 提取的股票简称
        """
        try:
            stock_abbr = self._search_pattern_configs(
                text_content,
                self.STOCK_ABBR_PRIMARY_PATTERNS,
                validator=self._is_stock_abbr_candidate,
            )
            if stock_abbr:
                logger.info(f"从PDF文本中提取到股票简称: {stock_abbr}")
                return stock_abbr

            text_start = self._prepare_matching_text(text_content, limit=2000)
            stock_abbr = self._search_pattern_configs(
                text_start,
                self.STOCK_ABBR_START_PATTERNS,
                validator=self._is_stock_abbr_candidate,
            )
            if stock_abbr:
                logger.info(f"从PDF文本开头提取到股票简称: {stock_abbr}")
                return stock_abbr

            # 如果还没有找到，尝试从股票代码附近找股票简称
            # 先找到股票代码，然后看它前后的内容
            stock_code_matches = self._find_all_pattern_values(text_content, self.STOCK_CODE_FALLBACK_PATTERN)
            for stock_code in stock_code_matches:
                # 找到股票代码在文本中的位置
                for match in re.finditer(re.escape(stock_code), text_content):
                    # 查看股票代码前后200个字符
                    start_idx = max(0, match.start() - 200)
                    end_idx = min(len(text_content), match.end() + 200)
                    surrounding_text = text_content[start_idx:end_idx]
                    
                    # 在周围文本中找可能的股票简称
                    possible_abbrs = self._find_all_pattern_values(
                        surrounding_text,
                        self.STOCK_ABBR_NEARBY_PATTERN,
                    )
                    for abbr in possible_abbrs:
                        if self._is_stock_abbr_candidate(abbr) and len(self._normalize_raw_stock_abbr_text(abbr) or "") <= 10:
                            logger.info(f"从股票代码附近提取到股票简称: {abbr}")
                            return abbr

            logger.warning("未从PDF文本中提取到股票简称")
            return None
        except Exception as e:
            logger.error(f"提取股票简称时出错: {e}")
            return None

    def extract_report_info_from_text(self, text_content):
        """
        从 PDF 文本中同时提取报告期和报告年份
        
        :param text_content: PDF 文本内容
        :return: 字典 {'report_period': str, 'report_year': int}，失败返回 None
        """
        try:
            clean_text_start = self._prepare_matching_text(text_content, limit=10000)
            result = {
                'report_period': None,
                'report_year': None
            }
            title_info = self._search_pattern_configs(
                clean_text_start,
                self.REPORT_INFO_TITLE_PATTERNS,
                validator=lambda item: (
                    isinstance(item, tuple)
                    and len(item) == 2
                    and self._is_valid_report_period(item[0])
                    and self._is_valid_report_year(item[1])
                ),
                logger_debug_prefix='标题',
            )
            if title_info:
                result['report_period'], result['report_year'] = title_info
                logger.info(f"从 PDF 标题中提取到报告期：{result['report_period']}，年份：{result['report_year']}")
                return result

            result['report_period'] = self._search_pattern_configs(
                clean_text_start,
                self.REPORT_PERIOD_PATTERNS,
                validator=self._is_valid_report_period,
                logger_debug_prefix='报告期',
            )
            if result['report_period']:
                logger.info(f"从 PDF 文本中提取到报告期：{result['report_period']}")
            else:
                for period, keyword_list in self.REPORT_PERIOD_KEYWORDS.items():
                    if any(keyword in clean_text_start for keyword in keyword_list):
                        result['report_period'] = period
                        logger.info(f"从 PDF 文本关键词中提取到报告期：{period}")
                        break

            for pattern in self.REPORT_YEAR_START_PATTERNS:
                match = re.search(pattern, clean_text_start)
                if match:
                    year = int(match.group(1))
                    if self._is_valid_report_year(year):
                        result['report_year'] = year
                        logger.info(f"从 PDF 文本开头提取到报告年份：{year}")
                        break

            if not result['report_year']:
                most_common_year = self._most_common_valid_year(text_content)
                if most_common_year is not None:
                    result['report_year'] = most_common_year
                    logger.info(f"从 PDF 文本中提取到最常见的报告年份：{most_common_year}")
            
            # 检查是否至少提取到一个字段
            if result['report_period'] or result['report_year']:
                logger.info(f"提取结果 - 报告期：{result['report_period']}，年份：{result['report_year']}")
                return result
            else:
                logger.warning("未从 PDF 文本中提取到报告期或年份")
                return None
                
        except Exception as e:
            logger.error(f"提取报告信息时出错：{e}")
            return None

    def extract_report_period_from_text(self, text_content):
        """
        从 PDF 文本中提取报告期（已废弃，请使用 extract_report_info_from_text）
    
        :param text_content: PDF 文本内容
        :return: 提取的报告期，只返回 FY、Q1、HY、Q3 中的一个
        """
        try:
            report_info = self.extract_report_info_from_text(text_content)
            if report_info and report_info.get('report_period'):
                return report_info['report_period']
            logger.warning("未从PDF文本中提取到报告期")
            return None
        except Exception as e:
            logger.error(f"提取报告期时出错: {e}")
            return None

    def extract_report_year_from_text(self, text_content):
        """
        从PDF文本中提取报告年份

        :param text_content: PDF文本内容
        :return: 提取的报告年份
        """
        try:
            report_info = self.extract_report_info_from_text(text_content)
            if report_info and report_info.get('report_year'):
                return report_info['report_year']

            clean_text = self._prepare_matching_text(text_content)
            report_year = self._search_pattern_configs(
                clean_text,
                tuple((pattern, lambda m: int(m.group(1))) for pattern in self.REPORT_YEAR_TITLE_PATTERNS),
                validator=self._is_valid_report_year,
            )
            if report_year is not None:
                logger.info(f"从PDF标题中提取到报告年份: {report_year}")
                return report_year

            text_start = self._prepare_matching_text(text_content, limit=10000)
            report_year = self._search_pattern_configs(
                text_start,
                tuple((pattern, lambda m: int(m.group(1))) for pattern in self.REPORT_YEAR_START_PATTERNS),
                validator=self._is_valid_report_year,
            )
            if report_year is not None:
                logger.info(f"从PDF文本开头提取到报告年份: {report_year}")
                return report_year

            most_common_year = self._most_common_valid_year(text_content)
            if most_common_year is not None:
                logger.info(f"从PDF文本中提取到报告年份: {most_common_year}")
                return most_common_year

            # 如果仍然没有找到，返回None，让调用方从文件名中提取
            logger.warning("未从PDF文本中提取到报告年份")
            return None
        except Exception as e:
            logger.error(f"提取报告年份时出错: {e}")
            return None

    def clean_number(self, number_str):
        """
        清理数字字符串，移除千分位逗号等

        :param number_str: 数字字符串
        :return: 清理后的数字
        """
        try:
            if number_str is None:
                return None
            if isinstance(number_str, (int, float)):
                return float(number_str)
            return self._clean_number_cached(str(number_str))
        except (ValueError, AttributeError, TypeError):
            return None

    def calculate_growth(self, text_content, indicator, growth_type):
        """
        计算增长率

        :param text_content: 文本内容
        :param indicator: 指标名称或指标名称列表
        :param growth_type: 增长率类型（同比/环比）
        :return: 增长率
        """
        try:
            indicator_key = indicator if isinstance(indicator, str) else tuple(indicator)
            self._ensure_text_runtime_cache(text_content)
            cache_key = ("calculate_growth", indicator_key, growth_type)
            if cache_key in self._data_extract_cache:
                return self._data_extract_cache[cache_key]

            # 支持多个指标名称变体
            indicators = [indicator] if isinstance(indicator, str) else indicator

            for ind in indicators:
                if growth_type == '同比':
                    patterns = [
                        # 标准格式
                        rf'{ind}.*?同比增长.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}.*?同比.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'同比增长.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 表格中的增长率格式
                        rf'{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 更多格式
                        rf'{ind}.*?同比增长率.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'{ind}.*?同比增长.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'同比.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'{ind}.*?增长.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 表格中的数值格式
                        rf'{ind}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        # 新增：匹配更多同比增长率格式
                        rf'{ind}.*?较上年同期.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'较上年同期.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'{ind}.*?同比变动.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'同比变动.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        # 带空格的格式
                        rf'{ind}\s+同比\s+([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}\s+同比增长\s+([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 新增：匹配更多可能的表述
                        rf'{ind}.*?同比增长率.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'同比增长率.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}.*?较同期.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'较同期.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}.*?同比.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'同比.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 匹配表格中的数据行
                        rf'{ind}[\s\S]{{0,100}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%[\s\S]{{0,50}}?同比',
                        rf'{ind}[\s\S]{{0,100}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,50}}?同比',
                    ]
                else:  # 环比
                    patterns = [
                        # 标准格式
                        rf'{ind}.*?环比增长.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}.*?环比.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'环比增长.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 表格中的增长率格式
                        rf'{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 更多格式
                        rf'{ind}.*?环比增长率.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'{ind}.*?环比增长.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'环比.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'{ind}.*?增长.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 表格中的数值格式
                        rf'{ind}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        # 新增：匹配更多环比增长率格式
                        rf'{ind}.*?较上季度.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'较上季度.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'{ind}.*?环比变动.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        rf'环比变动.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                        # 带空格的格式
                        rf'{ind}\s+环比\s+([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}\s+环比增长\s+([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 新增：匹配更多可能的表述
                        rf'{ind}.*?环比增长率.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'环比增长率.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}.*?较上一期.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'较上一期.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'{ind}.*?环比.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        rf'环比.*?{ind}.*?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%',
                        # 匹配表格中的数据行
                        rf'{ind}[\s\S]{{0,100}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)%[\s\S]{{0,50}}?环比',
                        rf'{ind}[\s\S]{{0,100}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,50}}?环比',
                    ]

                for pattern in patterns:
                    matches = self._findall_cached(pattern, text_content)
                    if matches:
                        for match in matches:
                            growth = self.clean_number(match)
                            if growth is not None:
                                # 检查是否需要转换为小数（如果已经是小数形式）
                                if growth > 100 or growth < -100:
                                    result = growth / 100
                                    self._data_extract_cache[cache_key] = result
                                    return result
                                self._data_extract_cache[cache_key] = growth
                                return growth
            self._data_extract_cache[cache_key] = None
            return None
        except Exception as e:
            logger.error(f"计算{growth_type}增长率时出错: {e}")
            return None

    def extract_table_data(self, text_content, indicator, report_period=None):
        """
        从表格中提取数据
        
        :param text_content: 文本内容
        :param indicator: 指标名称（如'资产总计'、'负债合计'）
        :param report_period: 报告期（如'FY'、'Q1'、'HY'、'Q3'）
        :return: 提取的数据值
        """
        try:
            self._ensure_text_runtime_cache(text_content)
            cache_key = ("extract_table_data", indicator, report_period)
            if cache_key in self._data_extract_cache:
                return self._data_extract_cache[cache_key]

            indicator_whitespace_pattern = indicator.replace(" ", r"\s+")
            # 表格数据提取模式
            table_patterns = [
                # 标准表格格式：指标名称 数值
                rf'{indicator}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 表格行格式：指标名称 数值 数值（多列）
                rf'{indicator}\s*[\s\S]{{0,100}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,50}}?(?:\d{{4}}|万元|元)',
                # 带单位的表格格式
                rf'{indicator}\s*\([^)]*\)\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 多行表格格式
                rf'{indicator}\s*\n?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带空格的表格格式
                rf'{indicator}\s+([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s+',
                # 报告期相关的表格格式
                rf'{indicator}[\s\S]{{0,100}}?{report_period}[\s\S]{{0,100}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)' if report_period else None,
                # 数值在指标名称前面的格式
                rf'([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,100}}?{indicator}',
                # 紧凑表格格式
                rf'{indicator}\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带千分位的表格格式
                rf'{indicator}[\s\S]{{0,50}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,50}}?\d{{4}}',
                # 带单位的表格格式
                rf'{indicator}[\s\S]{{0,50}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,50}}?(万元|元)',
                # 新增模式：匹配带空格的指标名称
                rf'{indicator_whitespace_pattern}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 新增模式：匹配带括号的指标名称
                rf'\({indicator}\)\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 新增模式：匹配表格中的数据行
                rf'{indicator}[\s\S]{{0,50}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)[\s\S]{{0,50}}?(?=\n|$)',
                # 新增模式：匹配带单位的数值
                rf'{indicator}[\s\S]{{0,50}}?([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*(万元|元)[\s\S]{{0,50}}?(?=\n|$)',
            ]

            # 过滤掉None值
            table_patterns = [p for p in table_patterns if p]

            potential_values = []
            for pattern in table_patterns:
                matches = self._findall_cached(pattern, text_content)
                for match in matches:
                    # 处理元组匹配（当模式包含多个捕获组时）
                    if isinstance(match, tuple):
                        # 找到第一个数值捕获组
                        for group in match:
                            if re.match(r'[+-]?\d', str(group)):
                                value = self.clean_number(group)
                                break
                    else:
                        value = self.clean_number(match)
                    
                    if value is not None:
                        # 检查单位，如果是元单位，转换为万元
                        if '元' in pattern and '万' not in pattern:
                            value = value / 10000
                        # 检查数值大小，如果很大，可能是元单位
                        elif value > 1000000:
                            value = value / 10000
                        potential_values.append(value)

            if potential_values:
                # 选择最合理的值（中位数）
                result = sorted(potential_values)[len(potential_values) // 2]
                self._data_extract_cache[cache_key] = result
                return result
            self._data_extract_cache[cache_key] = None
            return None
        except Exception as e:
            logger.error(f"从表格中提取数据时出错: {e}")
            return None

    def extract_text_data(self, text_content, indicator, report_period=None):
        """
        从文本中提取数据
        
        :param text_content: 文本内容
        :param indicator: 指标名称（如'资产总计'、'负债合计'）
        :param report_period: 报告期（如'FY'、'Q1'、'HY'、'Q3'）
        :return: 提取的数据值
        """
        try:
            self._ensure_text_runtime_cache(text_content)
            cache_key = ("extract_text_data", indicator, report_period)
            if cache_key in self._data_extract_cache:
                return self._data_extract_cache[cache_key]

            indicator_whitespace_pattern = indicator.replace(" ", r"\s+")
            # 文本数据提取模式
            text_patterns = [
                # 标准文本格式：指标名称 数值
                rf'{indicator}\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带单位的文本格式
                rf'{indicator}\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*(万元|元)',
                # 带括号单位的文本格式
                rf'{indicator}\s*\([^)]*\)\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 紧凑文本格式
                rf'{indicator}\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 报告期相关的文本格式
                rf'{indicator}[\s\S]{{0,100}}?{report_period}[\s\S]{{0,100}}?[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)' if report_period else None,
                # 数值在指标名称前面的格式
                rf'([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*{indicator}',
                # 带描述的文本格式
                rf'{indicator}\s*[是为为]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带比较的文本格式
                rf'{indicator}\s*[同比环比]?\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带单位的比较格式
                rf'{indicator}\s*[同比环比]?\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*(万元|元)',
                # 带空格的格式
                rf'{indicator}\s+[:：]?\s+([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带冒号的紧凑格式
                rf'{indicator}[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 带小数点的格式
                rf'{indicator}\s*[:：]\s*([+-]?\d+\.\d+)',
                # 带整数的格式
                rf'{indicator}\s*[:：]\s*([+-]?\d+)',
                # 新增模式：匹配带空格的指标名称
                rf'{indicator_whitespace_pattern}\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 新增模式：匹配带括号的指标名称
                rf'\({indicator}\)\s*[:：]?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 新增模式：匹配带描述的格式
                rf'{indicator}.*?[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)',
                # 新增模式：匹配带单位的描述格式
                rf'{indicator}.*?[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*(万元|元)',
                # 新增模式：匹配带百分比的格式
                rf'{indicator}.*?[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?%)',
                # 新增模式：匹配紧凑的数值格式
                rf'{indicator}\s*[:：]\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*',
            ]

            # 过滤掉None值
            text_patterns = [p for p in text_patterns if p]

            potential_values = []
            for pattern in text_patterns:
                matches = self._findall_cached(pattern, text_content)
                for match in matches:
                    # 处理元组匹配（当模式包含多个捕获组时）
                    if isinstance(match, tuple):
                        # 找到第一个数值捕获组
                        for group in match:
                            if re.match(r'[+-]?\d', str(group)):
                                value = self.clean_number(group)
                                break
                    else:
                        value = self.clean_number(match)
                    
                    if value is not None:
                        # 检查单位，如果是元单位，转换为万元
                        if '元' in pattern and '万' not in pattern:
                            value = value / 10000
                        # 检查数值大小，如果很大，可能是元单位
                        elif value > 1000000:
                            value = value / 10000
                        potential_values.append(value)

            if potential_values:
                # 选择最合理的值（中位数）
                result = sorted(potential_values)[len(potential_values) // 2]
                self._data_extract_cache[cache_key] = result
                return result
            self._data_extract_cache[cache_key] = None
            return None
        except Exception as e:
            logger.error(f"从文本中提取数据时出错: {e}")
            return None

    def calculate_balance_sheet_growth(self, text_content, indicator):
        """
        从比较资产负债表中计算增长率
        
        比较资产负债表通常包含两列数据：期末余额和期初余额
        通过比较这两期的数据来计算增长率
        
        :param text_content: 文本内容
        :param indicator: 指标名称（如'资产总计'、'负债合计'）
        :return: 增长率（小数形式）
        """
        try:
            self._ensure_text_runtime_cache(text_content)
            cache_key = ("calculate_balance_sheet_growth", indicator)
            if cache_key in self._data_extract_cache:
                return self._data_extract_cache[cache_key]

            # 匹配比较资产负债表中的数据格式
            # 格式通常是：指标名称\n期末余额数值\n期初余额数值
            pattern = rf'{indicator}\s*\n?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)\s*\n?\s*([+-]?\d{{1,3}}(?:,\d{{3}})*(?:\.\d+)?)'
            matches = self._findall_cached(pattern, text_content)
            
            if matches:
                for match in matches:
                    current_value = self.clean_number(match[0])  # 期末余额（本期）
                    previous_value = self.clean_number(match[1])  # 期初余额（上期）
                    
                    if current_value is not None and previous_value is not None and previous_value != 0:
                        # 将元转换为万元（如果数值很大）
                        if current_value > 1000000:
                            current_value = current_value / 10000
                        if previous_value > 1000000:
                            previous_value = previous_value / 10000
                        
                        # 计算增长率
                        growth_rate = (current_value - previous_value) / abs(previous_value)
                        logger.debug(f"从比较资产负债表计算{indicator}增长率: 本期={current_value}, 上期={previous_value}, 增长率={growth_rate}")
                        self._data_extract_cache[cache_key] = growth_rate
                        return growth_rate
            
            # 如果没有找到匹配，返回None
            self._data_extract_cache[cache_key] = None
            return None
        except Exception as e:
            logger.error(f"计算资产负债表增长率时出错: {e}")
            return None

    def extract_core_performance(self, text_content, existing_data=None):
        """从财报文本中提取核心业绩指标
        
        :param text_content: 财报文本内容
        :param existing_data: 已提取的其他表数据（如现金流量表），用于计算依赖字段
        """
        core_data = {}
        if existing_data is None:
            existing_data = {}

        # 从文本中提取净利润
        net_profit_patterns = [
            # 匹配表格中的净利润（元单位）
            r'归属于上市公司股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?202[0-9]年',
            r'归属于上市公司股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            # 匹配表格中的净利润（万元单位）
            r'归属于上市公司股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            # 优先匹配更具体的模式，包含更多上下文
            r'归属于上市公司股东的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带元单位的模式
            r'归属于上市公司股东的净利润[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者的净利润[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带上下文的模式
            r'归属于上市公司股东的净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'归母净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'归属于上市公司股东的净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?元',
            r'净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?元',
            r'归母净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?元',
            # 新增模式：匹配表格中的数值
            r'净利润[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?[20]\d{3}',
            r'归属于母公司股东的净利润[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更广泛的净利润格式
            r'归属于.*?股东的净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的净利润
            r'归属于上市公司股东的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的净利润数据
            r'归属于.*?股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 新增模式：匹配带括号的净利润
            r'归属于上市公司股东的净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配带空格的格式
            r'归属于\s+上市公司\s+股东的\s+净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于\s+母公司\s+所有者的\s+净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更简洁的格式
            r'净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配带单位的简洁格式
            r'净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
            r'净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            r'归母净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
            r'归母净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元'
        ]
        net_profit_found = False
        potential_net_profits = []
        for pattern in net_profit_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                net_profit = self.clean_number(match)
                if net_profit is not None:
                    # 检查文本中是否有元的单位，如果有则转换为万元
                    if '元' in pattern and '万' not in pattern:
                        net_profit = net_profit / 10000
                    # 检查是否需要从元转换为万元（通过数值大小判断）
                    elif net_profit > 1000000:  # 如果数值很大，可能是元单位
                        net_profit = net_profit / 10000
                    # 只考虑合理的数值
                    if net_profit > -1000000 and net_profit < 10000000:  # 限制范围，避免提取到错误的大数值
                        potential_net_profits.append(net_profit)

        # 选择最合理的值作为净利润
        if potential_net_profits:
            # 选择最合理的值（中位数）
            core_data['net_profit_10k_yuan'] = sorted(potential_net_profits)[len(potential_net_profits) // 2]
            # 验证净利润值是否合理
            if core_data['net_profit_10k_yuan'] < -1000000 or core_data['net_profit_10k_yuan'] > 10000000:
                logger.warning(f"提取到的净利润值可能不合理: {core_data['net_profit_10k_yuan']}")
            else:
                # 首先尝试使用多个指标名称变体计算同比增长率
                core_data['net_profit_yoy_growth'] = self.calculate_growth(
                    text_content, 
                    ['净利润', '归属于上市公司股东的净利润', '归母净利润', '归属于母公司所有者的净利润'], 
                    '同比'
                )
                
                # 如果没有找到同比增长率，尝试从比较财务报表中计算
                if core_data['net_profit_yoy_growth'] is None:
                    core_data['net_profit_yoy_growth'] = self.calculate_balance_sheet_growth(
                        text_content, 
                        '归属于上市公司股东的净利润'
                    )
                    if core_data['net_profit_yoy_growth'] is not None:
                        logger.info(f"从比较财务报表计算得到净利润同比增长率: {core_data['net_profit_yoy_growth']}")
                
                logger.debug(f"提取到净利润: {core_data['net_profit_10k_yuan']}")
                net_profit_found = True

        # 如果没有提取到净利润，尝试使用表格和文本提取函数
        if not net_profit_found:
            # 尝试从表格中提取
            table_net_profit = self.extract_table_data(text_content, '归属于上市公司股东的净利润')
            if table_net_profit is not None:
                core_data['net_profit_10k_yuan'] = table_net_profit
                net_profit_found = True
                logger.info(f"从表格中提取到净利润: {core_data['net_profit_10k_yuan']}")
            else:
                # 尝试从文本中提取
                text_net_profit = self.extract_text_data(text_content, '归属于上市公司股东的净利润')
                if text_net_profit is not None:
                    core_data['net_profit_10k_yuan'] = text_net_profit
                    net_profit_found = True
                    logger.info(f"从文本中提取到净利润: {core_data['net_profit_10k_yuan']}")
                else:
                    # 尝试其他变体
                    variants = ['净利润', '归母净利润', '归属于母公司所有者的净利润']
                    for variant in variants:
                        table_value = self.extract_table_data(text_content, variant)
                        if table_value is not None:
                            core_data['net_profit_10k_yuan'] = table_value
                            net_profit_found = True
                            logger.info(f"从表格中提取到{variant}: {core_data['net_profit_10k_yuan']}")
                            break
                        text_value = self.extract_text_data(text_content, variant)
                        if text_value is not None:
                            core_data['net_profit_10k_yuan'] = text_value
                            net_profit_found = True
                            logger.info(f"从文本中提取到{variant}: {core_data['net_profit_10k_yuan']}")
                            break

        if not net_profit_found:
            logger.warning("未提取到合理的净利润数据")

        # 从文本中提取营业总收入
        revenue_patterns = [
            # 匹配表格中的营业收入（元单位）
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?202[0-9]年',
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            # 匹配表格中的营业收入（万元单位）
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            # 优先匹配更具体的模式，包含更多上下文
            r'营业总收入[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总营收[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带元单位的模式
            r'营业总收入[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带上下文的模式
            r'营业收入[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'营业总收入[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'营业收入[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?元',
            r'营业总收入[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?元',
            # 新增模式：匹配表格中的数值
            r'营业收入[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?[20]\d{3}',
            r'营业总收入[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的营业总收入
            r'营业总收入\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总营收\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的营业收入数据
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'营业总收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 新增模式：匹配带括号的营业总收入
            r'营业总收入\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总营收\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        revenue_found = False
        potential_revenues = []
        for pattern in revenue_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                total_revenue = self.clean_number(match)
                if total_revenue is not None:
                    # 检查文本中是否有元的单位，如果有则转换为万元
                    if '元' in pattern and '万' not in pattern:
                        total_revenue = total_revenue / 10000
                    # 检查是否需要从元转换为万元（通过数值大小判断）
                    elif total_revenue > 1000000:  # 如果数值很大，可能是元单位
                        total_revenue = total_revenue / 10000
                    # 只考虑合理的数值
                    if total_revenue >= 1:  # 放宽限制，允许较小的收入值
                        potential_revenues.append(total_revenue)

        # 选择最大的合理值作为收入
        if potential_revenues:
            valid_revenue = max(potential_revenues)
            # 验证营业总收入值是否合理
            if valid_revenue < 1 or valid_revenue > 100000000:
                logger.warning(f"提取到的营业总收入值可能不合理: {valid_revenue}")
            else:
                core_data['total_operating_revenue'] = valid_revenue
                core_data['operating_revenue_yoy_growth'] = self.calculate_growth(text_content, '营业收入', '同比')
                logger.debug(f"提取到营业总收入: {valid_revenue}")
                revenue_found = True

        # 如果没有提取到营业总收入，尝试使用表格和文本提取函数
        if not revenue_found:
            # 尝试从表格中提取
            table_revenue = self.extract_table_data(text_content, '营业总收入')
            if table_revenue is not None:
                core_data['total_operating_revenue'] = table_revenue
                revenue_found = True
                logger.info(f"从表格中提取到营业总收入: {core_data['total_operating_revenue']}")
            else:
                # 尝试从文本中提取
                text_revenue = self.extract_text_data(text_content, '营业总收入')
                if text_revenue is not None:
                    core_data['total_operating_revenue'] = text_revenue
                    revenue_found = True
                    logger.info(f"从文本中提取到营业总收入: {core_data['total_operating_revenue']}")
                else:
                    # 尝试其他变体
                    variants = ['营业收入', '总营收']
                    for variant in variants:
                        table_value = self.extract_table_data(text_content, variant)
                        if table_value is not None:
                            core_data['total_operating_revenue'] = table_value
                            revenue_found = True
                            logger.info(f"从表格中提取到{variant}: {core_data['total_operating_revenue']}")
                            break
                        text_value = self.extract_text_data(text_content, variant)
                        if text_value is not None:
                            core_data['total_operating_revenue'] = text_value
                            revenue_found = True
                            logger.info(f"从文本中提取到{variant}: {core_data['total_operating_revenue']}")
                            break

        if not revenue_found:
            logger.warning("未提取到合理的营业总收入数据")

        # 从文本中提取每股收益
        eps_patterns = [
            r'基本每股收益[\s:：]*[（\(]?[元]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股收益[\s:：]*[（\(]?[元]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'基本每股收益[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股收益[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'基本每股收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            # 新增模式：匹配不同格式的每股收益
            r'基本每股收益\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股收益\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'基本每股收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            r'每股收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            # 新增模式：匹配表格中的每股收益数据
            r'基本每股收益[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'每股收益[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}'
        ]
        eps_found = False
        potential_eps = []
        for pattern in eps_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                eps = self.clean_number(match)
                if eps is not None and -10 <= eps <= 10:  # 合理范围
                    potential_eps.append(eps)

        if potential_eps:
            # 选择最合理的值（中位数）
            core_data['eps'] = sorted(potential_eps)[len(potential_eps) // 2]
            # 验证每股收益值是否合理
            if core_data['eps'] < -10 or core_data['eps'] > 100:
                logger.warning(f"提取到的每股收益值可能不合理: {core_data['eps']}")
            else:
                logger.debug(f"提取到每股收益: {core_data['eps']}")
                eps_found = True

        # 如果没有提取到每股收益，尝试使用表格和文本提取函数
        if not eps_found:
            # 尝试从表格中提取
            table_eps = self.extract_table_data(text_content, '基本每股收益')
            if table_eps is not None:
                core_data['eps'] = table_eps
                eps_found = True
                logger.info(f"从表格中提取到基本每股收益: {core_data['eps']}")
            else:
                # 尝试从文本中提取
                text_eps = self.extract_text_data(text_content, '基本每股收益')
                if text_eps is not None:
                    core_data['eps'] = text_eps
                    eps_found = True
                    logger.info(f"从文本中提取到基本每股收益: {core_data['eps']}")
                else:
                    # 尝试其他变体
                    variants = ['每股收益', 'EPS']
                    for variant in variants:
                        table_value = self.extract_table_data(text_content, variant)
                        if table_value is not None:
                            core_data['eps'] = table_value
                            eps_found = True
                            logger.info(f"从表格中提取到{variant}: {core_data['eps']}")
                            break
                        text_value = self.extract_text_data(text_content, variant)
                        if text_value is not None:
                            core_data['eps'] = text_value
                            eps_found = True
                            logger.info(f"从文本中提取到{variant}: {core_data['eps']}")
                            break

        if not eps_found:
            logger.warning("未提取到每股收益数据")

        # 从文本中提取净资产收益率
        roe_patterns = [
            # 优先匹配年报摘要中的格式（处理换行）
            r'加权平均净资[\s\S]{0,10}?产收益率[\s\S]{0,50}?（%）[\s\S]{0,200}?([+-]?\d+\.?\d*)',
            r'加权平均净资产收益率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'ROE[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            r'加权平均净资产收益率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            # 新增模式：匹配更多格式
            r'加权平均净资产收益率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*%)',
            r'净资产收益率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*%)',
            r'加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'ROE\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*%)',
            r'加权平均ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均ROE\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*%)',
            r'ROE\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\s*[:：]\s*([+-]?\d+\.?\d*)',
            r'净资产收益率\s*[:：]\s*([+-]?\d+\.?\d*)',
            r'ROE\s*[:：]\s*([+-]?\d+\.?\d*)',
            r'加权平均净资产收益率\s*[:：]\s*([+-]?\d+\.?\d*)\s*%',
            r'净资产收益率\s*[:：]\s*([+-]?\d+\.?\d*)\s*%',
            r'ROE\s*[:：]\s*([+-]?\d+\.?\d*)\s*%',
            # 新增模式：匹配更多格式的净资产收益率
            r'加权平均净资产收益率\s*[（\(]%[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率\s*[（\(]%[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\s*[（\(]\%[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率\s*[（\(]\%[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'ROE\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d+\.?\d*)',
            r'净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d+\.?\d*)',
            r'ROE\s*[：:][\s\S]{0,50}?([+-]?\d+\.?\d*)',
            r'加权平均净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'ROE\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'加权平均净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d+\.?\d*)\s*%',
            r'净资产收益率\s*[：:][\s\S]{0,50}?([+-]?\d+\.?\d*)\s*%',
            r'ROE\s*[：:][\s\S]{0,50}?([+-]?\d+\.?\d*)\s*%'
        ]
        potential_roe = []
        for pattern in roe_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                roe = self.clean_number(match)
                if roe is not None and -100 <= roe <= 100:  # 合理范围
                    potential_roe.append(roe)

        # 如果没有提取到净资产收益率，尝试使用表格和文本提取函数
        if not potential_roe:
            # 尝试从表格中提取
            table_roe = self.extract_table_data(text_content, '加权平均净资产收益率')
            if table_roe is not None:
                potential_roe.append(table_roe)
                logger.info(f"从表格中提取到加权平均净资产收益率: {table_roe}")
            else:
                # 尝试从文本中提取
                text_roe = self.extract_text_data(text_content, '加权平均净资产收益率')
                if text_roe is not None:
                    potential_roe.append(text_roe)
                    logger.info(f"从文本中提取到加权平均净资产收益率: {text_roe}")
                else:
                    # 尝试其他变体
                    variants = ['净资产收益率', 'ROE']
                    for variant in variants:
                        table_value = self.extract_table_data(text_content, variant)
                        if table_value is not None:
                            potential_roe.append(table_value)
                            logger.info(f"从表格中提取到{variant}: {table_value}")
                            break
                        text_value = self.extract_text_data(text_content, variant)
                        if text_value is not None:
                            potential_roe.append(text_value)
                            logger.info(f"从文本中提取到{variant}: {text_value}")
                            break

        if potential_roe:
            # 选择最合理的值（中位数）
            core_data['roe'] = sorted(potential_roe)[len(potential_roe) // 2]
            # 验证净资产收益率值是否合理
            if core_data['roe'] < -100 or core_data['roe'] > 100:
                logger.warning(f"提取到的净资产收益率值可能不合理: {core_data['roe']}")
            else:
                logger.debug(f"提取到净资产收益率: {core_data['roe']}")

        # 从文本中提取销售毛利率
        gross_profit_patterns = [
            # 匹配表格中的毛利率
            r'销售毛利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'毛利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'销售毛利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'毛利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 优先匹配更具体的模式，包含更多上下文
            r'销售毛利率[\s:：]*[（\(]?%[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率[\s:：]*[（\(]?%[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的模式
            r'销售毛利率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售毛利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'毛利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            # 匹配简洁模式
            r'销售毛利率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售毛利率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售毛利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            r'毛利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            # 新增模式：匹配不同格式的毛利率
            r'销售毛利率[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售毛利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增：匹配更多可能的表述
            r'销售毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售毛利率\s*\(\%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率\s*\(\%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配年报中的格式
            r'销售毛利率[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'毛利率[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            # 匹配简化格式
            r'销售毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'销售毛利率[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%',
            r'毛利率[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%',
            # 新增：匹配更多可能的表述
            r'销售毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'销售毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*百分比',
            r'毛利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*百分比'
        ]
        gross_profit_found = False
        potential_gross_profit = []
        for pattern in gross_profit_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"销售毛利率模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                gross_profit_margin = self.clean_number(match)
                # 验证毛利率是否合理（0-100%）
                if gross_profit_margin is not None and 0 <= gross_profit_margin <= 100:
                    potential_gross_profit.append(gross_profit_margin)

        if potential_gross_profit:
            # 选择最合理的值（中位数）
            core_data['gross_profit_margin'] = sorted(potential_gross_profit)[len(potential_gross_profit) // 2]
            # 验证销售毛利率值是否合理
            if core_data['gross_profit_margin'] < 0 or core_data['gross_profit_margin'] > 100:
                logger.warning(f"提取到的销售毛利率值可能不合理: {core_data['gross_profit_margin']}")
            else:
                logger.debug(f"提取到销售毛利率: {core_data['gross_profit_margin']}")
                gross_profit_found = True
        else:
            # 尝试从文本中提取营业成本
            cost_of_sales = None
            cost_patterns = [
                r'营业成本[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'营业成本[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'营业成本[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'营业成本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
                r'营业成本\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'营业成本\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
                r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}'
            ]
            for pattern in cost_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    for match in matches:
                        cost_value = self.clean_number(match)
                        if cost_value is not None:
                            # 检查单位，如果是元，转换为万元
                            if '元' in pattern and '万' not in pattern:
                                cost_value = cost_value / 10000
                            # 检查是否需要从元转换为万元（通过数值大小判断）
                            elif cost_value > 1000000:
                                cost_value = cost_value / 10000
                            cost_of_sales = cost_value
                            logger.debug(f"提取到营业成本: {cost_of_sales}")
                            break
                if cost_of_sales is not None:
                    break
            
            # 如果提取到营业成本，尝试通过营业收入和营业成本计算毛利率
            if 'total_operating_revenue' in core_data and cost_of_sales is not None:
                total_revenue = core_data['total_operating_revenue']
                if total_revenue > 0:
                    # 确保单位一致
                    if total_revenue > 1000 and cost_of_sales > 1000000:
                        # 如果营业收入是万元单位，营业成本是元单位，转换为万元
                        cost_of_sales = cost_of_sales / 10000
                    gross_profit = total_revenue - cost_of_sales
                    gross_profit_margin = (gross_profit / total_revenue) * 100
                    if 0 <= gross_profit_margin <= 100:
                        core_data['gross_profit_margin'] = gross_profit_margin
                        logger.info(f"通过营业收入和营业成本计算得到销售毛利率: {gross_profit_margin}")
                        gross_profit_found = True

        if not gross_profit_found:
            # 尝试使用existing_data中的数据计算毛利率
            total_revenue = existing_data.get('total_operating_revenue')
            cost_of_sales = existing_data.get('operating_expense_cost_of_sales')
            
            if total_revenue is not None and cost_of_sales is not None and total_revenue > 0:
                # 确保单位一致
                if total_revenue > 1000 and cost_of_sales > 1000000:
                    # 如果营业收入是万元单位，营业成本是元单位，转换为万元
                    cost_of_sales = cost_of_sales / 10000
                gross_profit = total_revenue - cost_of_sales
                gross_profit_margin = (gross_profit / total_revenue) * 100
                if 0 <= gross_profit_margin <= 100:
                    core_data['gross_profit_margin'] = gross_profit_margin
                    logger.info(f"通过existing_data中的营业收入({total_revenue})和营业成本({cost_of_sales})计算得到销售毛利率: {gross_profit_margin}")
                    gross_profit_found = True
            
            if not gross_profit_found:
                logger.warning("未提取到合理的销售毛利率数据")

        # 从文本中提取销售净利率
        net_profit_margin_patterns = [
            # 匹配表格中的净利率
            r'销售净利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'净利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'销售净利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'净利率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 优先匹配更具体的模式，包含更多上下文
            r'销售净利率[\s:：]*[（\(]?%[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率[\s:：]*[（\(]?%[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的模式
            r'销售净利率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售净利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            r'净利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            # 匹配简洁模式
            r'销售净利率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售净利率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的净利率
            r'销售净利率[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售净利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增：匹配更多可能的表述
            r'销售净利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售净利率\s*\(\%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率\s*\(\%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配年报中的格式
            r'销售净利率[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'净利率[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            # 匹配简化格式
            r'销售净利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'销售净利率[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%',
            r'净利率[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%'
        ]
        net_profit_margin_found = False
        potential_net_profit_margin = []
        for pattern in net_profit_margin_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"销售净利率模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                net_profit_margin = self.clean_number(match)
                if net_profit_margin is not None and -100 <= net_profit_margin <= 100:  # 合理范围
                    potential_net_profit_margin.append(net_profit_margin)

        # 如果没有提取到销售净利率，尝试使用表格和文本提取函数
        if not potential_net_profit_margin:
            # 尝试从表格中提取
            table_margin = self.extract_table_data(text_content, '销售净利率')
            if table_margin is not None:
                potential_net_profit_margin.append(table_margin)
                logger.info(f"从表格中提取到销售净利率: {table_margin}")
            else:
                # 尝试从文本中提取
                text_margin = self.extract_text_data(text_content, '销售净利率')
                if text_margin is not None:
                    potential_net_profit_margin.append(text_margin)
                    logger.info(f"从文本中提取到销售净利率: {text_margin}")
                else:
                    # 尝试其他变体
                    variants = ['净利率']
                    for variant in variants:
                        table_value = self.extract_table_data(text_content, variant)
                        if table_value is not None:
                            potential_net_profit_margin.append(table_value)
                            logger.info(f"从表格中提取到{variant}: {table_value}")
                            break
                        text_value = self.extract_text_data(text_content, variant)
                        if text_value is not None:
                            potential_net_profit_margin.append(text_value)
                            logger.info(f"从文本中提取到{variant}: {text_value}")
                            break

        if potential_net_profit_margin:
            # 选择最合理的值（中位数）
            core_data['net_profit_margin'] = sorted(potential_net_profit_margin)[len(potential_net_profit_margin) // 2]
            logger.debug(f"提取到销售净利率: {core_data['net_profit_margin']}")
            net_profit_margin_found = True
        else:
            # 如果没有提取到净利率，尝试通过净利润和营业收入计算
            if 'net_profit_10k_yuan' in core_data and 'total_operating_revenue' in core_data:
                net_profit = core_data['net_profit_10k_yuan']
                total_revenue = core_data['total_operating_revenue']
                if total_revenue > 0:
                    net_profit_margin = (net_profit / total_revenue) * 100
                    if -100 <= net_profit_margin <= 100:
                        core_data['net_profit_margin'] = net_profit_margin
                        logger.info(f"通过净利润和营业收入计算得到销售净利率: {net_profit_margin}")
                        net_profit_margin_found = True

        if not net_profit_margin_found:
            # 尝试使用existing_data中的数据计算净利率
            net_profit = existing_data.get('net_profit_10k_yuan')
            total_revenue = existing_data.get('total_operating_revenue')
            
            if net_profit is not None and total_revenue is not None and total_revenue > 0:
                net_profit_margin = (net_profit / total_revenue) * 100
                if -100 <= net_profit_margin <= 100:
                    core_data['net_profit_margin'] = net_profit_margin
                    logger.info(f"通过existing_data中的净利润({net_profit})和营业收入({total_revenue})计算得到销售净利率: {net_profit_margin}")
                    net_profit_margin_found = True
            
            if not net_profit_margin_found:
                logger.warning("未提取到合理的销售净利率数据")

        # 从文本中提取每股净资产
        net_asset_patterns = [
            # 匹配表格中的每股净资产
            r'每股净资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'每股净资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 优先匹配更具体的模式，包含更多上下文
            r'每股净资产[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产[\s:：]*[（\(]?[元]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的模式
            r'每股净资产\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            # 匹配简洁模式
            r'每股净资产[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            # 新增模式：匹配不同格式的每股净资产
            r'每股净资产[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增更多模式
            r'每股净资产\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[：:][\s\S]*?([+-]?\d+\.?\d*)',
            r'每股净资产.*?([+-]?\d+\.?\d*)',
            r'每股净资产\s*[:：]?\s*([+-]?\d+\.?\d*)',
            r'每股净资产\s*\((?:元|万元)\)\s*[:：]?\s*([+-]?\d+\.?\d*)',
            # 匹配表格中的数值
            r'每股净资产[\s\S]{0,100}?([+-]?\d+\.?\d*)[\s\S]{0,50}?\d{4}',
            r'每股净资产[\s\S]{0,100}?([+-]?\d+\.?\d*)[\s\S]{0,50}?元',
            # 新增更多模式
            r'每股净资产\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            r'每股净资产\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*\([^)]*\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*\(元\)',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*\(万元\)',
            # 匹配表格行格式
            r'每股净资产\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'每股净资产\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s+元',
            # 新增：匹配更多可能的表述
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元/股',
            r'每股净资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元每股',
            r'每股净资产\s*\(元/股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*\(元每股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配年报中的格式
            r'每股净资产[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'每股净资产[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配简化格式
            r'每股净资产\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股净资产\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行
            r'每股净资产[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'每股净资产[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?元'
        ]
        potential_net_asset = []
        for pattern in net_asset_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"每股净资产模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                net_asset_per_share = self.clean_number(match)
                if net_asset_per_share is not None and -100 <= net_asset_per_share <= 1000:  # 合理范围
                    potential_net_asset.append(net_asset_per_share)

        # 如果没有提取到每股净资产，尝试使用表格和文本提取函数
        if not potential_net_asset:
            # 尝试从表格中提取
            table_net_asset = self.extract_table_data(text_content, '每股净资产')
            if table_net_asset is not None:
                potential_net_asset.append(table_net_asset)
                logger.info(f"从表格中提取到每股净资产: {table_net_asset}")
            else:
                # 尝试从文本中提取
                text_net_asset = self.extract_text_data(text_content, '每股净资产')
                if text_net_asset is not None:
                    potential_net_asset.append(text_net_asset)
                    logger.info(f"从文本中提取到每股净资产: {text_net_asset}")

        if potential_net_asset:
            # 选择最合理的值（中位数）
            core_data['net_asset_per_share'] = sorted(potential_net_asset)[len(potential_net_asset) // 2]
            logger.debug(f"提取到每股净资产: {core_data['net_asset_per_share']}")
        else:
            # 尝试通过股东权益和总股本计算每股净资产
            logger.debug("尝试通过股东权益和总股本计算每股净资产")
            
            # 1. 提取归属于上市公司股东的净资产（从主要会计数据和财务指标部分）
            total_equity = None
            equity_patterns = [
                # 优先匹配年报摘要中的格式（处理换行）
                r'归属于上市公[\s\S]{0,50}?司股东的净资[\s\S]{0,50}?产[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于上市公司股东的净资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
                r'归属于上市公司股东的净资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
                r'归属于上市公司股东的净资产[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于上市公司股东的净资产[\s:：]*[（\(]?[元]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'股东权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'所有者权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于母公司股东权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'股东权益合计[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'所有者权益合计[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                # 新增更多模式
                r'股东权益合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'所有者权益合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于母公司股东权益合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'股东权益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'所有者权益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'股东权益合计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'所有者权益合计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                # 新增：匹配更多可能的表述
                r'归属于母公司所有者权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'股东权益\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'所有者权益\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于母公司所有者权益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于母公司股东权益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
            ]
            
            for pattern in equity_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    for match in matches:
                        equity_value = self.clean_number(match)
                        if equity_value is not None and equity_value > 0:
                            # 检查单位，如果是元，转换为万元
                            if '元' in pattern and '万' not in pattern:
                                equity_value = equity_value / 10000
                            # 检查数值大小，如果很大，可能是元单位
                            elif equity_value > 1000000:
                                equity_value = equity_value / 10000
                            total_equity = equity_value
                            logger.debug(f"提取到股东权益: {total_equity}")
                            break
                if total_equity is not None:
                    break
            
            # 2. 提取总股本
            total_share = None
            
            # 2.1 从股东情况部分计算总股本
            # 提取前10名股东持股数量并求和
            # 注意：PDF中"期末持股数量"可能被断行成"期末持股数"和"量"
            shareholding_patterns = [
                r'期末持股数[\s\S]{0,10}?量[\s\S]{0,500}?(\d{1,3}(?:,\d{3})*)',
                r'期末持股数量[\s\S]{0,500}?(\d{1,3}(?:,\d{3})*)',
                r'持股数[\s\S]{0,10}?量[\s\S]{0,500}?(\d{1,3}(?:,\d{3})*)',
                r'期末持股[\s\S]{0,50}?(\d{1,3}(?:,\d{3})*)',
            ]
            shareholdings = []
            for pattern in shareholding_patterns:
                shareholdings = re.findall(pattern, text_content)
                if shareholdings:
                    logger.debug(f"股东持股数量模式匹配成功: {pattern}，找到 {len(shareholdings)} 个匹配")
                    break
            
            if shareholdings:
                total_share_from_holders = 0
                for holding in shareholdings:
                    holding_clean = holding.replace(',', '')
                    try:
                        total_share_from_holders += int(holding_clean)
                    except ValueError:
                        pass
                if total_share_from_holders > 0:
                    # 转换为万股
                    total_share = total_share_from_holders / 10000
                    logger.debug(f"从股东持股数量计算得到总股本: {total_share} 万股")
            
            # 2.2 如果从股东情况部分没有提取到，尝试其他模式
            if total_share is None:
                total_share_patterns = [
                    r'总股本[\s:：]*[（\(]?万股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本[\s:：]*[（\(]?股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*\(万股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*\(股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 新增更多模式
                    r'股本\s*[（\(]?万股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[（\(]?股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*\(万股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*\(股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 新增：匹配更多可能的表述
                    r'总股本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司总股本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司股本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司总股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
                ]
                
                for pattern in total_share_patterns:
                    matches = re.findall(pattern, text_content)
                    if matches:
                        for match in matches:
                            share_value = self.clean_number(match)
                            if share_value is not None and 0 < share_value < 100000000:  # 合理范围检查
                                # 检查单位，如果是股，转换为万股
                                if '股' in pattern and '万' not in pattern:
                                    # 检查数值大小，如果很小，可能已经是万股单位
                                    if share_value < 1000:
                                        # 假设已经是万股单位
                                        logger.debug(f"假设总股本值已经是万股单位: {share_value} 万股")
                                    else:
                                        # 转换为万股单位
                                        share_value = share_value / 10000
                                        logger.debug(f"总股本单位转换（股→万股）: {share_value} 万股")
                                # 检查数值大小，如果很大，可能是股单位
                                elif share_value > 1000000:
                                    share_value = share_value / 10000
                                    logger.debug(f"总股本单位转换（可能是股单位）: {share_value} 万股")
                                total_share = share_value
                                logger.debug(f"提取到总股本: {total_share} 万股")
                                break
                    if total_share is not None:
                        break
            
            # 3. 如果都提取到了，计算每股净资产
            if total_equity is not None and total_share is not None and total_share > 0:
                net_asset_per_share = total_equity / total_share
                if -100 <= net_asset_per_share <= 1000:
                    core_data['net_asset_per_share'] = net_asset_per_share
                    logger.info(f"通过股东权益({total_equity})和总股本({total_share})计算得到每股净资产: {net_asset_per_share}")
            else:
                logger.warning("未提取到每股净资产数据，无法通过计算获得")

        # 从文本中提取每股经营现金流
        operating_cf_patterns = [
            # 匹配表格中的每股经营现金流 - 优先匹配更具体的模式
            r'每股经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金净流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的每股现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带括号的单位格式
            r'每股经营活动产生的现金流量净额\s*[（\(]元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s*[（\(]元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s*[（\(]元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位后缀的格式
            r'每股经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            r'每股经营现金流\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            r'每股经营活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
            # 匹配简洁格式（只有数字）
            r'每股经营活动产生的现金流量净额[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格行格式（指标名后跟数字）
            r'每股经营活动产生的现金流量净额.*?([+-]?\d+\.?\d*)',
            r'每股经营现金流.*?([+-]?\d+\.?\d*)',
            r'每股经营活动现金流量.*?([+-]?\d+\.?\d*)',
            # 匹配更广泛的模式
            r'每股经营现金流[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式
            r'每股经营活动产生的现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动每股现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营净现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动净现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*/\s*总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*/\s*总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多可能的表述
            r'每股经营活动现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动每股现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行
            r'每股经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'每股经营现金流[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 新增：匹配更多可能的表述
            r'每股经营活动产生的现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的每股现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动产生的现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配年报中的格式
            r'每股经营现金流[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'每股经营活动现金流量[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'每股经营活动产生的现金流量净额[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            # 匹配简化格式
            r'每股经营现金流\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'每股经营现金流[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'每股经营活动现金流量[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'每股经营活动产生的现金流量净额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            # 新增：匹配更多可能的表述
            r'经营活动现金流量净额\s*\/\s*总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*\/\s*总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金净流量\s*\/\s*总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金净流量\s*\/\s*总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'每股经营活动产生的现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的其他格式
            r'每股经营活动产生的现金流量净额\s*[（\(]元[）\)]\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s*[（\(]元[）\)]\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s*[（\(]元[）\)]\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配最广泛的模式
            r'每股经营.*?现金流.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动.*?每股.*?现金流.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股.*?经营活动.*?现金流.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据（更广泛）
            r'每股经营活动产生的现金流量净额[\s\S]{0,300}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,200}?元',
            r'每股经营现金流[\s\S]{0,300}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,200}?元',
            r'每股经营活动现金流量[\s\S]{0,300}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,200}?元',
            # 匹配年报摘要中的格式
            r'每股经营现金流[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}年',
            r'每股经营活动现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}年',
            r'每股经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}年',
            # 匹配带单位的简洁格式
            r'每股经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*元)',
            r'每股经营现金流\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*元)',
            r'每股经营活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*元)',
            # 匹配括号格式的变体
            r'每股经营活动产生的现金流量净额\s*\(元\)\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营现金流\s*\(元\)\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'每股经营活动现金流量\s*\(元\)\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_operating_cf = []
        for pattern in operating_cf_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"每股经营现金流模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                operating_cf_per_share = self.clean_number(match)
                if operating_cf_per_share is not None and -100 <= operating_cf_per_share <= 100:  # 合理范围
                    potential_operating_cf.append(operating_cf_per_share)

        operating_cf_found = False
        if potential_operating_cf:
            # 选择最合理的值（中位数）
            core_data['operating_cf_per_share'] = sorted(potential_operating_cf)[len(potential_operating_cf) // 2]
            logger.info(f"提取到每股经营现金流: {core_data['operating_cf_per_share']}")
            operating_cf_found = True
        
        # 如果直接提取失败，尝试通过经营活动现金流量净额和总股本计算
        if not operating_cf_found:
            logger.debug("尝试通过经营活动现金流量净额和总股本计算每股经营现金流")
            
            # 首先尝试从existing_data中获取经营活动现金流量净额
            operating_cf_net = existing_data.get('operating_cf_net_amount')
            if operating_cf_net is not None:
                logger.debug(f"从existing_data中获取到经营活动现金流量净额: {operating_cf_net}")
            else:
                # 从文本中提取经营活动现金流量净额
                operating_cf_net_patterns = [
                    # 优先匹配年报摘要中的格式（处理换行）
                    r'经营活动产生[\s\S]{0,50}?的现金流量净[\s\S]{0,50}?额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配带单位的经营活动现金流量净额
                    r'经营活动产生的现金流量净额[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量净额[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额[\s:：]*[（\(]?元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配括号格式
                    r'经营活动产生的现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配表格中的数据
                    r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                    r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                    r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
                    r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
                    # 匹配带冒号的格式
                    r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
                    r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
                    r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
                    r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*元',
                    r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配广泛模式
                    r'经营活动产生的现金流量净额.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配表格中的年份数据
                    r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
                    r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
                    # 匹配简化格式
                    r'经营活动现金流量净额[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配带空格的格式
                    r'经营活动产生的现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 新增：匹配更多可能的表述
                    r'经营活动产生的现金流量[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
                    r'经营活动现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
                    r'经营活动现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
                    r'经营活动产生的现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                    r'经营活动现金流[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                    r'经营活动现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                    # 新增：匹配更多可能的表述
                    r'经营活动现金流量净额[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量净额\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量净额\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动产生的现金流量\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'经营活动现金流量\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配表格中的数据行
                    r'经营活动现金流量净额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
                    r'经营活动产生的现金流量[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
                    r'经营活动现金流[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
                    r'经营活动现金流量[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}'
                ]
                for pattern in operating_cf_net_patterns:
                    matches = re.findall(pattern, text_content)
                    if matches:
                        logger.debug(f"经营活动现金流量净额模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
                        for match in matches:
                            cf_value = self.clean_number(match)
                            if cf_value is not None and abs(cf_value) < 100000000:  # 合理范围检查
                                # 检查单位，如果是元，转换为万元
                                if '元' in pattern and '万' not in pattern:
                                    cf_value = cf_value / 10000
                                # 检查数值大小，如果很大，可能是元单位
                                elif cf_value > 1000000:
                                    cf_value = cf_value / 10000
                                operating_cf_net = cf_value
                                logger.debug(f"从文本提取到经营活动现金流量净额: {operating_cf_net}")
                                break
                    if operating_cf_net is not None:
                        break
            
            # 提取总股本
            total_share = None
            # 1. 尝试从existing_data中获取总股本（如果资产负债表中有股本数据）
            if 'equity_total_equity' in existing_data and 'net_asset_per_share' in existing_data:
                equity_total_equity = existing_data['equity_total_equity']
                net_asset_per_share = existing_data['net_asset_per_share']
                if equity_total_equity is not None and net_asset_per_share is not None and net_asset_per_share > 0:
                    # 通过股东权益和每股净资产计算总股本
                    total_share = equity_total_equity / net_asset_per_share
                    if 0 < total_share < 1000000:  # 合理的总股本范围（万股）
                        logger.debug(f"通过股东权益({equity_total_equity})和每股净资产({net_asset_per_share})计算得到总股本: {total_share} 万股")
            
            # 2. 尝试从existing_data中获取总股本（如果资产负债表中有股本项目）
            # 注意：这里只是作为一个参考，仍然会尝试从文本中提取总股本，因为资产负债表中的股本可能不是最新的
            if total_share is None and 'equity_capital' in existing_data:
                equity_capital = existing_data['equity_capital']
                if equity_capital is not None and 0 < equity_capital < 1000000:
                    total_share = equity_capital
                    logger.debug(f"从existing_data中获取到股本: {total_share} 万股")
            
            # 3. 无论是否从existing_data中获取到股本，都尝试从文本中提取总股本，因为文本中的总股本可能更准确
            # 存储从existing_data中获取的股本值，用于比较
            existing_total_share = total_share
            
            # 3. 尝试从文本中提取总股本（无论是否从existing_data中获取到股本）
            total_share_patterns = [
                    # 匹配带单位的总股本
                    r'总股本[\s:：]*[（\(]?万股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本[\s:：]*[（\(]?股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*\(万股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*\(股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数[\s:：]*[（\(]?万股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数[\s:：]*[（\(]?股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配表格中的总股本
                    r'总股本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万股',
                    r'股本总数[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万股',
                    # 匹配带冒号的总股本
                    r'总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万股',
                    r'股本总数\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万股',
                    r'总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配公司总股本
                    r'公司总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万股',
                    r'公司总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配表格中的数据
                    r'总股本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
                    r'股本总数[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
                    # 匹配更广泛的模式
                    r'总股本.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配不带单位的总股本
                    r'总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 新增模式：匹配更多格式
                    r'总股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'公司总股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万股',
                    r'股本总数\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万股',
                    # 匹配简化格式
                    r'总股本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配带空格的格式
                    r'总股本\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本总数\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 新增模式：匹配股本项目
                    r'股本\s*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[（\(]?股[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*\(股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 新增更多模式
                    r'公司股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*万股)',
                    r'公司股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*股)',
                    r'股本总数\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*股)',
                    r'公司总股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*股)',
                    r'公司股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*股)',
                    r'股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*万股)',
                    r'股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*股)',
                    # 匹配表格中的股本数据
                    r'股本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
                    r'股本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?股',
                    r'股本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
                    # 匹配带单位的股本
                    r'股本\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*\(股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    # 匹配简化的股本格式
                    r'股本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*万股)',
                    r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*股)'
            ]
            # 存储所有可能的总股本值，然后选择最合理的
            potential_shares = []
            for pattern in total_share_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    logger.debug(f"总股本模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
                    for match in matches:
                        share_value = self.clean_number(match)
                        if share_value is not None:
                            # 检查单位，如果是股，转换为万股
                            if '股' in pattern and '万' not in pattern:
                                # 检查数值大小，如果很小，可能已经是万股单位
                                if share_value < 1000:
                                    # 假设已经是万股单位
                                    logger.debug(f"假设总股本值已经是万股单位: {share_value} 万股")
                                else:
                                    # 转换为万股单位
                                    share_value = share_value / 10000
                                    logger.debug(f"总股本单位转换（股→万股）: {share_value} 万股")
                            # 检查数值大小，如果很大，可能是股单位
                            elif share_value > 1000000:
                                share_value = share_value / 10000
                                logger.debug(f"总股本单位转换（可能是股单位）: {share_value} 万股")
                            # 确保值在合理范围内
                            if 0.1 <= share_value < 1000000:  # 合理的总股本范围（万股）
                                potential_shares.append(share_value)
                                logger.debug(f"提取到总股本候选值: {share_value} 万股")
            
            # 从候选值中选择最合理的总股本
            if potential_shares:
                # 按大小排序，选择出现次数最多的值，或者最大值（更可能是正确的总股本）
                # 计算每个值的出现次数
                value_counts = {}
                for value in potential_shares:
                    # 四舍五入到2位小数，避免精度问题
                    rounded_value = round(value, 2)
                    if rounded_value in value_counts:
                        value_counts[rounded_value] += 1
                    else:
                        value_counts[rounded_value] = 1
                
                # 选择出现次数最多的值
                most_common_value = max(value_counts, key=value_counts.get)
                # 同时也考虑最大值，因为总股本通常较大
                max_value = max(potential_shares)
                
                # 优先选择最大值，因为错误的单位转换通常会产生较小的值
                if max_value > most_common_value * 10:  # 如果最大值比最常见值大10倍以上，选择最大值
                    text_total_share = max_value
                    logger.debug(f"从文本中选择最大值作为总股本: {text_total_share} 万股")
                else:
                    text_total_share = most_common_value
                    logger.debug(f"从文本中选择最常见值作为总股本: {text_total_share} 万股")
                
                # 比较从文本中提取的总股本和从existing_data中获取的股本，选择更合理的那个
                if existing_total_share is not None:
                    # 如果从文本中提取的总股本比从existing_data中获取的股本大10倍以上，选择文本中的值
                    if text_total_share > existing_total_share * 10:
                        total_share = text_total_share
                        logger.debug(f"文本中的总股本({text_total_share}万股)比existing_data中的股本({existing_total_share}万股)更合理，选择文本中的值")
                    # 如果从existing_data中获取的股本比从文本中提取的股本大10倍以上，选择existing_data中的值
                    elif existing_total_share > text_total_share * 10:
                        total_share = existing_total_share
                        logger.debug(f"existing_data中的股本({existing_total_share}万股)比文本中的总股本({text_total_share}万股)更合理，选择existing_data中的值")
                    # 如果两者相差不大，选择较大的值（更可能是正确的总股本）
                    else:
                        total_share = max(text_total_share, existing_total_share)
                        logger.debug(f"文本中的总股本({text_total_share}万股)和existing_data中的股本({existing_total_share}万股)相差不大，选择较大的值: {total_share} 万股")
                else:
                    # 如果从existing_data中没有获取到股本，使用从文本中提取的总股本
                    total_share = text_total_share
                    logger.debug(f"从existing_data中没有获取到股本，使用从文本中提取的总股本: {total_share} 万股")
            
            # 如果都提取到了，计算每股经营现金流
            if operating_cf_net is not None and total_share is not None and total_share > 0:
                # 确保单位一致：经营活动现金流量净额（万元）/ 总股本（万股） = 每股经营现金流（元）
                logger.debug(f"开始计算每股经营现金流，经营活动现金流量净额: {operating_cf_net} 万元，总股本: {total_share} 万股")
                
                # 计算每股经营现金流
                op_cf_per_share = operating_cf_net / total_share
                
                # 验证计算结果是否合理
                if -100 <= op_cf_per_share <= 100:
                    core_data['operating_cf_per_share'] = op_cf_per_share
                    logger.info(f"通过经营活动现金流量净额({operating_cf_net}万元)和总股本({total_share}万股)计算得到每股经营现金流: {op_cf_per_share}元")
                    operating_cf_found = True
                else:
                    # 如果结果不合理，检查并调整单位
                    logger.debug(f"计算结果不合理: {op_cf_per_share}，尝试调整单位")
                    
                    # 可能性1: 经营活动现金流量净额是元单位，总股本是万股单位
                    if operating_cf_net > 1000000:
                        cf_net_wan = operating_cf_net / 10000  # 转换为万元
                        op_cf_per_share_adjusted = cf_net_wan / total_share
                        if -100 <= op_cf_per_share_adjusted <= 100:
                            core_data['operating_cf_per_share'] = op_cf_per_share_adjusted
                            logger.info(f"调整单位后计算得到每股经营现金流: {op_cf_per_share_adjusted}元")
                            operating_cf_found = True
                    
                    # 可能性2: 总股本是股单位，经营活动现金流量净额是万元单位
                    if not operating_cf_found and total_share < 1:
                        share_value_wan = total_share * 10000  # 转换为万股
                        op_cf_per_share_adjusted = operating_cf_net / share_value_wan
                        if -100 <= op_cf_per_share_adjusted <= 100:
                            core_data['operating_cf_per_share'] = op_cf_per_share_adjusted
                            logger.info(f"调整单位后计算得到每股经营现金流: {op_cf_per_share_adjusted}元")
                            operating_cf_found = True
                    
                    # 可能性3: 经营活动现金流量净额是元单位，总股本是股单位
                    if not operating_cf_found and operating_cf_net > 1000000 and total_share < 1:
                        cf_net_wan = operating_cf_net / 10000  # 转换为万元
                        share_value_wan = total_share * 10000  # 转换为万股
                        op_cf_per_share_adjusted = cf_net_wan / share_value_wan
                        if -100 <= op_cf_per_share_adjusted <= 100:
                            core_data['operating_cf_per_share'] = op_cf_per_share_adjusted
                            logger.info(f"调整单位后计算得到每股经营现金流: {op_cf_per_share_adjusted}元")
                            operating_cf_found = True
                
                if not operating_cf_found:
                    logger.warning(f"计算得到的每股经营现金流值仍然不合理，尝试了多种单位转换组合")
            else:
                logger.warning(f"无法计算每股经营现金流，经营活动现金流量净额: {operating_cf_net}，总股本: {total_share}")
            
            if not operating_cf_found:
                logger.warning("未提取到每股经营现金流数据，无法通过计算获得")

        # 从文本中提取扣除非经常性损益的净利润
        non_recurring_patterns = [
            # 匹配表格中的扣非净利润
            r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 优先匹配更具体的模式，包含更多上下文
            r'扣除非经常性损益的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的模式
            r'扣除非经常性损益的净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
            r'扣非净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万元',
            # 匹配简洁模式
            r'扣除非经常性损益的净利润[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            r'扣非净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            # 新增模式：匹配不同格式的扣非净利润
            r'扣除非经常性损益的净利润[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增更多模式
            r'扣除非经常性损益的净利润\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数值
            r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            # 新增模式：匹配更多可能的表述
            r'归属于上市公司股东的扣除非经常性损益的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东的扣除非经常性损益的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后归属于上市公司股东的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行
            r'扣非净利润[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'扣除非经常性损益的净利润[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配更广泛的模式
            r'扣非.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增：匹配更多可能的表述
            r'归属于上市公司股东的扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东的扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后归属于上市公司股东的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配年报中的格式
            r'扣非净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'扣除非经常性损益的净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            r'归属于上市公司股东的扣除非经常性损益的净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?万元',
            # 匹配简化格式
            r'扣非净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'扣非净利润[\s\S]{0,250}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,150}?\d{4}',
            r'扣除非经常性损益的净利润[\s\S]{0,250}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,150}?\d{4}',
            r'归属于上市公司股东的扣除非经常性损益的净利润[\s\S]{0,250}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,150}?\d{4}',
            # 新增：匹配更多可能的表述
            r'归属于母公司所有者的扣除非经常性损益的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后归属于母公司股东的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后归属于母公司所有者的净利润[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'扣非净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的简洁格式
            r'扣非净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*万元)',
            r'扣非净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*元)',
            r'扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*万元)',
            r'扣除非经常性损益的净利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?=\s*元)',
            # 匹配括号格式的变体
            r'扣非净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配最广泛的模式
            r'扣非.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于.*?扣除非经常性.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据（更广泛）
            r'扣非净利润[\s\S]{0,300}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,200}?万元',
            r'扣除非经常性损益的净利润[\s\S]{0,300}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,200}?万元',
            r'归属于上市公司股东的扣除非经常性损益的净利润[\s\S]{0,300}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,200}?万元',
            # 匹配年报摘要中的格式
            r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}年',
            r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}年',
            # 匹配带单位的其他格式
            r'扣非净利润\s*[（\(]万元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非净利润\s*[（\(]元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[（\(]万元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益的净利润\s*[（\(]元[）\)]\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_non_recurring = []
        for pattern in non_recurring_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"扣非净利润模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                net_profit_excl_non_recurring = self.clean_number(match)
                if net_profit_excl_non_recurring is not None:
                    # 检查文本中是否有元的单位，如果有则转换为万元
                    if '元' in pattern and '万' not in pattern:
                        net_profit_excl_non_recurring = net_profit_excl_non_recurring / 10000
                    # 检查是否需要从元转换为万元（通过数值大小判断）
                    elif net_profit_excl_non_recurring > 1000000:  # 如果数值很大，可能是元单位
                        net_profit_excl_non_recurring = net_profit_excl_non_recurring / 10000
                    potential_non_recurring.append(net_profit_excl_non_recurring)

        # 如果没有提取到扣除非经常性损益的净利润，尝试使用表格和文本提取函数
        if not potential_non_recurring:
            # 尝试从表格中提取
            table_non_recurring = self.extract_table_data(text_content, '扣除非经常性损益的净利润')
            if table_non_recurring is not None:
                potential_non_recurring.append(table_non_recurring)
                logger.info(f"从表格中提取到扣除非经常性损益的净利润: {table_non_recurring}")
            else:
                # 尝试从文本中提取
                text_non_recurring = self.extract_text_data(text_content, '扣除非经常性损益的净利润')
                if text_non_recurring is not None:
                    potential_non_recurring.append(text_non_recurring)
                    logger.info(f"从文本中提取到扣除非经常性损益的净利润: {text_non_recurring}")
                else:
                    # 尝试其他变体
                    variants = ['扣非净利润', '扣除非经常性损益后净利润', '归属于上市公司股东的扣除非经常性损益的净利润', '归属于母公司股东的扣除非经常性损益的净利润']
                    for variant in variants:
                        table_value = self.extract_table_data(text_content, variant)
                        if table_value is not None:
                            potential_non_recurring.append(table_value)
                            logger.info(f"从表格中提取到{variant}: {table_value}")
                            break
                        text_value = self.extract_text_data(text_content, variant)
                        if text_value is not None:
                            potential_non_recurring.append(text_value)
                            logger.info(f"从文本中提取到{variant}: {text_value}")
                            break

        if potential_non_recurring:
            # 选择最合理的值（中位数）
            core_data['net_profit_excl_non_recurring'] = sorted(potential_non_recurring)[len(potential_non_recurring) // 2]
            # 使用多个指标名称变体计算同比增长率
            core_data['net_profit_excl_non_recurring_yoy'] = self.calculate_growth(
                text_content, 
                ['扣非净利润', '扣除非经常性损益的净利润', '扣除非经常性损益后净利润'], 
                '同比'
            )
            
            # 如果没有找到同比增长率，尝试从比较财务报表中计算
            if core_data['net_profit_excl_non_recurring_yoy'] is None:
                core_data['net_profit_excl_non_recurring_yoy'] = self.calculate_balance_sheet_growth(
                    text_content, 
                    '扣除非经常性损益的净利润'
                )
                if core_data['net_profit_excl_non_recurring_yoy'] is not None:
                    logger.info(f"从比较财务报表计算得到扣非净利润同比增长率: {core_data['net_profit_excl_non_recurring_yoy']}")
            
            # 如果仍然没有找到，尝试从文本中直接提取
            if core_data['net_profit_excl_non_recurring_yoy'] is None:
                # 直接搜索扣非净利润同比增长率的各种表述
                yoy_patterns = [
                    # 匹配带百分号的格式
                    r'扣非净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'同比.*?扣非净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'扣非净利润.*?增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'扣除非经常性损益的净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'扣非净利润.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'扣非净利润.*?同比增长率.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'扣除非经常性损益后净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    # 匹配不带百分号的格式
                    r'扣非净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'同比.*?扣非净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣非净利润.*?增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣除非经常性损益的净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣非净利润.*?同比增长率.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'同比增长率.*?扣非净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣非净利润.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣非净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%.*?同比',
                    r'扣非净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%.*?增长',
                    # 匹配表格中的数值
                    r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%[\s\S]{0,50}?同比',
                    r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%[\s\S]{0,50}?同比',
                    r'扣非净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?同比',
                    r'扣除非经常性损益的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?同比',
                    # 匹配更多表述方式
                    r'扣非.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'同比.*?扣非.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣非.*?增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣除非经常性.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'扣非净利润.*?较上年同期.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'较上年同期.*?扣非净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
                ]
                for pattern in yoy_patterns:
                    matches = re.findall(pattern, text_content)
                    if matches:
                        for match in matches:
                            growth = self.clean_number(match)
                            if growth is not None:
                                if growth > 100 or growth < -100:
                                    core_data['net_profit_excl_non_recurring_yoy'] = growth / 100
                                else:
                                    core_data['net_profit_excl_non_recurring_yoy'] = growth
                                logger.info(f"直接从文本提取到扣非净利润同比增长率: {core_data['net_profit_excl_non_recurring_yoy']}")
                                break
                    if core_data['net_profit_excl_non_recurring_yoy'] is not None:
                        break
            
            logger.debug(f"提取到扣非净利润: {core_data['net_profit_excl_non_recurring']}")
        else:
            # 尝试通过净利润和非经常性损益计算扣非净利润
            logger.debug("尝试通过净利润和非经常性损益计算扣非净利润")
            # 提取非经常性损益
            non_recurring_income_patterns = [
                r'非经常性损益[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'非经常性损益[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'非经常性收益[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'非经常性收益[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
            ]
            non_recurring_income = None
            for pattern in non_recurring_income_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    for match in matches:
                        non_recurring_value = self.clean_number(match)
                        if non_recurring_value is not None:
                            # 检查单位，如果是元，转换为万元
                            if '元' in pattern and '万' not in pattern:
                                non_recurring_value = non_recurring_value / 10000
                            # 检查是否需要从元转换为万元（通过数值大小判断）
                            elif abs(non_recurring_value) > 1000000:  # 如果数值很大，可能是元单位
                                non_recurring_value = non_recurring_value / 10000
                            non_recurring_income = non_recurring_value
                            logger.debug(f"提取到非经常性损益: {non_recurring_income}")
                            break
                if non_recurring_income is not None:
                    break
            
            # 如果提取到非经常性损益，并且有净利润，计算扣非净利润
            if non_recurring_income is not None and 'net_profit_10k_yuan' in core_data:
                net_profit = core_data['net_profit_10k_yuan']
                net_profit_excl_non_recurring = net_profit - non_recurring_income
                core_data['net_profit_excl_non_recurring'] = net_profit_excl_non_recurring
                logger.info(f"通过净利润({net_profit})和非经常性损益({non_recurring_income})计算得到扣非净利润: {net_profit_excl_non_recurring}")
            else:
                logger.warning("未提取到扣非净利润数据，无法通过计算获得")

        # 从文本中提取加权平均净资产收益率（扣非）
        roe_non_recurring_patterns = [
            # 优先匹配年报摘要中的格式（处理换行）
            r'扣除非经常性损益后的加权平均净资产收益率[\s\S]{0,50}?（%）[\s\S]{0,200}?([+-]?\d+\.?\d*)',
            r'扣非[\s\S]{0,50}?加权平均净资产收益率[\s\S]{0,50}?（%）[\s\S]{0,200}?([+-]?\d+\.?\d*)',
            # 匹配表格中的加权平均净资产收益率（扣非）
            r'扣除非经常性损益后的加权平均净资产收益率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'扣非加权平均净资产收益率[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'扣非ROE[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            # 优先匹配更具体的模式，包含更多上下文
            r'扣除非经常性损益后的加权平均净资产收益率\s*[（\(]%[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权平均净资产收益率\s*[（\(]%[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带单位的模式
            r'扣除非经常性损益后的加权平均净资产收益率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权平均净资产收益率\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE\s*\(%\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简洁模式
            r'扣除非经常性损益后的加权平均净资产收益率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权平均净资产收益率[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE[\s:：]*[（\(]?[%]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后的加权平均净资产收益率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权平均净资产收益率[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的加权平均净资产收益率（扣非）
            r'扣除非经常性损益后的加权平均净资产收益率[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权平均净资产收益率[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE[\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后的加权平均净资产收益率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权平均净资产收益率\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            # 新增：匹配更多可能的表述
            r'扣除非经常性损益后的ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非加权ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\(扣非\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'加权平均净资产收益率\s*\(扣非\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配年报中的格式
            r'扣非加权平均净资产收益率[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'扣除非经常性损益后的加权平均净资产收益率[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            r'扣非ROE[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?%',
            # 匹配简化格式
            r'扣非加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣除非经常性损益后的加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'扣非ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'扣非加权平均净资产收益率[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%',
            r'扣除非经常性损益后的加权平均净资产收益率[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%',
            r'扣非ROE[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?%',
            # 新增：匹配更多可能的表述
            r'扣非加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            r'扣除非经常性损益后的加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            r'扣非ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*%)',
            r'扣非加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'扣除非经常性损益后的加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'扣非ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%',
            r'扣非加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*百分比',
            r'扣除非经常性损益后的加权平均净资产收益率\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*百分比',
            r'扣非ROE\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*百分比'
        ]
        roe_non_recurring_found = False
        potential_roe_non_recurring = []
        for pattern in roe_non_recurring_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"加权平均净资产收益率（扣非）模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                roe_weighted_excl_non_recurring = self.clean_number(match)
                if roe_weighted_excl_non_recurring is not None and -100 <= roe_weighted_excl_non_recurring <= 100:  # 合理范围
                    potential_roe_non_recurring.append(roe_weighted_excl_non_recurring)

        if potential_roe_non_recurring:
            # 选择最合理的值（中位数）
            core_data['roe_weighted_excl_non_recurring'] = sorted(potential_roe_non_recurring)[len(potential_roe_non_recurring) // 2]
            logger.debug(f"提取到加权平均净资产收益率（扣非）: {core_data['roe_weighted_excl_non_recurring']}")
            roe_non_recurring_found = True
        else:
            # 如果没有提取到加权平均净资产收益率（扣非），可以考虑使用普通ROE作为备选
            if 'roe' in core_data:
                core_data['roe_weighted_excl_non_recurring'] = core_data['roe']
                logger.info(f"使用普通ROE作为加权平均净资产收益率（扣非）的备选值: {core_data['roe']}")
                roe_non_recurring_found = True
            # 如果普通ROE也没有，尝试通过扣非净利润和股东权益计算
            elif 'net_profit_excl_non_recurring' in core_data:
                # 尝试提取股东权益
                equity_patterns = [
                    r'股东权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'所有者权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'归属于母公司股东权益合计[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
                ]
                total_equity = None
                for pattern in equity_patterns:
                    matches = re.findall(pattern, text_content)
                    if matches:
                        for match in matches:
                            equity_value = self.clean_number(match)
                            if equity_value is not None and equity_value > 0:
                                # 检查单位，如果是元，转换为万元
                                if '元' in pattern and '万' not in pattern:
                                    equity_value = equity_value / 10000
                                # 检查数值大小，如果很大，可能是元单位
                                elif equity_value > 1000000:
                                    equity_value = equity_value / 10000
                                total_equity = equity_value
                                logger.debug(f"提取到股东权益: {total_equity}")
                                break
                    if total_equity is not None:
                        break
                
                if total_equity is not None and total_equity > 0:
                    net_profit_excl_non_recurring = core_data['net_profit_excl_non_recurring']
                    # 计算ROE（扣非）= 扣非净利润 / 股东权益 * 100
                    roe_weighted_excl_non_recurring = (net_profit_excl_non_recurring / total_equity) * 100
                    if -100 <= roe_weighted_excl_non_recurring <= 100:
                        core_data['roe_weighted_excl_non_recurring'] = roe_weighted_excl_non_recurring
                        logger.info(f"通过扣非净利润({net_profit_excl_non_recurring})和股东权益({total_equity})计算得到加权平均净资产收益率（扣非）: {roe_weighted_excl_non_recurring}")
                        roe_non_recurring_found = True

        if not roe_non_recurring_found:
            logger.warning("未提取到合理的加权平均净资产收益率（扣非）数据")

        # 从文本中提取营业总收入环比增长率
        core_data['operating_revenue_qoq_growth'] = self.calculate_growth(
            text_content, 
            ['营业收入', '营业总收入', '总营收'], 
            '环比'
        )

        # 从文本中提取净利润环比增长率
        # 首先尝试使用多个指标名称变体计算环比增长率
        core_data['net_profit_qoq_growth'] = self.calculate_growth(
            text_content, 
            ['净利润', '归属于上市公司股东的净利润', '归母净利润', '归属于母公司所有者的净利润'], 
            '环比'
        )
        
        # 如果没有找到环比增长率，尝试从比较财务报表中计算
        if core_data['net_profit_qoq_growth'] is None:
            core_data['net_profit_qoq_growth'] = self.calculate_balance_sheet_growth(
                text_content, 
                '归属于上市公司股东的净利润'
            )
            if core_data['net_profit_qoq_growth'] is not None:
                logger.info(f"从比较财务报表计算得到净利润环比增长率: {core_data['net_profit_qoq_growth']}")
        
        # 如果仍然没有找到，尝试从文本中直接提取
        if core_data['net_profit_qoq_growth'] is None:
            # 直接搜索净利润环比增长率的各种表述
            qoq_patterns = [
                # 标准格式
                r'净利润.*?环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'环比.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'净利润.*?增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                # 具体指标格式
                r'归属于上市公司股东的净利润.*?环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'归母净利润.*?环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                # 增长率表述
                r'净利润.*?环比增长率.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'环比增长率.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'净利润.*?环比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                # 带百分号的格式
                r'净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%.*?环比',
                r'净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%.*?增长',
                # 表格中的数值
                r'净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%[\s\S]{0,50}?环比',
                r'归属于上市公司股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%[\s\S]{0,50}?环比',
                # 新增更多模式
                r'净利润.*?较上季度.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'较上季度.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'净利润.*?环比变动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'环比变动.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'净利润.*?Q[1-4].*?Q[1-4].*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'Q[1-4].*?Q[1-4].*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                # 新增：匹配更多格式
                r'净利润.*?环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'环比.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'净利润.*?增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归属于上市公司股东的净利润.*?环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'归母净利润.*?环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                # 匹配表格中的数据行
                r'净利润[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?环比',
                r'归属于上市公司股东的净利润[\s\S]{0,150}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?环比',
                # 匹配年报中的格式
                r'净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?环比',
                r'归属于上市公司股东的净利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?环比',
                # 新增模式：匹配更多可能的表述
                r'净利润.*?环比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'环比增长.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'净利润.*?季度环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'季度环比.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'净利润.*?较本年前三季度.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'较本年前三季度.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'净利润.*?较上年同期.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                r'较上年同期.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                # 匹配不带百分号的格式
                r'净利润.*?环比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'环比增长.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'净利润.*?季度环比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                r'季度环比.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            ]
            for pattern in qoq_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    for match in matches:
                        growth = self.clean_number(match)
                        if growth is not None:
                            if growth > 100 or growth < -100:
                                core_data['net_profit_qoq_growth'] = growth / 100
                            else:
                                core_data['net_profit_qoq_growth'] = growth
                            logger.info(f"直接从文本提取到净利润环比增长率: {core_data['net_profit_qoq_growth']}")
                            break
                if core_data['net_profit_qoq_growth'] is not None:
                    break

        return core_data

    def extract_balance_sheet(self, text_content):
        """从财报文本中提取资产负债表数据"""
        balance_data = {}
        logger.info("开始提取资产负债表数据")

        # 从文本中提取总资产
        logger.debug("开始提取总资产数据")
        total_assets_patterns = [
            r'资产总计[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总资产[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产总额[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产总计[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总资产[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产总计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            r'总资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            # 新增模式：匹配不同格式的总资产
            r'资产总计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总资产\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产总额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的总资产数据
            r'资产总计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'总资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}'
        ]
        potential_total_assets = []
        for pattern in total_assets_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"总资产模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                total_assets = self.clean_number(match)
                if total_assets is not None:
                    # 检查文本中是否有元的单位，如果有则转换为万元
                    if '元' in pattern and '万' not in pattern:
                        total_assets = total_assets / 10000
                    potential_total_assets.append(total_assets)

        if potential_total_assets:
            # 选择最合理的值（中位数）
            balance_data['asset_total_assets'] = sorted(potential_total_assets)[len(potential_total_assets) // 2]
            
            # 尝试从比较资产负债表中计算总资产增长率
            asset_growth = self.calculate_balance_sheet_growth(text_content, '资产总计')
            if asset_growth is not None:
                balance_data['asset_total_assets_yoy_growth'] = asset_growth
                logger.info(f"计算得到总资产同比增长率: {asset_growth}")
            else:
                # 使用多个指标名称变体来查找总资产同比增长率
                balance_data['asset_total_assets_yoy_growth'] = self.calculate_growth(
                    text_content, ['资产总计', '总资产', '资产总额'], '同比'
                )
                if balance_data['asset_total_assets_yoy_growth'] is not None:
                    logger.info(f"提取到总资产同比增长率: {balance_data['asset_total_assets_yoy_growth']}")
                else:
                    logger.warning("未提取到总资产同比增长率数据")
            logger.info(f"提取到总资产: {balance_data['asset_total_assets']}")
        else:
            logger.warning("未提取到总资产数据")

        # 从文本中提取总负债
        logger.debug("开始提取总负债数据")
        total_liabilities_patterns = [
            r'负债合计[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'负债总计[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总负债[\s:：]*[（\(]?[元万]?[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'负债合计[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总负债[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'负债合计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            r'总负债\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            # 新增模式：匹配不同格式的总负债
            r'负债合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'负债总计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总负债\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的总负债数据
            r'负债合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'总负债[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}'
        ]
        potential_total_liabilities = []
        for pattern in total_liabilities_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"总负债模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                total_liabilities = self.clean_number(match)
                if total_liabilities is not None:
                    # 检查文本中是否有元的单位，如果有则转换为万元
                    if '元' in pattern and '万' not in pattern:
                        total_liabilities = total_liabilities / 10000
                    potential_total_liabilities.append(total_liabilities)

        if potential_total_liabilities:
            # 选择最合理的值（中位数）
            balance_data['liability_total_liabilities'] = sorted(potential_total_liabilities)[len(potential_total_liabilities) // 2]
            
            # 尝试从比较资产负债表中计算总负债增长率
            liability_growth = self.calculate_balance_sheet_growth(text_content, '负债合计')
            if liability_growth is not None:
                balance_data['liability_total_liabilities_yoy_growth'] = liability_growth
                logger.info(f"计算得到总负债同比增长率: {liability_growth}")
            else:
                # 使用多个指标名称变体来查找总负债同比增长率
                balance_data['liability_total_liabilities_yoy_growth'] = self.calculate_growth(
                    text_content, ['负债合计', '总负债', '负债总计'], '同比'
                )
                if balance_data['liability_total_liabilities_yoy_growth'] is not None:
                    logger.info(f"提取到总负债同比增长率: {balance_data['liability_total_liabilities_yoy_growth']}")
                else:
                    logger.warning("未提取到总负债同比增长率数据")
            logger.info(f"提取到总负债: {balance_data['liability_total_liabilities']}")
        else:
            logger.warning("未提取到总负债数据")

        # 从文本中提取股东权益
        logger.debug("开始提取股东权益数据")
        total_equity_patterns = [
            # 匹配带单位的股东权益
            r'股东权益合计\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股东权益合计\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股东权益合计\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'股东权益合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股东权益合计\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股东权益合计\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'股东权益合计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            r'所有者权益合计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            r'归属于母公司股东权益合计\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*[元万])',
            # 匹配带单位后缀的格式
            r'股东权益合计\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'所有者权益合计\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'归属于母公司股东权益合计\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'股东权益合计\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'所有者权益合计\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'归属于母公司股东权益合计\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            # 匹配表格中的数据
            r'股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'所有者权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'归属于母公司股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'所有者权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'归属于母公司股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'所有者权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'归属于母公司股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            # 匹配表格中的年份数据
            r'股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'所有者权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'归属于母公司股东权益合计[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'股东权益合计.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股东权益.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者权益.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'股东权益合计\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'股东权益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者权益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'股东权益合计\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益合计\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益合计\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股东权益\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'所有者权益\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司股东权益\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者权益\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'股东权益合计[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'所有者权益合计[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'归属于母公司股东权益[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'归属于母公司所有者权益[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}'
        ]
        potential_total_equity = []
        # 存储每个匹配的单位信息
        unit_info = []
        for pattern in total_equity_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"股东权益模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
                # 提取单位信息
                unit = '万元'  # 默认单位
                if '亿元' in pattern or '亿' in pattern:
                    unit = '亿元'
                elif '元' in pattern:
                    unit = '元'
                for match in matches:
                    total_equity = self.clean_number(match)
                    if total_equity is not None:
                        potential_total_equity.append(total_equity)
                        unit_info.append(unit)

        if potential_total_equity:
            # 选择最合理的值（中位数）
            total_equity_value = sorted(potential_total_equity)[len(potential_total_equity) // 2]
            # 获取对应的值的单位
            selected_unit = unit_info[len(potential_total_equity) // 2] if unit_info else '万元'
            
            # 根据单位转换为万元
            if selected_unit == '亿元':
                total_equity_value = total_equity_value * 10000
                logger.info(f"股东权益单位转换（亿元→万元）: {total_equity_value}")
            elif selected_unit == '元':
                total_equity_value = total_equity_value / 10000
                logger.info(f"股东权益单位转换（元→万元）: {total_equity_value}")
            
            # 额外的单位检查逻辑
            if total_equity_value < 0.1 and selected_unit != '亿元':
                # 如果值很小，可能是亿元单位
                total_equity_value = total_equity_value * 10000
                logger.info(f"股东权益单位转换（可能是亿元→万元）: {total_equity_value}")
            elif total_equity_value > 1000000 and selected_unit != '元':
                # 如果值很大，可能是元单位
                total_equity_value = total_equity_value / 10000
                logger.info(f"股东权益单位转换（可能是元→万元）: {total_equity_value}")
            
            balance_data['equity_total_equity'] = total_equity_value
            logger.info(f"提取到股东权益: {balance_data['equity_total_equity']}")
        else:
            logger.warning("未提取到股东权益数据")

        # 从文本中提取货币资金
        logger.debug("开始提取货币资金数据")
        cash_patterns = [
            r'货币资金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'货币资金\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'货币资金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'货币资金[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cash = []
        for pattern in cash_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"货币资金模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    cash_value = self.clean_number(match[1])
                else:
                    cash_value = self.clean_number(match)
                if cash_value is not None:
                    potential_cash.append(cash_value)

        if potential_cash:
            # 选择最合理的值（中位数）
            balance_data['asset_cash_and_cash_equivalents'] = sorted(potential_cash)[len(potential_cash) // 2]
            logger.info(f"提取到货币资金: {balance_data['asset_cash_and_cash_equivalents']}")
        else:
            logger.warning("未提取到货币资金数据")

        # 从文本中提取应收账款
        logger.debug("开始提取应收账款数据")
        accounts_receivable_patterns = [
            r'应收账款\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'应收账款\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'应收账款\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'应收账款[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_ar = []
        for pattern in accounts_receivable_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"应收账款模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    ar_value = self.clean_number(match[1])
                else:
                    ar_value = self.clean_number(match)
                if ar_value is not None:
                    potential_ar.append(ar_value)

        if potential_ar:
            # 选择最合理的值（中位数）
            balance_data['asset_accounts_receivable'] = sorted(potential_ar)[len(potential_ar) // 2]
            logger.info(f"提取到应收账款: {balance_data['asset_accounts_receivable']}")
        else:
            logger.warning("未提取到应收账款数据")

        # 从文本中提取存货
        logger.debug("开始提取存货数据")
        inventory_patterns = [
            r'存货\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'存货\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'存货\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'存货[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_inventory = []
        for pattern in inventory_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"存货模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    inventory_value = self.clean_number(match[1])
                else:
                    inventory_value = self.clean_number(match)
                if inventory_value is not None:
                    potential_inventory.append(inventory_value)

        if potential_inventory:
            # 选择最合理的值（中位数）
            balance_data['asset_inventory'] = sorted(potential_inventory)[len(potential_inventory) // 2]
            logger.info(f"提取到存货: {balance_data['asset_inventory']}")
        else:
            logger.warning("未提取到存货数据")

        # 从文本中提取交易性金融资产
        logger.debug("开始提取交易性金融资产数据")
        trading_assets_patterns = [
            r'交易性金融资产\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'交易性金融资产\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'交易性金融资产\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'交易性金融资产[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式
            r'交易性金融资产\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'交易性金融资产\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'交易性金融资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'交易性金融资产[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元'
        ]
        potential_trading = []
        for pattern in trading_assets_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"交易性金融资产模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    trading_value = self.clean_number(match[1])
                else:
                    trading_value = self.clean_number(match)
                if trading_value is not None:
                    potential_trading.append(trading_value)

        if potential_trading:
            # 选择最合理的值（中位数）
            balance_data['asset_trading_financial_assets'] = sorted(potential_trading)[len(potential_trading) // 2]
            logger.info(f"提取到交易性金融资产: {balance_data['asset_trading_financial_assets']}")
        else:
            logger.warning("未提取到交易性金融资产数据")

        # 从文本中提取在建工程
        logger.debug("开始提取在建工程数据")
        construction_patterns = [
            r'在建工程\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'在建工程\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'在建工程\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'在建工程[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_construction = []
        for pattern in construction_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"在建工程模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    construction_value = self.clean_number(match[1])
                else:
                    construction_value = self.clean_number(match)
                if construction_value is not None:
                    potential_construction.append(construction_value)

        if potential_construction:
            # 选择最合理的值（中位数）
            balance_data['asset_construction_in_progress'] = sorted(potential_construction)[len(potential_construction) // 2]
            logger.info(f"提取到在建工程: {balance_data['asset_construction_in_progress']}")
        else:
            logger.warning("未提取到在建工程数据")

        # 从文本中提取应付账款
        logger.debug("开始提取应付账款数据")
        accounts_payable_patterns = [
            r'应付账款\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'应付账款\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'应付账款\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'应付账款[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_ap = []
        for pattern in accounts_payable_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"应付账款模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    ap_value = self.clean_number(match[1])
                else:
                    ap_value = self.clean_number(match)
                if ap_value is not None:
                    potential_ap.append(ap_value)

        if potential_ap:
            # 选择最合理的值（中位数）
            balance_data['liability_accounts_payable'] = sorted(potential_ap)[len(potential_ap) // 2]
            logger.info(f"提取到应付账款: {balance_data['liability_accounts_payable']}")
        else:
            logger.warning("未提取到应付账款数据")

        # 从文本中提取预收账款
        logger.debug("开始提取预收账款数据")
        advance_patterns = [
            r'预收款项\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'预收账款\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'预收账款\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'预收款项\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'预收账款\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'预收款项[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'预收账款[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_advance = []
        for pattern in advance_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"预收账款模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    advance_value = self.clean_number(match[1])
                else:
                    advance_value = self.clean_number(match)
                if advance_value is not None:
                    potential_advance.append(advance_value)

        if potential_advance:
            # 选择最合理的值（中位数）
            balance_data['liability_advance_from_customers'] = sorted(potential_advance)[len(potential_advance) // 2]
            logger.info(f"提取到预收账款: {balance_data['liability_advance_from_customers']}")
        else:
            logger.warning("未提取到预收账款数据")

        # 从文本中提取合同负债
        logger.debug("开始提取合同负债数据")
        contract_liabilities_patterns = [
            r'合同负债\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'合同负债\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'合同负债\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'合同负债[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_contract = []
        for pattern in contract_liabilities_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"合同负债模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    contract_value = self.clean_number(match[1])
                else:
                    contract_value = self.clean_number(match)
                if contract_value is not None:
                    potential_contract.append(contract_value)

        if potential_contract:
            # 选择最合理的值（中位数）
            balance_data['liability_contract_liabilities'] = sorted(potential_contract)[len(potential_contract) // 2]
            logger.info(f"提取到合同负债: {balance_data['liability_contract_liabilities']}")
        else:
            logger.warning("未提取到合同负债数据")

        # 从文本中提取短期借款
        logger.debug("开始提取短期借款数据")
        short_term_loans_patterns = [
            r'短期借款\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'短期借款\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'短期借款\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'短期借款[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_st_loans = []
        for pattern in short_term_loans_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"短期借款模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    st_loan_value = self.clean_number(match[1])
                else:
                    st_loan_value = self.clean_number(match)
                if st_loan_value is not None:
                    potential_st_loans.append(st_loan_value)

        if potential_st_loans:
            # 选择最合理的值（中位数）
            balance_data['liability_short_term_loans'] = sorted(potential_st_loans)[len(potential_st_loans) // 2]
            logger.info(f"提取到短期借款: {balance_data['liability_short_term_loans']}")
        else:
            logger.warning("未提取到短期借款数据")

        # 从文本中提取股本
        logger.debug("开始提取股本数据")
        equity_capital_patterns = [
            r'股本\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本\s*[（\(]?股[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本\s*\(股\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*万股',
            r'股本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*股'
        ]
        potential_capital = []
        for pattern in equity_capital_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"股本模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    capital_value = self.clean_number(match[1])
                else:
                    capital_value = self.clean_number(match)
                if capital_value is not None:
                    # 检查单位，如果是股，转换为万股
                    if '股' in pattern and '万' not in pattern:
                        # 检查数值大小，如果很小，可能已经是万股单位
                        if capital_value < 1000:
                            # 假设已经是万股单位
                            logger.debug(f"假设股本值已经是万股单位: {capital_value} 万股")
                        else:
                            # 转换为万股单位
                            capital_value = capital_value / 10000
                            logger.debug(f"股本单位转换（股→万股）: {capital_value} 万股")
                    # 检查数值大小，如果很大，可能是股单位
                    elif capital_value > 1000000:
                        capital_value = capital_value / 10000
                        logger.debug(f"股本单位转换（可能是股单位）: {capital_value} 万股")
                    # 确保值在合理范围内（1万股到100亿股之间）
                    if 0.1 <= capital_value < 1000000:
                        potential_capital.append(capital_value)
                        logger.debug(f"提取到股本候选值: {capital_value} 万股")

        if potential_capital:
            # 选择最合理的值（中位数）
            balance_data['equity_capital'] = sorted(potential_capital)[len(potential_capital) // 2]
            logger.info(f"提取到股本: {balance_data['equity_capital']}")
        else:
            logger.warning("未提取到股本数据")

        # 从文本中提取未分配利润
        logger.debug("开始提取未分配利润数据")
        unappropriated_profit_patterns = [
            r'未分配利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'未分配利润\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'未分配利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'未分配利润[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_profit = []
        for pattern in unappropriated_profit_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"未分配利润模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    profit_value = self.clean_number(match[1])
                else:
                    profit_value = self.clean_number(match)
                if profit_value is not None:
                    potential_profit.append(profit_value)

        if potential_profit:
            # 选择最合理的值（中位数）
            balance_data['equity_unappropriated_profit'] = sorted(potential_profit)[len(potential_profit) // 2]
            logger.info(f"提取到未分配利润: {balance_data['equity_unappropriated_profit']}")
        else:
            logger.warning("未提取到未分配利润数据")

        # 计算资产负债率
        if 'asset_total_assets' in balance_data and 'liability_total_liabilities' in balance_data:
            if balance_data['asset_total_assets'] > 0:
                balance_data['asset_liability_ratio'] = balance_data['liability_total_liabilities'] / balance_data['asset_total_assets'] * 100
                logger.info(f"计算得到资产负债率: {balance_data['asset_liability_ratio']}")
        else:
            logger.warning("无法计算资产负债率，缺少总资产或总负债数据")

        # 统计提取成功的字段数量
        non_none_count = sum(1 for v in balance_data.values() if v is not None)
        total_count = len(balance_data)
        logger.info(f"资产负债表数据提取完成，成功提取 {non_none_count}/{total_count} 个字段")

        return balance_data

    def extract_cash_flow(self, text_content):
        """从财报文本中提取现金流量表数据"""
        cash_flow_data = {}
        logger.info("开始提取现金流量表数据")

        # 从文本中提取经营活动产生的现金流量净额
        logger.debug("开始提取经营活动产生的现金流量净额数据")
        operating_cf_patterns = [
            # 匹配带单位的经营活动现金流量净额
            r'经营活动产生的现金流量净额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'经营活动产生的现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万)',
            r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万)',
            r'经营活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿)',
            r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿)',
            # 匹配带单位后缀的格式
            r'经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'经营活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'经营活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            r'经营活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            r'经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万)',
            r'经营活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万)',
            r'经营活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿)',
            r'经营活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿)',
            # 匹配表格中的数据
            r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'经营活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'经营活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'经营活动产生的现金流量净额.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金流量净额.*?经营活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动.*?现金.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金.*?经营活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'经营活动产生的现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'经营活动.*?现金流量.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金流量.*?经营活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营现金流.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金流量.*?净额.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增：匹配更多可能的表述
            r'经营活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营现金流\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'经营活动现金流量净额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'经营活动产生的现金流量[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'经营活动现金流[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'经营活动现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            # 新增：匹配更多可能的表述
            r'经营活动现金流量金额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流入\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流出\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动净现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营现金流净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金流量表.*?经营活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动.*?现金流量表.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动产生的现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营活动净现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'经营现金流净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_operating_cf = []
        # 存储每个匹配的单位信息
        unit_info = []
        for pattern in operating_cf_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"经营活动现金流量模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
                # 提取单位信息
                unit = '万元'  # 默认单位
                if '亿元' in pattern or '亿' in pattern:
                    unit = '亿元'
                elif '元' in pattern:
                    unit = '元'
                for match in matches:
                    # 处理不同的匹配组结构
                    if isinstance(match, tuple):
                        cf_value = self.clean_number(match[1])
                    else:
                        cf_value = self.clean_number(match)
                    if cf_value is not None:
                        potential_operating_cf.append(cf_value)
                        unit_info.append(unit)

        if potential_operating_cf:
            # 选择最合理的值（中位数）
            operating_cf_value = sorted(potential_operating_cf)[len(potential_operating_cf) // 2]
            # 获取对应的值的单位
            selected_unit = unit_info[len(potential_operating_cf) // 2] if unit_info else '万元'
            
            # 根据单位转换为万元
            if selected_unit == '亿元':
                operating_cf_value = operating_cf_value * 10000
                logger.info(f"经营活动产生的现金流量净额单位转换（亿元→万元）: {operating_cf_value}")
            elif selected_unit == '元':
                operating_cf_value = operating_cf_value / 10000
                logger.info(f"经营活动产生的现金流量净额单位转换（元→万元）: {operating_cf_value}")
            
            # 额外的单位检查逻辑
            if operating_cf_value < 0.1 and selected_unit != '亿元':
                # 如果值很小，可能是亿元单位
                operating_cf_value = operating_cf_value * 10000
                logger.info(f"经营活动产生的现金流量净额单位转换（可能是亿元→万元）: {operating_cf_value}")
            elif operating_cf_value > 1000000 and selected_unit != '元':
                # 如果值很大，可能是元单位
                operating_cf_value = operating_cf_value / 10000
                logger.info(f"经营活动产生的现金流量净额单位转换（可能是元→万元）: {operating_cf_value}")
            
            cash_flow_data['operating_cf_net_amount'] = operating_cf_value
            logger.info(f"提取到经营活动产生的现金流量净额: {cash_flow_data['operating_cf_net_amount']} 万元")
        else:
            logger.warning("未提取到经营活动产生的现金流量净额数据")

        # 从文本中提取投资活动产生的现金流量净额
        logger.debug("开始提取投资活动产生的现金流量净额数据")
        investing_cf_patterns = [
            r'投资活动产生的现金流量净额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'投资活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'投资活动产生的现金流量净额[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的投资活动现金流量
            r'投资活动产生的现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的投资活动现金流量数据
            r'投资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'投资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 新增模式：匹配更多格式的投资活动现金流量
            r'投资活动产生的现金流量净额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'投资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'投资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'投资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'投资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'投资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'投资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'投资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'投资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'投资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'投资活动产生的现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'投资活动现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'投资活动产生的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'投资活动现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'现金流量净额.*?投资活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动.*?现金.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金.*?投资活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'投资活动产生的现金流量[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'投资活动现金流[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'投资活动现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'投资活动现金流量金额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流入\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流出\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动净现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资现金流净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金流量表.*?投资活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动.*?现金流量表.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动产生的现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资活动净现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资现金流净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_investing_cf = []
        for pattern in investing_cf_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"投资活动现金流量模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    cf_value = self.clean_number(match[1])
                else:
                    cf_value = self.clean_number(match)
                if cf_value is not None:
                    potential_investing_cf.append(cf_value)

        if potential_investing_cf:
            # 选择最合理的值（中位数）
            cash_flow_data['investing_cf_net_amount'] = sorted(potential_investing_cf)[len(potential_investing_cf) // 2]
            logger.info(f"提取到投资活动产生的现金流量净额: {cash_flow_data['investing_cf_net_amount']}")
        else:
            logger.warning("未提取到投资活动产生的现金流量净额数据")

        # 从文本中提取筹资活动产生的现金流量净额
        logger.debug("开始提取筹资活动产生的现金流量净额数据")
        financing_cf_patterns = [
            r'筹资活动产生的现金流量净额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'筹资活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'筹资活动产生的现金流量净额[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的筹资活动现金流量
            r'筹资活动产生的现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的筹资活动现金流量数据
            r'筹资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'筹资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 新增模式：匹配更多格式的筹资活动现金流量
            r'筹资活动产生的现金流量净额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'筹资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'筹资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'筹资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'筹资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'筹资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'筹资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'筹资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'筹资活动产生的现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'筹资活动现金流量净额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'筹资活动产生的现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'筹资活动现金流量[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'筹资活动产生的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'筹资活动现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'现金流量净额.*?筹资活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动.*?现金.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金.*?筹资活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'筹资活动产生的现金流量[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'筹资活动现金流[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'筹资活动现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'筹资活动现金流量金额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流入\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流出\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动净现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资现金流净额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金流量表.*?筹资活动.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动.*?现金流量表.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动产生的现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资活动净现金流量\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'筹资现金流净额\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_financing_cf = []
        for pattern in financing_cf_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"筹资活动现金流量模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    cf_value = self.clean_number(match[1])
                else:
                    cf_value = self.clean_number(match)
                if cf_value is not None:
                    potential_financing_cf.append(cf_value)

        if potential_financing_cf:
            # 选择最合理的值（中位数）
            cash_flow_data['financing_cf_net_amount'] = sorted(potential_financing_cf)[len(potential_financing_cf) // 2]
            logger.info(f"提取到筹资活动产生的现金流量净额: {cash_flow_data['financing_cf_net_amount']}")
        else:
            logger.warning("未提取到筹资活动产生的现金流量净额数据")

        # 计算现金及现金等价物净增加额
        if 'operating_cf_net_amount' in cash_flow_data and 'investing_cf_net_amount' in cash_flow_data and 'financing_cf_net_amount' in cash_flow_data:
            # 保持万元单位，不转换为元（所有数据都是万元单位）
            net_cash_flow_wan = (
                cash_flow_data['operating_cf_net_amount'] +
                cash_flow_data['investing_cf_net_amount'] +
                cash_flow_data['financing_cf_net_amount']
            )
            # 转换为元单位（数据库要求 net_cash_flow 单位为元）
            cash_flow_data['net_cash_flow'] = net_cash_flow_wan * 10000
            logger.info(f"计算得到现金及现金等价物净增加额：{cash_flow_data['net_cash_flow']} 元")
            
            # 计算各部分占比
            if net_cash_flow_wan != 0:  # 使用万元单位计算占比
                cash_flow_data['operating_cf_ratio_of_net_cf'] = cash_flow_data['operating_cf_net_amount'] / net_cash_flow_wan * 100
                cash_flow_data['investing_cf_ratio_of_net_cf'] = cash_flow_data['investing_cf_net_amount'] / net_cash_flow_wan * 100
                cash_flow_data['financing_cf_ratio_of_net_cf'] = cash_flow_data['financing_cf_net_amount'] / net_cash_flow_wan * 100
                logger.info(f"计算得到经营活动现金流量占比: {cash_flow_data['operating_cf_ratio_of_net_cf']}%")
                logger.info(f"计算得到投资活动现金流量占比: {cash_flow_data['investing_cf_ratio_of_net_cf']}%")
                logger.info(f"计算得到筹资活动现金流量占比: {cash_flow_data['financing_cf_ratio_of_net_cf']}%")
        else:
            logger.warning("无法计算现金及现金等价物净增加额，缺少经营活动、投资活动或筹资活动现金流量数据")

        # 从文本中提取现金及现金等价物净增加额同比增长率
        logger.debug("开始提取现金及现金等价物净增加额同比增长率数据")
        net_cash_flow_yoy_patterns = [
            # 匹配带单位的增长率
            r'现金及现金等价物净增加额同比增长\s*[（\(]%[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金及现金等价物净增加额同比增长\s*[：:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金及现金等价物净增加额同比增长\s*[：:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金及现金等价物净增加额\s*同比增长\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金及现金等价物净增加额\s*同比增长\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'现金及现金等价物净增加额\s+同比增长\s+[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金及现金等价物净增加额\s+同比增长\s+[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'现金及现金等价物净增加\s*同比增长\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金及现金等价物净增加\s*同比增长\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金净增加额\s*同比增长\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金净增加额\s*同比增长\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'现金及现金等价物净增加额同比增长\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金及现金等价物净增加额同比增长\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化的同比增长模式
            r'现金等价物净增加额同比增长\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
            r'现金等价物净增加额同比增长\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配广泛模式
            r'现金及现金等价物净增加额.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金及现金等价物净增加.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金净增加额.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金等价物净增加额.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据
            r'现金及现金等价物净增加额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?同比增长',
            r'现金及现金等价物净增加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?同比增长',
            r'现金净增加额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?同比增长',
            # 匹配冒号格式
            r'现金及现金等价物净增加额同比增长\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金及现金等价物净增加同比增长\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金净增加额同比增长\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配最广泛的模式
            r'现金.*?净增加.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'现金等价物.*?净增加.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_net_cash_flow_yoy = []
        for pattern in net_cash_flow_yoy_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"现金及现金等价物净增加额同比增长模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    growth_value = self.clean_number(match[1])
                else:
                    growth_value = self.clean_number(match)
                if growth_value is not None:
                    potential_net_cash_flow_yoy.append(growth_value)

        if potential_net_cash_flow_yoy:
            # 选择最合理的值（中位数）
            net_cash_flow_yoy_value = sorted(potential_net_cash_flow_yoy)[len(potential_net_cash_flow_yoy) // 2]
            # 确保值在合理范围内（-100%到1000%之间）
            if net_cash_flow_yoy_value < -100:
                net_cash_flow_yoy_value = -100
                logger.info(f"现金及现金等价物净增加额同比增长值调整（低于-100%）: {net_cash_flow_yoy_value}")
            elif net_cash_flow_yoy_value > 1000:
                net_cash_flow_yoy_value = 1000
                logger.info(f"现金及现金等价物净增加额同比增长值调整（高于1000%）: {net_cash_flow_yoy_value}")
            
            cash_flow_data['net_cash_flow_yoy_growth'] = net_cash_flow_yoy_value
            logger.debug(f"提取到现金及现金等价物净增加额同比增长: {cash_flow_data['net_cash_flow_yoy_growth']}")
        else:
            # 尝试从比较现金流量表中计算增长率
            cash_flow_growth = self.calculate_balance_sheet_growth(text_content, '现金及现金等价物净增加额')
            if cash_flow_growth is not None:
                cash_flow_data['net_cash_flow_yoy_growth'] = cash_flow_growth
                logger.info(f"计算得到现金及现金等价物净增加额同比增长率: {cash_flow_growth}")
            else:
                # 使用多个指标名称变体来查找现金及现金等价物净增加额同比增长率
                cash_flow_data['net_cash_flow_yoy_growth'] = self.calculate_growth(
                    text_content, 
                    ['现金及现金等价物净增加额', '现金及现金等价物净增加', '现金净增加额'], 
                    '同比'
                )
                if cash_flow_data['net_cash_flow_yoy_growth'] is not None:
                    logger.info(f"提取到现金及现金等价物净增加额同比增长率: {cash_flow_data['net_cash_flow_yoy_growth']}")
                else:
                    logger.warning("未提取到现金及现金等价物净增加额同比增长率数据")

        # 从文本中提取销售商品、提供劳务收到的现金
        logger.debug("开始提取销售商品、提供劳务收到的现金数据")
        cash_from_sales_patterns = [
            r'销售商品、提供劳务收到的现金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'销售商品、提供劳务收到的现金[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的销售商品、提供劳务收到的现金
            r'销售商品、提供劳务收到的现金\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'销售商品提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'销售商品、提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'销售商品提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'销售商品、提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'销售商品提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'销售商品、提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'销售商品提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'销售商品、提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'销售商品提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'销售商品、提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'销售商品提供劳务收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'销售商品、提供劳务收到的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品、提供劳务收到的现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'销售商品提供劳务收到的现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'销售商品、提供劳务收到的现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售商品提供劳务收到的现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cash_from_sales = []
        for pattern in cash_from_sales_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"销售商品、提供劳务收到的现金模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                cash_value = self.clean_number(match)
                if cash_value is not None:
                    potential_cash_from_sales.append(cash_value)

        if potential_cash_from_sales:
            # 选择最合理的值（中位数）
            cash_flow_data['operating_cf_cash_from_sales'] = sorted(potential_cash_from_sales)[len(potential_cash_from_sales) // 2]
            logger.info(f"提取到销售商品、提供劳务收到的现金: {cash_flow_data['operating_cf_cash_from_sales']}")
        else:
            logger.warning("未提取到销售商品、提供劳务收到的现金数据")

        # 从文本中提取投资支付的现金
        logger.debug("开始提取投资支付的现金数据")
        cash_for_investments_patterns = [
            r'投资支付的现金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'投资支付的现金[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式
            r'投资支付的现金\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'投资支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            # 新增模式：匹配更多格式的投资支付的现金
            r'投资支付的现金\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'投资支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'投资支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'投资支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'投资支付的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'投资支付的现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'投资支付的现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cash_for_investments = []
        for pattern in cash_for_investments_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"投资支付的现金模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                cash_value = self.clean_number(match)
                if cash_value is not None:
                    potential_cash_for_investments.append(cash_value)

        if potential_cash_for_investments:
            # 选择最合理的值（中位数）
            cash_flow_data['investing_cf_cash_for_investments'] = sorted(potential_cash_for_investments)[len(potential_cash_for_investments) // 2]
            logger.info(f"提取到投资支付的现金: {cash_flow_data['investing_cf_cash_for_investments']}")
        else:
            logger.warning("未提取到投资支付的现金数据")

        # 从文本中提取收回投资收到的现金
        logger.debug("开始提取收回投资收到的现金数据")
        cash_from_investment_recovery_patterns = [
            r'收回投资收到的现金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'收回投资收到的现金[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的收回投资收到的现金
            r'收回投资收到的现金\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'收回投资收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'收回投资收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'收回投资收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'收回投资收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'收回投资收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'收回投资收到的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'收回投资收到的现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'收回投资收到的现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cash_from_recovery = []
        for pattern in cash_from_investment_recovery_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"收回投资收到的现金模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                cash_value = self.clean_number(match)
                if cash_value is not None:
                    potential_cash_from_recovery.append(cash_value)

        if potential_cash_from_recovery:
            # 选择最合理的值（中位数）
            cash_flow_data['investing_cf_cash_from_investment_recovery'] = sorted(potential_cash_from_recovery)[len(potential_cash_from_recovery) // 2]
            logger.info(f"提取到收回投资收到的现金: {cash_flow_data['investing_cf_cash_from_investment_recovery']}")
        else:
            logger.warning("未提取到收回投资收到的现金数据")

        # 从文本中提取取得借款收到的现金
        logger.debug("开始提取取得借款收到的现金数据")
        cash_from_borrowing_patterns = [
            # 匹配带单位的取得借款收到的现金
            r'取得借款收到的现金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'取得借款收到的现金\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款收到的现金\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'取得借款收到的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'取得借款收到的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'取得借款收到的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'取得借款收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'取得借款收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'取得借款收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'取得借款收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'取得借款收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'取得借款收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'取得借款收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'取得借款收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'取得借款收到的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'取得借款收到的现金.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'取得借款收到的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'取得借款收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'取得借款收到的现金\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'取得借款收到的现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            # 新增：匹配更多可能的表述
            r'取得借款收到的现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'借款收到的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'取得借款现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'借款现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cash_from_borrowing = []
        for pattern in cash_from_borrowing_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"取得借款收到的现金模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                cash_value = self.clean_number(match)
                if cash_value is not None:
                    potential_cash_from_borrowing.append(cash_value)

        if potential_cash_from_borrowing:
            # 选择最合理的值（中位数）
            cash_flow_data['financing_cf_cash_from_borrowing'] = sorted(potential_cash_from_borrowing)[len(potential_cash_from_borrowing) // 2]
            logger.info(f"提取到取得借款收到的现金: {cash_flow_data['financing_cf_cash_from_borrowing']}")
        else:
            logger.warning("未提取到取得借款收到的现金数据")

        # 从文本中提取偿还债务支付的现金
        logger.debug("开始提取偿还债务支付的现金数据")
        cash_for_debt_repayment_patterns = [
            # 匹配带单位的偿还债务支付的现金
            r'偿还债务支付的现金\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'偿还债务支付的现金\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务支付的现金\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'偿还债务支付的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'偿还债务支付的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'偿还债务支付的现金\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'偿还债务支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'偿还债务支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'偿还债务支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'偿还债务支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'偿还债务支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'偿还债务支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'偿还债务支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'偿还债务支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'偿还债务支付的现金[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'偿还债务支付的现金.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'偿还债务支付的现金\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'偿还债务支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'偿还债务支付的现金\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'偿还债务支付的现金[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            # 新增：匹配更多可能的表述
            r'偿还债务支付的现金\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'债务偿还支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'偿还债务所支付的现金\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cash_for_repayment = []
        for pattern in cash_for_debt_repayment_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"偿还债务支付的现金模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            for match in matches:
                cash_value = self.clean_number(match)
                if cash_value is not None:
                    potential_cash_for_repayment.append(cash_value)

        if potential_cash_for_repayment:
            # 选择最合理的值（中位数）
            cash_flow_data['financing_cf_cash_for_debt_repayment'] = sorted(potential_cash_for_repayment)[len(potential_cash_for_repayment) // 2]
            logger.info(f"提取到偿还债务支付的现金: {cash_flow_data['financing_cf_cash_for_debt_repayment']}")
        else:
            logger.warning("未提取到偿还债务支付的现金数据")



        # 统计提取成功的字段数量
        non_none_count = sum(1 for v in cash_flow_data.values() if v is not None)
        total_count = len(cash_flow_data)
        logger.info(f"现金流量表数据提取完成，成功提取 {non_none_count}/{total_count} 个字段")

        return cash_flow_data

    def extract_income_statement(self, text_content):
        """从财报文本中提取利润表数据"""
        income_data = {}

        # 从文本中提取营业总收入
        revenue_patterns = [
            # 匹配表格中的营业收入（元单位）
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?202[0-9]年',
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            # 匹配表格中的营业收入（万元单位）
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            # 优先匹配更具体的模式，包含更多上下文
            r'营业总收入[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总营收[\s:：]*[（\(]?万元[）\)]?[\s:：]*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的营业总收入
            r'营业总收入\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总营收\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业总收入\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业收入\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'总营收\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的营业收入数据
            r'营业收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'营业总收入[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}'
        ]
        potential_revenues = []
        for pattern in revenue_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                total_revenue = self.clean_number(match)
                if total_revenue is not None:
                    # 检查文本中是否有元的单位，如果有则转换为万元
                    if '元' in pattern and '万' not in pattern:
                        total_revenue = total_revenue / 10000
                    # 检查是否需要从元转换为万元（通过数值大小判断）
                    elif total_revenue > 1000000:  # 如果数值很大，可能是元单位
                        total_revenue = total_revenue / 10000
                    # 只考虑合理的数值
                    if total_revenue >= 1:  # 放宽限制，允许较小的收入值
                        potential_revenues.append(total_revenue)

        if potential_revenues:
            # 选择最大的合理值作为收入
            valid_revenue = max(potential_revenues)
            income_data['total_operating_revenue'] = valid_revenue
            income_data['operating_revenue_yoy_growth'] = self.calculate_growth(text_content, '营业收入', '同比')
            logger.debug(f"提取到营业总收入: {valid_revenue}")

        # 从文本中提取营业成本
        cost_of_sales_patterns = [
            # 匹配带单位的营业成本
            r'营业成本\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业成本\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业成本\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'营业成本\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业成本\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业成本\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'营业成本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'营业成本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'营业成本\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'营业成本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'营业成本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'营业成本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            # 匹配表格中的年份数据
            r'营业成本[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'营业成本.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'营业成本\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'营业成本\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'营业成本\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_cost_of_sales = []
        # 存储每个匹配的单位信息
        unit_info = []
        for pattern in cost_of_sales_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"营业成本模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            # 提取单位信息
            unit = '万元'  # 默认单位
            if '亿元' in pattern or '亿' in pattern:
                unit = '亿元'
            elif '元' in pattern:
                unit = '元'
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    cost_value = self.clean_number(match[1])
                else:
                    cost_value = self.clean_number(match)
                if cost_value is not None:
                    potential_cost_of_sales.append(cost_value)
                    unit_info.append(unit)

        if potential_cost_of_sales:
            # 选择最合理的值（中位数）
            cost_of_sales_value = sorted(potential_cost_of_sales)[len(potential_cost_of_sales) // 2]
            # 获取对应的值的单位
            selected_unit = unit_info[len(potential_cost_of_sales) // 2] if unit_info else '万元'
            
            # 根据单位转换为万元
            if selected_unit == '亿元':
                cost_of_sales_value = cost_of_sales_value * 10000
                logger.info(f"营业成本单位转换（亿元→万元）: {cost_of_sales_value}")
            elif selected_unit == '元':
                cost_of_sales_value = cost_of_sales_value / 10000
                logger.info(f"营业成本单位转换（元→万元）: {cost_of_sales_value}")
            
            # 额外的单位检查逻辑
            if cost_of_sales_value < 0.1 and selected_unit != '亿元':
                # 如果值很小，可能是亿元单位
                cost_of_sales_value = cost_of_sales_value * 10000
                logger.info(f"营业成本单位转换（可能是亿元→万元）: {cost_of_sales_value}")
            elif cost_of_sales_value > 1000000 and selected_unit != '元':
                # 如果值很大，可能是元单位
                cost_of_sales_value = cost_of_sales_value / 10000
                logger.info(f"营业成本单位转换（可能是元→万元）: {cost_of_sales_value}")
            
            # 合理性检查：营业成本应该小于或等于营业收入
            if 'total_operating_revenue' in income_data:
                total_revenue = income_data['total_operating_revenue']
                if cost_of_sales_value > total_revenue * 1.1:  # 允许10%的误差
                    # 如果营业成本大于营业收入，可能是单位问题
                    if selected_unit == '万元':
                        # 尝试转换为元单位
                        cost_of_sales_value = cost_of_sales_value / 10000
                        logger.info(f"营业成本单位转换（可能是元→万元）: {cost_of_sales_value}")
                    elif selected_unit == '元':
                        # 尝试转换为万元单位
                        cost_of_sales_value = cost_of_sales_value * 10000
                        logger.info(f"营业成本单位转换（可能是万元→元）: {cost_of_sales_value}")
            
            income_data['operating_expense_cost_of_sales'] = cost_of_sales_value
            logger.debug(f"提取到营业成本: {income_data['operating_expense_cost_of_sales']}")

        # 从文本中提取销售费用
        selling_expenses_patterns = [
            r'销售费用\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'销售费用\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'销售费用[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的销售费用
            r'销售费用\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'销售费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'销售费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'销售费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'销售费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'销售费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'销售费用\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'销售费用[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'销售费用\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_selling_expenses = []
        for pattern in selling_expenses_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    expense_value = self.clean_number(match[1])
                else:
                    expense_value = self.clean_number(match)
                if expense_value is not None:
                    potential_selling_expenses.append(expense_value)

        if potential_selling_expenses:
            # 选择最合理的值（中位数）
            income_data['operating_expense_selling_expenses'] = sorted(potential_selling_expenses)[len(potential_selling_expenses) // 2]
            logger.debug(f"提取到销售费用: {income_data['operating_expense_selling_expenses']}")

        # 从文本中提取管理费用
        administrative_expenses_patterns = [
            r'管理费用\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'管理费用\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'管理费用[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的管理费用
            r'管理费用\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'管理费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'管理费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'管理费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'管理费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'管理费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'管理费用\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'管理费用[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'管理费用\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_administrative_expenses = []
        for pattern in administrative_expenses_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    expense_value = self.clean_number(match[1])
                else:
                    expense_value = self.clean_number(match)
                if expense_value is not None:
                    potential_administrative_expenses.append(expense_value)

        if potential_administrative_expenses:
            # 选择最合理的值（中位数）
            income_data['operating_expense_administrative_expenses'] = sorted(potential_administrative_expenses)[len(potential_administrative_expenses) // 2]
            logger.debug(f"提取到管理费用: {income_data['operating_expense_administrative_expenses']}")

        # 从文本中提取财务费用
        financial_expenses_patterns = [
            r'财务费用\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'财务费用\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'财务费用[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的财务费用
            r'财务费用\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'财务费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'财务费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'财务费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'财务费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'财务费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'财务费用\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'财务费用[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'财务费用\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_financial_expenses = []
        for pattern in financial_expenses_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    expense_value = self.clean_number(match[1])
                else:
                    expense_value = self.clean_number(match)
                if expense_value is not None:
                    potential_financial_expenses.append(expense_value)

        if potential_financial_expenses:
            # 选择最合理的值（中位数）
            income_data['operating_expense_financial_expenses'] = sorted(potential_financial_expenses)[len(potential_financial_expenses) // 2]
            logger.debug(f"提取到财务费用: {income_data['operating_expense_financial_expenses']}")

        # 从文本中提取研发费用
        rnd_expenses_patterns = [
            r'研发费用\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'研发费用\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'研发费用[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的研发费用
            r'研发费用\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'研发费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'研发费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'研发费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'研发费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'研发费用[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'研发费用\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'研发费用[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'研发费用\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_rnd_expenses = []
        for pattern in rnd_expenses_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    expense_value = self.clean_number(match[1])
                else:
                    expense_value = self.clean_number(match)
                if expense_value is not None:
                    potential_rnd_expenses.append(expense_value)

        if potential_rnd_expenses:
            # 选择最合理的值（中位数）
            income_data['operating_expense_rnd_expenses'] = sorted(potential_rnd_expenses)[len(potential_rnd_expenses) // 2]
            logger.debug(f"提取到研发费用: {income_data['operating_expense_rnd_expenses']}")

        # 从文本中提取税金及附加
        taxes_patterns = [
            r'税金及附加\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'税金及附加\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'税金及附加[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配更多格式的税金及附加
            r'税金及附加\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'税金及附加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'税金及附加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'税金及附加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'税金及附加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'税金及附加[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'税金及附加\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'税金及附加[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'税金及附加\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_taxes = []
        for pattern in taxes_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    tax_value = self.clean_number(match[1])
                else:
                    tax_value = self.clean_number(match)
                if tax_value is not None:
                    potential_taxes.append(tax_value)

        if potential_taxes:
            # 选择最合理的值（中位数）
            income_data['operating_expense_taxes_and_surcharges'] = sorted(potential_taxes)[len(potential_taxes) // 2]
            logger.debug(f"提取到税金及附加: {income_data['operating_expense_taxes_and_surcharges']}")

        # 从文本中提取营业利润
        operating_profit_patterns = [
            # 匹配带单位的营业利润
            r'营业利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'营业利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'营业利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'营业利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'营业利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'营业利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'营业利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'营业利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'营业利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'营业利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'营业利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'营业利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'营业利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'营业利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'营业利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'营业利润\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'营业利润\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'营业利润\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'营业利润[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}'
        ]
        potential_operating_profit = []
        # 存储每个匹配的单位信息
        unit_info = []
        for pattern in operating_profit_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"营业利润模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            # 提取单位信息
            unit = '万元'  # 默认单位
            if '亿元' in pattern or '亿' in pattern:
                unit = '亿元'
            elif '元' in pattern:
                unit = '元'
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    profit_value = self.clean_number(match[1])
                else:
                    profit_value = self.clean_number(match)
                if profit_value is not None:
                    potential_operating_profit.append(profit_value)
                    unit_info.append(unit)

        if potential_operating_profit:
            # 选择最合理的值（中位数）
            operating_profit_value = sorted(potential_operating_profit)[len(potential_operating_profit) // 2]
            # 获取对应的值的单位
            selected_unit = unit_info[len(potential_operating_profit) // 2] if unit_info else '万元'
            
            # 根据单位转换为万元
            if selected_unit == '亿元':
                operating_profit_value = operating_profit_value * 10000
                logger.info(f"营业利润单位转换（亿元→万元）: {operating_profit_value}")
            elif selected_unit == '元':
                operating_profit_value = operating_profit_value / 10000
                logger.info(f"营业利润单位转换（元→万元）: {operating_profit_value}")
            
            # 额外的单位检查逻辑
            if operating_profit_value < 0.1 and selected_unit != '亿元':
                # 如果值很小，可能是亿元单位
                operating_profit_value = operating_profit_value * 10000
                logger.info(f"营业利润单位转换（可能是亿元→万元）: {operating_profit_value}")
            elif operating_profit_value > 1000000 and selected_unit != '元':
                # 如果值很大，可能是元单位
                operating_profit_value = operating_profit_value / 10000
                logger.info(f"营业利润单位转换（可能是元→万元）: {operating_profit_value}")
            
            income_data['operating_profit'] = operating_profit_value
            logger.debug(f"提取到营业利润: {income_data['operating_profit']}")

        # 从文本中提取利润总额
        total_profit_patterns = [
            # 匹配带单位的利润总额
            r'利润总额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'利润总额\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'利润总额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'利润总额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'利润总额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'利润总额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'利润总额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'利润总额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'利润总额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'利润总额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'利润总额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'利润总额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'利润总额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'利润总额[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'利润总额.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'利润总额\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'利润总额\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'利润总额\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'利润总额[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}'
        ]
        potential_total_profit = []
        # 存储每个匹配的单位信息
        total_profit_unit_info = []
        for pattern in total_profit_patterns:
            matches = re.findall(pattern, text_content)
            if matches:
                logger.debug(f"利润总额模式匹配成功: {pattern}，找到 {len(matches)} 个匹配")
            # 提取单位信息
            unit = '万元'  # 默认单位
            if '亿元' in pattern or '亿' in pattern:
                unit = '亿元'
            elif '元' in pattern:
                unit = '元'
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    profit_value = self.clean_number(match[1])
                else:
                    profit_value = self.clean_number(match)
                if profit_value is not None:
                    potential_total_profit.append(profit_value)
                    total_profit_unit_info.append(unit)

        if potential_total_profit:
            # 选择最合理的值（中位数）
            total_profit_value = sorted(potential_total_profit)[len(potential_total_profit) // 2]
            # 获取对应的值的单位
            selected_unit = total_profit_unit_info[len(potential_total_profit) // 2] if total_profit_unit_info else '万元'
            
            # 根据单位转换为万元
            if selected_unit == '亿元':
                total_profit_value = total_profit_value * 10000
                logger.info(f"利润总额单位转换（亿元→万元）: {total_profit_value}")
            elif selected_unit == '元':
                total_profit_value = total_profit_value / 10000
                logger.info(f"利润总额单位转换（元→万元）: {total_profit_value}")
            
            # 额外的单位检查逻辑
            if total_profit_value < 0.1 and selected_unit != '亿元':
                # 如果值很小，可能是亿元单位
                total_profit_value = total_profit_value * 10000
                logger.info(f"利润总额单位转换（可能是亿元→万元）: {total_profit_value}")
            elif total_profit_value > 1000000 and selected_unit != '元':
                # 如果值很大，可能是元单位
                total_profit_value = total_profit_value / 10000
                logger.info(f"利润总额单位转换（可能是元→万元）: {total_profit_value}")
            
            income_data['total_profit'] = total_profit_value
            logger.debug(f"提取到利润总额: {income_data['total_profit']}")

        # 从文本中提取资产减值损失
        impairment_loss_patterns = [
            r'资产减值损失\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'资产减值损失\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'资产减值损失[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式
            r'资产减值损失\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'资产减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            # 新增模式：匹配更多格式的资产减值损失
            r'资产减值损失\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'资产减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'资产减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'资产减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            r'资产减值损失\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'资产减值损失[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            r'资产减值损失\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_impairment_loss = []
        for pattern in impairment_loss_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    loss_value = self.clean_number(match[1])
                else:
                    loss_value = self.clean_number(match)
                if loss_value is not None:
                    potential_impairment_loss.append(loss_value)

        if potential_impairment_loss:
            # 选择最合理的值（中位数）
            income_data['asset_impairment_loss'] = sorted(potential_impairment_loss)[len(potential_impairment_loss) // 2]
            logger.debug(f"提取到资产减值损失: {income_data['asset_impairment_loss']}")

        # 从文本中提取信用减值损失
        credit_impairment_loss_patterns = [
            # 匹配带单位的信用减值损失
            r'信用减值损失\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'信用减值损失\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'信用减值损失\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'信用减值损失\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'信用减值损失\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'信用减值损失\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'信用减值损失\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'信用减值损失\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'信用减值损失\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'信用减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'信用减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'信用减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'信用减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'信用减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'信用减值损失[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'信用减值损失.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'信用减值损失\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'信用减值损失\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'信用减值损失\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'信用减值损失[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            # 新增：匹配更多可能的表述
            r'信用减值损失\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_credit_impairment_loss = []
        for pattern in credit_impairment_loss_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    loss_value = self.clean_number(match[1])
                else:
                    loss_value = self.clean_number(match)
                if loss_value is not None:
                    potential_credit_impairment_loss.append(loss_value)

        if potential_credit_impairment_loss:
            # 选择最合理的值（中位数）
            income_data['credit_impairment_loss'] = sorted(potential_credit_impairment_loss)[len(potential_credit_impairment_loss) // 2]
            logger.debug(f"提取到信用减值损失: {income_data['credit_impairment_loss']}")

        # 从文本中提取其他收益
        other_income_patterns = [
            # 匹配带单位的其他收益
            r'其他收益\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*[（\(]元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*[（\(]亿元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*[（\(]万[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*[（\(]亿[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配括号格式
            r'其他收益\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*\(元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*\(亿元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*\(万\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'其他收益\s*\(亿\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带冒号的格式
            r'其他收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'其他收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'其他收益\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配带单位后缀的格式
            r'其他收益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*万元)',
            r'其他收益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'其他收益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*亿元)',
            # 匹配表格中的数据
            r'其他收益[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万元',
            r'其他收益[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?元',
            r'其他收益[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿元',
            r'其他收益[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?万',
            r'其他收益[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?亿',
            # 匹配表格中的年份数据
            r'其他收益[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            # 匹配广泛模式
            r'其他收益.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配带空格的格式
            r'其他收益\s+[:：]\s+([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配简化格式
            r'其他收益\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配冒号格式
            r'其他收益\s*[：:][\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 匹配表格中的数据行（更广泛）
            r'其他收益[\s\S]{0,200}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,100}?\d{4}',
            # 新增：匹配更多可能的表述
            r'其他收益\s*[：:][\s\S]{0,50}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_other_income = []
        for pattern in other_income_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    income_value = self.clean_number(match[1])
                else:
                    income_value = self.clean_number(match)
                if income_value is not None:
                    potential_other_income.append(income_value)

        if potential_other_income:
            # 选择最合理的值（中位数）
            income_data['other_income'] = sorted(potential_other_income)[len(potential_other_income) // 2]
            logger.debug(f"提取到其他收益: {income_data['other_income']}")

        # 从文本中提取营业利润
        operating_profit_patterns = [
            r'营业利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'营业利润\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'营业利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'营业利润[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_operating_profit = []
        for pattern in operating_profit_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    profit_value = self.clean_number(match[1])
                else:
                    profit_value = self.clean_number(match)
                if profit_value is not None:
                    potential_operating_profit.append(profit_value)

        if potential_operating_profit:
            # 选择最合理的值（中位数）
            income_data['operating_profit'] = sorted(potential_operating_profit)[len(potential_operating_profit) // 2]
            logger.debug(f"提取到营业利润: {income_data['operating_profit']}")

        # 从文本中提取利润总额
        total_profit_patterns = [
            r'利润总额\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'利润总额\((万\w+)\)\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'利润总额\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?!\s*元)',
            r'利润总额[\s\S]*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
        ]
        potential_total_profit = []
        for pattern in total_profit_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # 处理不同的匹配组结构
                if isinstance(match, tuple):
                    profit_value = self.clean_number(match[1])
                else:
                    profit_value = self.clean_number(match)
                if profit_value is not None:
                    potential_total_profit.append(profit_value)

        if potential_total_profit:
            # 选择最合理的值（中位数）
            income_data['total_profit'] = sorted(potential_total_profit)[len(potential_total_profit) // 2]
            logger.debug(f"提取到利润总额: {income_data['total_profit']}")

        # 从文本中提取净利润
        net_profit_patterns = [
            r'归属于上市公司股东的净利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者的净利润\s*[（\(]万元[）\)]?\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配不同格式的净利润
            r'归属于上市公司股东的净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者的净利润\s*\(万元\)\s*[:：]?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于上市公司股东的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归母净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            r'归属于母公司所有者的净利润\s*[:：]\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            # 新增模式：匹配表格中的净利润数据
            r'归属于上市公司股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}',
            r'净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s\S]{0,50}?\d{4}'
        ]
        potential_net_profit = []
        for pattern in net_profit_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                net_profit = self.clean_number(match)
                if net_profit is not None:
                    potential_net_profit.append(net_profit)

        if potential_net_profit:
            # 选择最合理的值（中位数）
            income_data['net_profit'] = sorted(potential_net_profit)[len(potential_net_profit) // 2]
            # 首先尝试使用多个指标名称变体计算同比增长率
            income_data['net_profit_yoy_growth'] = self.calculate_growth(
                text_content, 
                ['净利润', '归属于上市公司股东的净利润', '归母净利润', '归属于母公司所有者的净利润'], 
                '同比'
            )
            
            # 如果没有找到同比增长率，尝试从比较财务报表中计算
            if income_data['net_profit_yoy_growth'] is None:
                income_data['net_profit_yoy_growth'] = self.calculate_balance_sheet_growth(
                    text_content, 
                    '归属于上市公司股东的净利润'
                )
                if income_data['net_profit_yoy_growth'] is not None:
                    logger.info(f"从比较财务报表计算得到净利润同比增长率: {income_data['net_profit_yoy_growth']}")
            
            # 如果仍然没有找到，尝试从文本中直接提取
            if income_data['net_profit_yoy_growth'] is None:
                # 直接搜索净利润同比增长率的各种表述
                yoy_patterns = [
                    r'净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'同比.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'净利润.*?增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'归属于上市公司股东的净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'归母净利润.*?同比.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%',
                    r'净利润.*?同比增长率.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'同比增长率.*?净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'净利润.*?同比增长.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                    r'净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%.*?同比',
                    r'净利润.*?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%.*?增长',
                    # 匹配表格中的数值
                    r'净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%[\s\S]{0,50}?同比',
                    r'归属于上市公司股东的净利润[\s\S]{0,100}?([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)%[\s\S]{0,50}?同比'
                ]
                for pattern in yoy_patterns:
                    matches = re.findall(pattern, text_content)
                    if matches:
                        for match in matches:
                            growth = self.clean_number(match)
                            if growth is not None:
                                if growth > 100 or growth < -100:
                                    income_data['net_profit_yoy_growth'] = growth / 100
                                else:
                                    income_data['net_profit_yoy_growth'] = growth
                                logger.info(f"直接从文本提取到净利润同比增长率: {income_data['net_profit_yoy_growth']}")
                                break
                    if income_data['net_profit_yoy_growth'] is not None:
                        break
            logger.debug(f"提取到净利润: {income_data['net_profit']}")

        # 计算总营业费用
        operating_expenses = []
        expense_fields = [
            'operating_expense_cost_of_sales',
            'operating_expense_selling_expenses',
            'operating_expense_administrative_expenses',
            'operating_expense_financial_expenses',
            'operating_expense_rnd_expenses',
            'operating_expense_taxes_and_surcharges'
        ]
        for field in expense_fields:
            if field in income_data:
                operating_expenses.append(income_data[field])
        
        if operating_expenses:
            income_data['total_operating_expenses'] = sum(operating_expenses)
            logger.debug(f"计算得到总营业费用: {income_data['total_operating_expenses']}")

        return income_data

    def extract_financial_data(self, text_content, stock_code, stock_abbr, report_period, report_year):
        """
        从财报文本中提取结构化财务数据

        :param text_content: 财报文本内容
        :param stock_code: 股票代码
        :param stock_abbr: 股票简称
        :param report_period: 报告期 (Q1, HY, Q3, FY)
        :param report_year: 报告年份
        :return: 提取的财务数据字典
        """
        # 初始化数据字典
        data = {
            'stock_code': stock_code,
            'stock_abbr': stock_abbr,
            'report_period': report_period,
            'report_year': report_year
        }
        cash_flow_data = {}
        balance_data = {}
        income_data = {}
        core_data = {}

        try:
            # 从文本中提取现金流量表数据（先提取，因为核心业绩指标可能需要使用）
            cash_flow_data = self.extract_cash_flow(text_content) or {}
            if cash_flow_data:
                data.update(cash_flow_data)
                logger.debug(f"提取到现金流量表数据: {cash_flow_data}")
            else:
                logger.warning("未提取到现金流量表数据")
        except Exception as e:
            logger.error(f"提取现金流量表数据时出错: {e}")

        try:
            # 从文本中提取资产负债表数据
            balance_data = self.extract_balance_sheet(text_content) or {}
            if balance_data:
                data.update(balance_data)
                logger.debug(f"提取到资产负债表数据: {balance_data}")
            else:
                logger.warning("未提取到资产负债表数据")
        except Exception as e:
            logger.error(f"提取资产负债表数据时出错: {e}")

        try:
            # 从文本中提取利润表数据
            income_data = self.extract_income_statement(text_content) or {}
            if income_data:
                data.update(income_data)
                logger.debug(f"提取到利润表数据: {income_data}")
            else:
                logger.warning("未提取到利润表数据")
        except Exception as e:
            logger.error(f"提取利润表数据时出错: {e}")

        try:
            # 从文本中提取核心业绩指标（最后更新，确保合理的值不会被覆盖）
            # 传递已提取的数据，以便在计算依赖字段时使用
            core_data = self.extract_core_performance(text_content, data) or {}
            if core_data:
                data.update(core_data)
                logger.debug(f"提取到核心业绩指标数据: {core_data}")
        except Exception as e:
            logger.error(f"提取核心业绩指标数据时出错: {e}")

        unit_adjustments = self._normalize_core_field_units(
            balance_data,
            income_data,
            core_data,
            cash_flow_data,
        )
        if unit_adjustments:
            data.update(balance_data)
            data.update(income_data)
            data.update(cash_flow_data)
            data.update(core_data)

        consistency_result = self.validate_cross_table_consistency(
            data,
            balance_data=balance_data,
            income_data=income_data,
            core_data=core_data,
        )
        data.update(consistency_result)
        if unit_adjustments:
            data['_unit_adjustments'] = unit_adjustments

        # 统计提取成功的字段数量
        non_none_count = self.count_non_null_values(data)
        total_count = len(self._iter_financial_items(data))
        logger.info(f"数据提取完成，成功提取 {non_none_count}/{total_count} 个字段")

        logger.debug(f"提取的完整数据: {data}")
        return data

    def _iter_financial_items(self, data_dict):
        return [
            (key, value)
            for key, value in data_dict.items()
            if not key.startswith('_')
        ]

    def count_non_null_values(self, data_dict):
        """
        计算字典中非空值的数量

        :param data_dict: 数据字典
        :return: 非空值的数量
        """
        return sum(1 for _, value in self._iter_financial_items(data_dict) if value is not None)

    def _build_consistency_check(self, check_name, left_value, right_value, *,
                                 left_label, right_label, warn_ratio,
                                 low_quality_ratio, absolute_tolerance):
        if left_value is None or right_value is None:
            return {
                'status': 'skipped',
                'left_label': left_label,
                'right_label': right_label,
                'left_value': left_value,
                'right_value': right_value,
                'difference': None,
                'relative_difference': None,
                'warn_ratio': warn_ratio,
                'low_quality_ratio': low_quality_ratio,
                'reason': 'missing_value',
            }

        difference = left_value - right_value
        scale = max(abs(left_value), abs(right_value), absolute_tolerance)
        relative_difference = abs(difference) / scale
        status = 'pass'
        if relative_difference >= low_quality_ratio:
            status = 'low_quality'
        elif relative_difference >= warn_ratio:
            status = 'warning'

        return {
            'status': status,
            'left_label': left_label,
            'right_label': right_label,
            'left_value': left_value,
            'right_value': right_value,
            'difference': difference,
            'relative_difference': relative_difference,
            'warn_ratio': warn_ratio,
            'low_quality_ratio': low_quality_ratio,
            'reason': None,
        }

    def _log_consistency_result(self, identity_text, check_name, result):
        if result['status'] == 'warning':
            logger.warning(
                "%s 一致性校验告警 [%s]：%s=%.6f, %s=%.6f, 差异=%.6f, 相对差异=%.4f%%",
                identity_text,
                check_name,
                result['left_label'],
                result['left_value'],
                result['right_label'],
                result['right_value'],
                result['difference'],
                result['relative_difference'] * 100,
            )
        elif result['status'] == 'low_quality':
            logger.warning(
                "%s 一致性校验异常 [%s]：%s=%.6f, %s=%.6f, 差异=%.6f, 相对差异=%.4f%%，标记为低质量",
                identity_text,
                check_name,
                result['left_label'],
                result['left_value'],
                result['right_label'],
                result['right_value'],
                result['difference'],
                result['relative_difference'] * 100,
            )

    def validate_cross_table_consistency(self, data, *, balance_data=None, income_data=None, core_data=None):
        balance_data = balance_data or {}
        income_data = income_data or {}
        core_data = core_data or {}
        identity_text = (
            f"{data.get('stock_code', 'UNKNOWN')}/"
            f"{data.get('stock_abbr', 'UNKNOWN')}/"
            f"{data.get('report_year', 'UNKNOWN')}/"
            f"{data.get('report_period', 'UNKNOWN')}"
        )
        checks = {}
        quality_flags = []

        balance_rule = self.CONSISTENCY_RULES['balance_equation']
        balance_check = self._build_consistency_check(
            'balance_equation',
            data.get('asset_total_assets'),
            (
                (data.get('liability_total_liabilities') or 0)
                + (data.get('equity_total_equity') or 0)
                if data.get('liability_total_liabilities') is not None
                and data.get('equity_total_equity') is not None
                else None
            ),
            left_label='asset_total_assets',
            right_label='liability_total_liabilities + equity_total_equity',
            **balance_rule,
        )
        checks['balance_equation'] = balance_check

        net_profit_rule = self.CONSISTENCY_RULES['net_profit_cross_table']
        net_profit_check = self._build_consistency_check(
            'net_profit_cross_table',
            income_data.get('net_profit'),
            core_data.get('net_profit_10k_yuan'),
            left_label='income_sheet.net_profit',
            right_label='core_performance_indicators_sheet.net_profit_10k_yuan',
            **net_profit_rule,
        )
        checks['net_profit_cross_table'] = net_profit_check

        revenue_rule = self.CONSISTENCY_RULES['revenue_cross_table']
        revenue_check = self._build_consistency_check(
            'revenue_cross_table',
            income_data.get('total_operating_revenue'),
            core_data.get('total_operating_revenue'),
            left_label='income_sheet.total_operating_revenue',
            right_label='core_performance_indicators_sheet.total_operating_revenue',
            **revenue_rule,
        )
        checks['revenue_cross_table'] = revenue_check

        for check_name, result in checks.items():
            self._log_consistency_result(identity_text, check_name, result)
            if result['status'] == 'warning':
                quality_flags.append(f'{check_name}:warning')
            elif result['status'] == 'low_quality':
                quality_flags.append(f'{check_name}:low_quality')

        record_quality = 'low_quality' if any(
            result['status'] == 'low_quality' for result in checks.values()
        ) else 'normal'

        logger.info(
            "%s 一致性校验完成：record_quality=%s, flags=%s",
            identity_text,
            record_quality,
            quality_flags if quality_flags else ['pass'],
        )

        return {
            '_consistency_checks': checks,
            '_record_quality': record_quality,
            '_quality_flags': quality_flags,
        }

    def _build_table_scoped_record(self, data, table_name):
        fields = self.TABLE_FIELDS.get(table_name, [])
        return {
            key: data.get(key)
            for key in fields
            if key in data
        }

    def _evaluate_numeric_field_quality(self, key, value):
        if value is None or not isinstance(value, (int, float)):
            return {'key': key, 'status': 'skip', 'sanitized': value}
        sanitized = self._sanitize_db_value(value, key)
        if sanitized is None:
            return {'key': key, 'status': 'invalid', 'sanitized': None}
        return {'key': key, 'status': 'valid', 'sanitized': sanitized}

    def _derive_record_consistency_checks(self, data):
        existing_checks = data.get('_consistency_checks')
        if existing_checks:
            return existing_checks
        consistency_meta = self.validate_cross_table_consistency(
            data,
            balance_data=self._build_table_scoped_record(data, 'balance_sheet'),
            income_data=self._build_table_scoped_record(data, 'income_sheet'),
            core_data=self._build_table_scoped_record(data, 'core_performance_indicators_sheet'),
        )
        return consistency_meta['_consistency_checks']

    def _normalize_amount_to_wanyuan(self, field_name, value):
        if value is None or not isinstance(value, (int, float)):
            return value, None
        rules = self.UNIT_NORMALIZATION_RULES.get(field_name)
        if not rules:
            return value, None
        adjusted_value = float(value)
        if abs(adjusted_value) >= rules['yuan_threshold']:
            return adjusted_value / 10000.0, 'yuan_to_wanyuan'
        return adjusted_value, None

    def _normalize_container_amount_fields(self, container, source_name, adjustments):
        for field_name in self.UNIT_NORMALIZATION_RULES:
            if field_name not in container:
                continue
            original_value = container.get(field_name)
            normalized_value, reason = self._normalize_amount_to_wanyuan(field_name, original_value)
            if reason:
                container[field_name] = normalized_value
                adjustments.append(
                    f"{source_name}.{field_name}:{reason}:{original_value}->{normalized_value}"
                )

    def _reconcile_scaled_pair(self, primary_container, primary_field, secondary_container, secondary_field, *, adjustments, source_pair_name):
        primary_value = primary_container.get(primary_field)
        secondary_value = secondary_container.get(secondary_field)
        if not isinstance(primary_value, (int, float)) or not isinstance(secondary_value, (int, float)):
            return
        if primary_value == 0 or secondary_value == 0:
            return
        ratio = abs(primary_value) / max(abs(secondary_value), 1e-6)
        if 5000 <= ratio <= 20000:
            old_value = primary_value
            primary_container[primary_field] = primary_value / 10000.0
            adjustments.append(
                f"{source_pair_name}:{primary_field}:divide_10000:{old_value}->{primary_container[primary_field]}"
            )
        elif 5000 <= (1 / ratio) <= 20000:
            old_value = primary_value
            primary_container[primary_field] = primary_value * 10000.0
            adjustments.append(
                f"{source_pair_name}:{primary_field}:multiply_10000:{old_value}->{primary_container[primary_field]}"
            )

    def _normalize_core_field_units(self, balance_data, income_data, core_data, cash_flow_data):
        adjustments = []
        for container, source_name in (
            (balance_data, 'balance_sheet'),
            (income_data, 'income_sheet'),
            (core_data, 'core_performance_indicators_sheet'),
            (cash_flow_data, 'cash_flow_sheet'),
        ):
            self._normalize_container_amount_fields(container, source_name, adjustments)

        self._reconcile_scaled_pair(
            core_data,
            'total_operating_revenue',
            income_data,
            'total_operating_revenue',
            adjustments=adjustments,
            source_pair_name='revenue_cross_table',
        )
        self._reconcile_scaled_pair(
            core_data,
            'net_profit_10k_yuan',
            income_data,
            'net_profit',
            adjustments=adjustments,
            source_pair_name='net_profit_cross_table',
        )

        if adjustments:
            logger.info("核心字段单位统一完成：%s", "; ".join(adjustments))
        return adjustments

    def _evaluate_core_amount_anomalies(self, data):
        anomalies = []
        for field_name in self.CORE_DUPLICATE_PRIORITY_FIELDS:
            value = data.get(field_name)
            rules = self.UNIT_NORMALIZATION_RULES.get(field_name)
            if value is None or not isinstance(value, (int, float)) or not rules:
                continue
            if abs(value) > rules['max_abs_wanyuan']:
                anomalies.append(f'{field_name}:magnitude')

        revenue = data.get('total_operating_revenue')
        if isinstance(revenue, (int, float)) and revenue > 0:
            for field_name in ('net_profit', 'net_profit_10k_yuan', 'total_profit'):
                value = data.get(field_name)
                if isinstance(value, (int, float)) and abs(value) > revenue * 5:
                    anomalies.append(f'{field_name}:profit_vs_revenue')
        return anomalies

    def calculate_record_quality_score(self, data):
        rules = self.QUALITY_SCORE_RULES
        non_null_count = self.count_non_null_values(data)
        score = non_null_count * rules['non_null_point']
        detail = {
            'non_null_count': non_null_count,
            'non_null_score': non_null_count * rules['non_null_point'],
            'key_field_score': 0.0,
            'valid_numeric_count': 0,
            'valid_numeric_score': 0.0,
            'invalid_numeric_fields': [],
            'invalid_numeric_penalty': 0.0,
            'core_valid_count': 0,
            'core_field_bonus': 0.0,
            'core_invalid_fields': [],
            'core_invalid_penalty': 0.0,
            'core_anomaly_fields': [],
            'core_anomaly_penalty': 0.0,
            'consistency_penalty': 0.0,
            'quality_flag_penalty': 0.0,
            'record_quality_penalty': 0.0,
            'consistency_statuses': {},
        }

        for field, weight in self.QUALITY_SCORE_KEY_FIELD_WEIGHTS.items():
            if data.get(field) is not None:
                score += weight
                detail['key_field_score'] += weight

        invalid_numeric_fields = []
        valid_numeric_count = 0
        for key, value in self._iter_financial_items(data):
            quality = self._evaluate_numeric_field_quality(key, value)
            if quality['status'] == 'valid':
                valid_numeric_count += 1
            elif quality['status'] == 'invalid':
                invalid_numeric_fields.append(key)

        valid_numeric_score = valid_numeric_count * rules['valid_numeric_point']
        invalid_numeric_penalty = len(invalid_numeric_fields) * rules['invalid_numeric_penalty']
        score += valid_numeric_score
        score -= invalid_numeric_penalty
        detail['valid_numeric_count'] = valid_numeric_count
        detail['valid_numeric_score'] = valid_numeric_score
        detail['invalid_numeric_fields'] = invalid_numeric_fields
        detail['invalid_numeric_penalty'] = invalid_numeric_penalty

        core_invalid_fields = []
        core_valid_count = 0
        for field_name in self.CORE_DUPLICATE_PRIORITY_FIELDS:
            field_value = data.get(field_name)
            if field_value is None:
                continue
            quality = self._evaluate_numeric_field_quality(field_name, field_value)
            if quality['status'] == 'valid':
                core_valid_count += 1
            else:
                core_invalid_fields.append(field_name)

        core_field_bonus = core_valid_count * rules['core_field_point']
        core_invalid_penalty = len(core_invalid_fields) * rules['core_invalid_penalty']
        score += core_field_bonus
        score -= core_invalid_penalty
        detail['core_valid_count'] = core_valid_count
        detail['core_field_bonus'] = core_field_bonus
        detail['core_invalid_fields'] = core_invalid_fields
        detail['core_invalid_penalty'] = core_invalid_penalty

        core_anomaly_fields = self._evaluate_core_amount_anomalies(data)
        core_anomaly_penalty = len(core_anomaly_fields) * rules['core_anomaly_penalty']
        score -= core_anomaly_penalty
        detail['core_anomaly_fields'] = core_anomaly_fields
        detail['core_anomaly_penalty'] = core_anomaly_penalty

        consistency_checks = self._derive_record_consistency_checks(data)
        consistency_penalty = 0.0
        for check_name, result in consistency_checks.items():
            detail['consistency_statuses'][check_name] = result['status']
            if result['status'] == 'warning':
                consistency_penalty += rules['warning_penalty']
            elif result['status'] == 'low_quality':
                consistency_penalty += rules['low_quality_penalty']
        score -= consistency_penalty
        detail['consistency_penalty'] = consistency_penalty

        quality_flags = data.get('_quality_flags', [])
        quality_flag_penalty = 0.0
        for flag in quality_flags:
            if 'low_quality' in flag:
                quality_flag_penalty += rules['quality_flag_low_quality_penalty']
            elif 'warning' in flag:
                quality_flag_penalty += rules['quality_flag_warning_penalty']
        score -= quality_flag_penalty
        detail['quality_flag_penalty'] = quality_flag_penalty

        record_quality = data.get('_record_quality')
        if record_quality is None and any(
            status == 'low_quality' for status in detail['consistency_statuses'].values()
        ):
            record_quality = 'low_quality'
        if record_quality == 'low_quality':
            score -= rules['record_low_quality_penalty']
            detail['record_quality_penalty'] = rules['record_low_quality_penalty']

        detail['record_quality'] = record_quality or 'normal'
        detail['total_score'] = round(score, 4)
        return detail

    def _log_duplicate_quality_comparison(self, identity_text, current_quality, existing_quality):
        logger.info(
            "%s 重复记录质量比较：current_score=%.4f (non_null=%s, key_score=%.1f, "
            "valid_numeric=%s, invalid_penalty=%.1f, consistency_penalty=%.1f, flag_penalty=%.1f, record=%s); "
            "existing_score=%.4f (non_null=%s, key_score=%.1f, valid_numeric=%s, invalid_penalty=%.1f, "
            "consistency_penalty=%.1f, flag_penalty=%.1f, record=%s)",
            identity_text,
            current_quality['total_score'],
            current_quality['non_null_count'],
            current_quality['key_field_score'],
            current_quality['valid_numeric_count'],
            current_quality['invalid_numeric_penalty'],
            current_quality['consistency_penalty'],
            current_quality['quality_flag_penalty'] + current_quality['record_quality_penalty'],
            current_quality['record_quality'],
            existing_quality['total_score'],
            existing_quality['non_null_count'],
            existing_quality['key_field_score'],
            existing_quality['valid_numeric_count'],
            existing_quality['invalid_numeric_penalty'],
            existing_quality['consistency_penalty'],
            existing_quality['quality_flag_penalty'] + existing_quality['record_quality_penalty'],
            existing_quality['record_quality'],
        )

    def _build_identity_params(self, stock_code, stock_abbr, report_period, report_year):
        return (stock_code, stock_abbr, report_period, report_year)

    def _build_duplicate_identity_params(self, stock_code, report_period, report_year):
        return (stock_code, report_period, report_year)

    def _fetch_existing_table_records(self, table_name, duplicate_identity_params):
        query = f"""
            SELECT * FROM {table_name}
            WHERE stock_code = %s AND report_period = %s AND report_year = %s
        """
        self.cursor.execute(query, duplicate_identity_params)
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
        return [
            {
                key: value
                for key, value in zip(columns, row)
                if key != 'serial_number' and value is not None
            }
            for row in rows
        ]

    def _collect_existing_duplicate_candidates(self, stock_code, report_period, report_year):
        duplicate_identity_params = self._build_duplicate_identity_params(stock_code, report_period, report_year)
        grouped_candidates = {}
        for table_name in self.TABLE_FIELDS:
            table_records = self._fetch_existing_table_records(table_name, duplicate_identity_params)
            for record in table_records:
                candidate_key = record.get('stock_abbr') or f"__EMPTY__{len(grouped_candidates)}"
                candidate = grouped_candidates.setdefault(candidate_key, {
                    'stock_code': stock_code,
                    'report_period': report_period,
                    'report_year': report_year,
                    'stock_abbr': record.get('stock_abbr'),
                })
                for key, value in record.items():
                    if key != 'serial_number' and value is not None:
                        candidate[key] = value
        return list(grouped_candidates.values())

    def _select_best_existing_duplicate_candidate(self, candidates):
        best_candidate = None
        best_quality = None
        for candidate in candidates:
            candidate_quality = self.calculate_record_quality_score(candidate)
            if best_quality is None or candidate_quality['total_score'] > best_quality['total_score']:
                best_candidate = candidate
                best_quality = candidate_quality
        return best_candidate, best_quality

    def _delete_records_by_duplicate_identity(self, duplicate_identity_params):
        for table_name in self.TABLE_FIELDS:
            query = f"""
                DELETE FROM {table_name}
                WHERE stock_code = %s AND report_period = %s AND report_year = %s
            """
            self.cursor.execute(query, duplicate_identity_params)

    def _sanitize_db_value(self, value, key=None):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            if math.isnan(value) or math.isinf(value):
                return None
            if key and 'growth' in key.lower():
                if value > 100000:
                    logger.warning(f"{key} 值异常：{value}，设置为 NULL")
                    return None
                if value < -100:
                    logger.warning(f"{key} 值异常：{value}，设置为 NULL")
                    return None
            if key and any(token in key.lower() for token in ['amount', 'cash', 'flow', 'profit', 'revenue']):
                if abs(value) > 1e14:
                    logger.warning(f"{key} 金额异常：{value}，设置为 NULL")
                    return None
            rules = self.UNIT_NORMALIZATION_RULES.get(key)
            if rules and abs(value) > rules['max_abs_wanyuan']:
                logger.warning(f"{key} 金额可能存在单位异常：{value}，设置为 NULL")
                return None
            return value
        return value

    def _round_db_value(self, value, key):
        if value is None or not isinstance(value, (int, float)):
            return value
        if key in self.IDENTITY_FIELDS:
            return value
        if key.endswith('_growth') or 'ratio' in key or 'margin' in key or 'roe' in key or key == 'eps':
            return round(float(value), 4)
        return round(float(value), 2)

    def _prepare_table_values(self, data, fields):
        values = []
        for key in fields:
            value = self._sanitize_db_value(data.get(key), key)
            value = self._round_db_value(value, key)
            values.append(value)
        return values

    def _insert_table_record(self, table_name, fields, values):
        placeholders = ', '.join(['%s'] * len(fields))
        query = f"""
            INSERT INTO {table_name} ({', '.join(fields)})
            VALUES ({placeholders})
        """
        self.cursor.execute(query, values)

    def check_duplicate_records(self, stock_code, stock_abbr, report_period, report_year):
        """
        检查是否存在重复记录

        :param stock_code: 股票代码
        :param stock_abbr: 股票简称
        :param report_period: 报告期
        :param report_year: 报告年份
        :return: 存在的重复记录数据，如果不存在返回 None
        """
        try:
            self.cursor = self.connection.cursor()
            duplicate_candidates = self._collect_existing_duplicate_candidates(stock_code, report_period, report_year)
            if not duplicate_candidates:
                return None
            best_candidate, best_quality = self._select_best_existing_duplicate_candidate(duplicate_candidates)
            if best_candidate is not None:
                best_candidate['_duplicate_candidate_count'] = len(duplicate_candidates)
                best_candidate['_duplicate_best_quality_score'] = best_quality['total_score'] if best_quality else None
                if len(duplicate_candidates) > 1:
                    logger.info(
                        "%s/%s/%s 找到 %s 条同期间候选记录，将以质量分最高者作为现有记录",
                        stock_code,
                        report_year,
                        report_period,
                        len(duplicate_candidates),
                    )
            return best_candidate
        except Exception as e:
            logger.error(f"检查重复记录时出错: {e}")
            return None

    def delete_duplicate_records(self, stock_code, stock_abbr, report_period, report_year):
        """
        删除重复记录

        :param stock_code: 股票代码
        :param stock_abbr: 股票简称
        :param report_period: 报告期
        :param report_year: 报告年份
        """
        try:
            self.cursor = self.connection.cursor()
            duplicate_identity_params = self._build_duplicate_identity_params(stock_code, report_period, report_year)
            self._delete_records_by_duplicate_identity(duplicate_identity_params)

            self.connection.commit()
            logger.info(f"删除重复记录成功: {stock_code}, {stock_abbr}, {report_period}, {report_year}")
        except Exception as e:
            logger.error(f"删除重复记录时出错: {e}")
            self.connection.rollback()

    def insert_data_to_db(self, data):
        """
        将提取的财务数据插入到数据库

        :param data: 提取的财务数据字典
        """
        try:
            self.cursor = self.connection.cursor()
            for field in self.IDENTITY_FIELDS:
                if field not in data or data[field] is None:
                    logger.error(f"缺少必填字段: {field}")
                    return

            stock_code, stock_abbr, report_period, report_year = (
                data['stock_code'],
                data['stock_abbr'],
                data['report_period'],
                data['report_year'],
            )
            existing_data = self.check_duplicate_records(stock_code, stock_abbr, report_period, report_year)
            identity_text = f"{stock_code}/{stock_abbr}/{report_year}/{report_period}"

            if existing_data:
                duplicate_candidate_count = existing_data.get('_duplicate_candidate_count', 1)
                if duplicate_candidate_count > 1:
                    logger.info(
                        "%s 检测到 %s 条同公司同期间记录候选，按质量分进行保留决策",
                        identity_text,
                        duplicate_candidate_count,
                    )
                current_quality = self.calculate_record_quality_score(data)
                existing_quality = self.calculate_record_quality_score(existing_data)
                self._log_duplicate_quality_comparison(identity_text, current_quality, existing_quality)
                if current_quality.get('core_invalid_fields') or current_quality.get('core_anomaly_fields'):
                    logger.warning(
                        "%s 当前记录关键字段降权：core_invalid=%s, core_anomalies=%s",
                        identity_text,
                        current_quality.get('core_invalid_fields'),
                        current_quality.get('core_anomaly_fields'),
                    )
                if existing_quality.get('core_invalid_fields') or existing_quality.get('core_anomaly_fields'):
                    logger.warning(
                        "%s 现有记录关键字段降权：core_invalid=%s, core_anomalies=%s",
                        identity_text,
                        existing_quality.get('core_invalid_fields'),
                        existing_quality.get('core_anomaly_fields'),
                    )

                if current_quality['total_score'] > existing_quality['total_score']:
                    logger.info(
                        "%s 当前记录质量分更高（%.4f > %.4f），将替换现有记录",
                        identity_text,
                        current_quality['total_score'],
                        existing_quality['total_score'],
                    )
                    self.delete_duplicate_records(stock_code, stock_abbr, report_period, report_year)
                else:
                    logger.info(
                        "%s 现有记录质量分更高或相同（%.4f >= %.4f），保持现有记录",
                        identity_text,
                        existing_quality['total_score'],
                        current_quality['total_score'],
                    )
                    return

            for table_name, fields in self.TABLE_FIELDS.items():
                values = self._prepare_table_values(data, fields)
                self._insert_table_record(table_name, fields, values)

            self.connection.commit()
            logger.info("数据插入成功")
            return True
        except Exception as e:
            logger.error(f"插入数据时出错: {e}")
            logger.error(f"出错时的数据: {data}")
            self.connection.rollback()
            # 不抛出异常，允许继续处理其他文件
            logger.warning("数据插入失败，但将继续处理其他文件")
            return False

    def close_connection(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    args = parse_args()

    # 数据库配置
    db_config = {
        'host': args.db_host,
        'instance': args.db_instance,
        'port': args.db_port,
        'user': args.db_user,
        'password': args.db_password,
        'database': args.db_name
    }

    processor = None
    try:
        # 创建处理器实例
        processor = FinancialDataProcessorComplete(db_config)

        # 创建表结构
        processor.create_tables()

        pdf_path = os.path.abspath(args.pdf_path)

        print("=" * 80)
        print("财务数据处理器 - 批量导入 PDF 财报到数据库")
        print("=" * 80)
        print(f"\n使用路径：{pdf_path}")
        print("扫描方式：递归扫描目录中的全部 PDF 文件")

        try:
            pdf_files = collect_pdf_files(pdf_path)
        except (FileNotFoundError, ValueError) as e:
            print(f"错误：{e}")
            sys.exit(1)

        if not pdf_files:
            print(f"错误：在 {pdf_path} 中没有找到 PDF 文件")
            sys.exit(1)

        print(f"\n找到 {len(pdf_files)} 个 PDF 文件，开始处理...")
        print(f"{'=' * 80}\n")

        # 处理所有找到的 PDF 文件
        success_count = 0
        failed_count = 0
        skipped_count = 0

        for idx, pdf_file in enumerate(pdf_files, 1):
            print(f"\n{'=' * 80}")
            print(f"处理进度：[{idx}/{len(pdf_files)}] - {os.path.basename(pdf_file)}")
            print(f"{'=' * 80}")

            try:
                # 提取文本
                text_content = processor.extract_text_from_pdf(pdf_file)
                print(f"文本提取成功，长度: {len(text_content)} 字符")

                # 提取股票信息
                stock_code = processor.extract_stock_code_from_text(text_content)
                if not stock_code:
                    filename = os.path.basename(pdf_file)
                    stock_code_match = re.search(r'(\d{6})', filename)
                    if stock_code_match:
                        stock_code = stock_code_match.group(1)
                        print(f"从文件名中提取到股票代码: {stock_code}")
                    else:
                        print(f"警告：未提取到股票代码，跳过此 PDF: {pdf_file}")
                        skipped_count += 1
                        continue
                else:
                    print(f"从PDF内容中提取到股票代码: {stock_code}")

                raw_stock_abbr = processor.extract_stock_abbr_from_text(text_content)
                if not raw_stock_abbr:
                    # 尝试从文件名中提取股票简称
                    filename = os.path.basename(pdf_file)
                    chinese_name_match = re.search(r'(.+?)[：:].*\.pdf', filename, re.IGNORECASE)
                    if chinese_name_match:
                        raw_stock_abbr = chinese_name_match.group(1)
                        print(f"从文件名中提取到原始股票简称: {raw_stock_abbr}")
                    else:
                        raw_stock_abbr = None
                        print("未提取到原始股票简称，准备尝试附件1简称映射")
                else:
                    print(f"从 PDF 内容中提取到原始股票简称：{raw_stock_abbr}")

                stock_abbr, used_attachment1_mapping = processor.normalize_stock_abbr(
                    raw_stock_abbr,
                    stock_code,
                    processor.company_abbr_mapping,
                )
                if not stock_abbr:
                    stock_abbr = stock_code
                    print(f"标准化后仍为空，使用股票代码作为最终股票简称: {stock_abbr}")
                else:
                    print(
                        f"最终股票简称：{stock_abbr}"
                        + ("（使用附件1官方简称映射）" if used_attachment1_mapping else "")
                    )

                # 同时从 PDF 内容中提取报告期和报告年份
                report_info = processor.extract_report_info_from_text(text_content)
                if not report_info:
                    print(f"警告：未从 PDF 内容中提取到报告期或年份，跳过此 PDF: {pdf_file}")
                    skipped_count += 1
                    continue

                report_period = report_info.get('report_period')
                report_year = report_info.get('report_year')

                # 验证是否提取到必要的信息
                if not report_period or not report_year:
                    print(f"警告：提取的报告信息不完整 - 报告期：{report_period}，年份：{report_year}")
                    print(f"跳过此 PDF: {pdf_file}")
                    skipped_count += 1
                    continue

                print(f"从 PDF 内容中提取到报告期：{report_period}，年份：{report_year}")

                print("使用以下信息处理财报:")
                print(f"股票代码: {stock_code}")
                print(f"股票简称: {stock_abbr}")
                print(f"报告期: {report_period}")
                print(f"报告年份: {report_year}")

                # 提取财务数据
                financial_data = processor.extract_financial_data(
                    text_content,
                    stock_code=stock_code,
                    stock_abbr=stock_abbr,
                    report_period=report_period,
                    report_year=report_year
                )

                # 打印提取的数据
                print("\n提取的财务数据:")
                for key, value in financial_data.items():
                    if not key.startswith('_') and value is not None:
                        print(f"{key}: {value}")

                # 统计非None值的数量
                non_none_count = processor.count_non_null_values(financial_data)
                total_count = len(processor._iter_financial_items(financial_data))
                print(f"\n提取成功率：{non_none_count}/{total_count} = {non_none_count/total_count*100:.2f}%")

                consistency_checks = financial_data.get('_consistency_checks', {})
                if consistency_checks:
                    print("\n跨表一致性校验:")
                    for check_name, result in consistency_checks.items():
                        print(f"{check_name}: {result['status']}")
                    print(f"record_quality: {financial_data.get('_record_quality', 'normal')}")
                    quality_flags = financial_data.get('_quality_flags', [])
                    if quality_flags:
                        print(f"quality_flags: {', '.join(quality_flags)}")

                # 插入数据到数据库
                insert_ok = processor.insert_data_to_db(financial_data)
                if insert_ok:
                    print("数据插入数据库成功")
                    success_count += 1
                else:
                    print("数据插入数据库失败")
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.exception(f"处理 PDF 失败: {pdf_file}")
                print(f"错误：处理失败，跳过此 PDF: {pdf_file}")
                print(f"原因：{e}")

        # 显示统计信息
        print(f"\n{'=' * 80}")
        print("处理完成！统计信息:")
        print(f"总文件数：{len(pdf_files)}")
        print(f"成功处理：{success_count} 个")
        print(f"失败：{failed_count} 个")
        print(f"跳过：{skipped_count} 个")
        print(f"{'=' * 80}\n")
    finally:
        if processor:
            processor.close_connection()
        print("处理完成！")
