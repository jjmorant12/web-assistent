import argparse
import hashlib
import json
import os
import re
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from data_paths import ATTACHMENT1_PATH, ATTACHMENT4_PATH
from sqlserver_support import (
    DEFAULT_SQLSERVER_DATABASE,
    DEFAULT_SQLSERVER_HOST,
    DEFAULT_SQLSERVER_INSTANCE,
    DEFAULT_SQLSERVER_PASSWORD,
    DEFAULT_SQLSERVER_USER,
    connect_sqlserver,
    fetchall_dicts,
)


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ATTACHMENT1 = ATTACHMENT1_PATH
DEFAULT_ATTACHMENT4 = ATTACHMENT4_PATH
DEFAULT_RESULT_DIR = BASE_DIR / "result"
DEFAULT_RESULT_XLSX = BASE_DIR / "result_2.xlsx"

EXCEL_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

PERIOD_ORDER = {"Q1": 1, "HY": 2, "Q3": 3, "FY": 4}
PERIOD_CN = {
    "Q1": "第一季度",
    "HY": "半年度",
    "Q3": "第三季度",
    "FY": "年度",
}
TREND_KEYWORDS = ("趋势", "走势", "变化", "近几年", "近几年的", "最近几年", "这几年", "变化咋样", "走势咋样", "可视化", "绘图", "图表")
LATEST_KEYWORDS = ("最新", "最近一期", "最新一期", "当前", "最近", "目前", "现在")
PROFIT_COLLOQUIAL_KEYWORDS = ("赚了多少钱", "赚多少钱", "赚了多少", "赚多少", "盈利多少", "盈利情况")
REVENUE_COLLOQUIAL_KEYWORDS = (
    "营收",
    "收入多少",
    "营收多少",
    "营业额",
    "卖了多少",
    "卖了多少钱",
    "卖得怎么样",
    "销售收入",
)
NET_PROFIT_COLLOQUIAL_KEYWORDS = ("净赚多少", "净赚了多少", "到手利润", "归母赚了多少", "净利润多少")
ASSET_COLLOQUIAL_KEYWORDS = ("家底", "资产规模", "家底厚不厚", "总家底")
CASHFLOW_COLLOQUIAL_KEYWORDS = ("现金流", "经营现金流", "经营性现金流", "现金流怎么样")
EPS_COLLOQUIAL_KEYWORDS = ("每股赚多少", "每股赚了多少", "每股能赚多少")

METRIC_CATALOG = [
    {
        "field": "total_profit",
        "name": "利润总额",
        "table": "income_sheet",
        "unit": "万元",
        "synonyms": ["利润总额", "利润总额金额", "利润有多少", "利润多少", "利润怎么样"],
    },
    {
        "field": "total_operating_revenue",
        "name": "主营业务收入",
        "table": "income_sheet",
        "unit": "万元",
        "synonyms": ["主营业务收入", "营业总收入", "营业收入", "销售额", "营收", "营业额", "销售收入"],
    },
    {
        "field": "net_profit",
        "name": "净利润",
        "table": "income_sheet",
        "unit": "万元",
        "synonyms": ["净利润", "归母净利润", "净赚", "到手利润"],
    },
    {
        "field": "eps",
        "name": "每股收益",
        "table": "core_performance_indicators_sheet",
        "unit": "元",
        "synonyms": ["每股收益", "eps", "每股赚多少"],
    },
    {
        "field": "asset_total_assets",
        "name": "资产总计",
        "table": "balance_sheet",
        "unit": "万元",
        "synonyms": ["资产总计", "总资产", "家底", "资产规模"],
    },
    {
        "field": "operating_cf_net_amount",
        "name": "经营活动产生的现金流量净额",
        "table": "cash_flow_sheet",
        "unit": "万元",
        "synonyms": ["经营活动产生的现金流量净额", "经营性现金流净额", "经营现金流净额", "现金流", "经营现金流"],
    },
]

MONEY_FIELDS_WANYUAN = {
    item["field"] for item in METRIC_CATALOG if item["unit"] == "万元"
}

PROFIT_FIELDS = {"total_profit", "net_profit"}
SIGNED_VALUE_FIELDS = {"eps", "operating_cf_net_amount"}


@dataclass
class Company:
    stock_code: str
    stock_abbr: str
    company_name: str
    exchange: str


def build_company_aliases(stock_abbr: str, company_name: str, stock_code: str) -> List[str]:
    aliases = {
        stock_abbr.strip(),
        company_name.strip(),
        stock_code.strip(),
        stock_code.strip().zfill(6),
    }
    for suffix in ("股份有限公司", "有限公司", "(集团)股份有限公司", "医药股份有限公司"):
        if company_name.endswith(suffix):
            aliases.add(company_name[: -len(suffix)])
    aliases.add(stock_abbr.replace("股份", ""))
    aliases.add(company_name.replace("股份有限公司", ""))
    return [alias for alias in aliases if alias]


def parse_args():
    parser = argparse.ArgumentParser(
        description="任务二：读取附件4问题，完成问数、出图并生成 result_2.xlsx。"
    )
    parser.add_argument("--attachment1", default=str(DEFAULT_ATTACHMENT1), help="附件1 公司信息文件路径")
    parser.add_argument("--attachment4", default=str(DEFAULT_ATTACHMENT4), help="附件4 问题文件路径")
    parser.add_argument("--result-dir", default=str(DEFAULT_RESULT_DIR), help="图表输出目录")
    parser.add_argument("--output", default=str(DEFAULT_RESULT_XLSX), help="result_2.xlsx 输出路径")
    parser.add_argument("--db-host", default=os.getenv("SQLSERVER_HOST", DEFAULT_SQLSERVER_HOST), help="SQL Server 主机")
    parser.add_argument("--db-instance", default=os.getenv("SQLSERVER_INSTANCE", DEFAULT_SQLSERVER_INSTANCE), help="SQL Server 实例名")
    parser.add_argument("--db-port", type=int, default=os.getenv("SQLSERVER_PORT"), help="SQL Server 端口，留空时自动探测 SQLEXPRESS 端口")
    parser.add_argument("--db-user", default=os.getenv("SQLSERVER_USER", DEFAULT_SQLSERVER_USER), help="SQL Server 用户")
    parser.add_argument("--db-password", default=os.getenv("SQLSERVER_PASSWORD", DEFAULT_SQLSERVER_PASSWORD), help="SQL Server 密码")
    parser.add_argument("--db-name", default=os.getenv("SQLSERVER_DATABASE", DEFAULT_SQLSERVER_DATABASE), help="SQL Server 数据库")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="进入终端对话模式，支持连续提问和上下文追问。",
    )
    return parser.parse_args()


def col_to_num(col: str) -> int:
    value = 0
    for char in col:
        if char.isalpha():
            value = value * 26 + ord(char.upper()) - 64
    return value


def num_to_col(num: int) -> str:
    chars = []
    while num:
        num, rem = divmod(num - 1, 26)
        chars.append(chr(65 + rem))
    return "".join(reversed(chars))


def read_xlsx(path: Path) -> Dict[str, List[List[str]]]:
    workbook: Dict[str, List[List[str]]] = {}
    with zipfile.ZipFile(path) as zf:
        shared_strings: List[str] = []
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
            rows: List[List[str]] = []
            for row in xml.findall("a:sheetData/a:row", EXCEL_NS):
                cell_map: Dict[int, str] = {}
                max_col = 0
                for cell in row.findall("a:c", EXCEL_NS):
                    ref = cell.attrib.get("r", "")
                    col_name = "".join(ch for ch in ref if ch.isalpha())
                    idx = col_to_num(col_name)
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


def rows_to_dicts(rows: List[List[str]]) -> List[Dict[str, str]]:
    if not rows:
        return []
    headers = rows[0]
    return [
        {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        for row in rows[1:]
        if any(cell != "" for cell in row)
    ]


def write_simple_xlsx(path: Path, headers: List[str], rows: List[List[str]], sheet_name: str = "Sheet1"):
    def cell_xml(ref: str, value: str) -> str:
        if value is None:
            value = ""
        text = escape(str(value)).replace("\n", "&#10;")
        return (
            f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'
        )

    sheet_rows = [headers] + rows
    sheet_xml_rows = []
    max_col = max(len(r) for r in sheet_rows) if sheet_rows else len(headers)
    for row_idx, row in enumerate(sheet_rows, start=1):
        cells = []
        for col_idx in range(1, max_col + 1):
            value = row[col_idx - 1] if col_idx - 1 < len(row) else ""
            ref = f"{num_to_col(col_idx)}{row_idx}"
            cells.append(cell_xml(ref, value))
        sheet_xml_rows.append(f'<row r="{row_idx}">{"".join(cells)}</row>')

    dimension = f"A1:{num_to_col(max_col)}{len(sheet_rows)}"
    sheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="{dimension}"/>
  <sheetViews><sheetView workbookViewId="0"/></sheetViews>
  <sheetFormatPr defaultRowHeight="15"/>
  <sheetData>
    {''.join(sheet_xml_rows)}
  </sheetData>
</worksheet>'''

    workbook_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>'''

    rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>'''

    workbook_rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>'''

    styles_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>'''

    content_types_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>'''

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("xl/styles.xml", styles_xml)


def format_stock_code(value: str) -> str:
    return str(value).strip().zfill(6)


def load_companies(path: Path) -> Dict[str, Company]:
    workbook = read_xlsx(path)
    rows = rows_to_dicts(workbook["基本信息表"])
    companies = {}
    for row in rows:
        company = Company(
            stock_code=format_stock_code(row["股票代码"]),
            stock_abbr=row["A股简称"].strip(),
            company_name=row["公司名称"].strip(),
            exchange=row["上市交易所"].strip(),
        )
        for alias in build_company_aliases(company.stock_abbr, company.company_name, company.stock_code):
            companies[alias] = company
    return companies


def load_questions(path: Path) -> List[Dict[str, str]]:
    workbook = read_xlsx(path)
    rows = rows_to_dicts(next(iter(workbook.values())))
    return rows


class Task2Assistant:
    def __init__(self, companies: Dict[str, Company], db_config: Dict[str, str], result_dir: Path):
        self.companies = companies
        self.result_dir = result_dir
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.chat_chart_counter = 1
        self.conn = connect_sqlserver(db_config)

        plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
        plt.rcParams["axes.unicode_minus"] = False

    def close(self):
        self.conn.close()

    def execute(self, sql: str, params: Tuple):
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        rows = fetchall_dicts(cursor)
        cursor.close()
        return rows

    def find_company(self, text: str) -> Optional[Company]:
        candidates = []
        for key, company in self.companies.items():
            if key and key in text:
                candidates.append((len(key), company))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def find_metric(self, text: str) -> Optional[Dict[str, str]]:
        ordered = sorted(METRIC_CATALOG, key=lambda item: max(len(s) for s in item["synonyms"]), reverse=True)
        for metric in ordered:
            for name in metric["synonyms"]:
                if name.lower() in text.lower():
                    return metric

        colloquial_rules = [
            ("total_operating_revenue", REVENUE_COLLOQUIAL_KEYWORDS),
            ("net_profit", NET_PROFIT_COLLOQUIAL_KEYWORDS),
            ("asset_total_assets", ASSET_COLLOQUIAL_KEYWORDS),
            ("operating_cf_net_amount", CASHFLOW_COLLOQUIAL_KEYWORDS),
            ("eps", EPS_COLLOQUIAL_KEYWORDS),
        ]
        for field, keywords in colloquial_rules:
            if any(keyword in text for keyword in keywords):
                return next(item for item in METRIC_CATALOG if item["field"] == field)
        return None

    def parse_year_range(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        pair_match = re.search(r"(20\d{2})年?[至到\-~](20\d{2})年?", text)
        if pair_match:
            start_year = int(pair_match.group(1))
            end_year = int(pair_match.group(2))
            return min(start_year, end_year), max(start_year, end_year)

        recent_match = re.search(r"近([两三四五六七八九十\d]+)年", text)
        if recent_match:
            token = recent_match.group(1)
            mapping = {
                "两": 2, "二": 2, "三": 3, "四": 4, "五": 5,
                "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
            }
            count = mapping.get(token)
            if count is None and token.isdigit():
                count = int(token)
            if count:
                return None, count
        return None, None

    def parse_period(self, text: str) -> Tuple[Optional[int], Optional[str]]:
        year_match = re.search(r"(20\d{2})", text)
        year = int(year_match.group(1)) if year_match else None
        period = None
        if (
            "第三季度" in text
            or "三季报" in text
            or "三季度" in text
            or re.search(r"(?i)Q\s*3", text)
        ):
            period = "Q3"
        elif (
            "第一季度" in text
            or "一季报" in text
            or "一季度" in text
            or re.search(r"(?i)Q\s*1", text)
        ):
            period = "Q1"
        elif (
            "半年度" in text
            or "半年报" in text
            or "中报" in text
            or "中期" in text
            or re.search(r"(?i)\bHY\b", text)
            or re.search(r"(?i)H\s*1", text)
        ):
            period = "HY"
        elif (
            "年度" in text
            or "年报" in text
            or "全年" in text
            or re.search(r"(?i)\bFY\b", text)
        ):
            period = "FY"
        return year, period

    def is_trend_query(self, text: str) -> bool:
        return any(keyword in text for keyword in TREND_KEYWORDS)

    def is_latest_query(self, text: str) -> bool:
        return any(keyword in text for keyword in LATEST_KEYWORDS)

    def normalize_amount(self, field: str, value: Optional[Decimal]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    def choose_display_unit(self, field: str, values: List[Optional[float]], unit: str) -> str:
        if unit == "元":
            return unit
        numeric_values = [abs(float(value)) for value in values if value is not None]
        if field in MONEY_FIELDS_WANYUAN and numeric_values and max(numeric_values) >= 10000:
            return "亿元"
        return unit

    def convert_for_display(self, field: str, value: Optional[float], display_unit: str) -> Optional[float]:
        if value is None:
            return None
        if field in MONEY_FIELDS_WANYUAN and display_unit == "亿元":
            return float(value) / 10000
        return float(value)

    def format_value(self, field: str, value: Optional[Decimal], unit: str) -> Tuple[str, str]:
        raw_number = self.normalize_amount(field, value)
        display_unit = self.choose_display_unit(field, [raw_number], unit)
        number = self.convert_for_display(field, raw_number, display_unit)
        if number is None:
            return "暂无数据", display_unit
        if display_unit == "元":
            return f"{number:.4f}", display_unit
        return f"{number:.2f}", display_unit

    def sql_for_point(self, company: Company, metric: Dict[str, str], year: int, period: str) -> str:
        return (
            f"SELECT {metric['field']} "
            f"FROM {metric['table']} "
            f"WHERE stock_code = '{company.stock_code}' "
            f"AND report_year = {year} "
            f"AND report_period = '{period}';"
        )

    def sql_for_trend(self, company: Company, metric: Dict[str, str]) -> str:
        return (
            f"SELECT report_year, report_period, {metric['field']} "
            f"FROM {metric['table']} "
            f"WHERE stock_code = '{company.stock_code}' "
            "ORDER BY report_year, CASE report_period "
            "WHEN 'Q1' THEN 1 WHEN 'HY' THEN 2 WHEN 'Q3' THEN 3 WHEN 'FY' THEN 4 ELSE 9 END;"
        )

    def get_latest_available_period(self, company: Company, metric: Dict[str, str]) -> Optional[Tuple[int, str]]:
        rows = self.execute(
            f"SELECT TOP 1 report_year, report_period FROM {metric['table']} "
            "WHERE stock_code=%s AND {field} IS NOT NULL "
            "ORDER BY report_year DESC, CASE report_period "
            "WHEN 'FY' THEN 4 WHEN 'Q3' THEN 3 WHEN 'HY' THEN 2 WHEN 'Q1' THEN 1 ELSE 0 END DESC".format(
                field=metric["field"]
            ),
            (company.stock_code,),
        )
        if not rows:
            return None
        return int(rows[0]["report_year"]), rows[0]["report_period"]

    def ask_clarification(self, intent: Dict) -> str:
        if not intent.get("company"):
            return "请问你查询的是哪一家上市公司？"
        if not intent.get("metric"):
            if intent.get("profit_like"):
                return "请问你想查询的是利润总额，还是净利润？"
            return "请问你想查询哪一项财务指标？"
        metric_name = intent["metric"]["name"]
        return f"请问你查询哪一个报告期的{metric_name}？"

    def build_period_label(self, year: int, period: str) -> str:
        return f"{year}年{PERIOD_CN.get(period, period)}"

    def answer_point_query(self, intent: Dict) -> Tuple[str, str]:
        company = intent["company"]
        metric = intent["metric"]
        year = intent.get("year")
        period = intent.get("period")
        if (year is None or period is None) and intent.get("latest"):
            latest_period = self.get_latest_available_period(company, metric)
            if latest_period:
                year, period = latest_period
        sql = self.sql_for_point(company, metric, year, period)
        rows = self.execute(
            f"SELECT {metric['field']} FROM {metric['table']} WHERE stock_code=%s AND report_year=%s AND report_period=%s",
            (company.stock_code, year, period),
        )
        if not rows:
            content = f"数据库中没有查到{company.stock_abbr}{self.build_period_label(year, period)}的{metric['name']}。"
            return content, sql

        raw_value = rows[0][metric["field"]]
        formatted, display_unit = self.format_value(metric["field"], raw_value, metric["unit"])
        sign_desc = ""
        normalized = self.normalize_amount(metric["field"], raw_value)
        if normalized is not None:
            field = metric["field"]
            if field in PROFIT_FIELDS:
                if normalized > 0:
                    sign_desc = "，处于盈利状态"
                elif normalized < 0:
                    sign_desc = "，呈现亏损状态"
                else:
                    sign_desc = "，处于盈亏平衡附近"
            elif field in SIGNED_VALUE_FIELDS:
                if normalized > 0:
                    sign_desc = "，为正值"
                elif normalized < 0:
                    sign_desc = "，为负值"

        content = (
            f"{company.stock_abbr}{self.build_period_label(year, period)}的{metric['name']}是"
            f"{formatted}{display_unit}{sign_desc}。"
        )
        return content, sql

    def compress_trend_rows(self, rows: List[Dict], field: str) -> List[Tuple[str, float]]:
        grouped: Dict[int, List[Dict]] = {}
        for row in rows:
            grouped.setdefault(int(row["report_year"]), []).append(row)

        points: List[Tuple[str, float]] = []
        for year in sorted(grouped):
            year_rows = sorted(grouped[year], key=lambda r: PERIOD_ORDER.get(r["report_period"], 9))
            representative = None
            for candidate in reversed(year_rows):
                if candidate["report_period"] == "FY":
                    representative = candidate
                    break
            if representative is None:
                representative = year_rows[-1]
            label = f"{representative['report_year']}{representative['report_period']}"
            value = self.normalize_amount(field, representative[field])
            if value is not None:
                points.append((label, value))
        return points

    def filter_trend_points(self, points: List[Tuple[str, float]], start_year: Optional[int], end_year: Optional[int], recent_count: Optional[int]) -> List[Tuple[str, float]]:
        filtered = points
        if start_year is not None and end_year is not None:
            filtered = [point for point in filtered if start_year <= int(point[0][:4]) <= end_year]
        if recent_count:
            filtered = filtered[-recent_count:]
        return filtered

    def format_point_label(self, label: str) -> str:
        year = int(label[:4])
        period = label[4:]
        return self.build_period_label(year, period)

    def summarize_change(self, first_value: float, last_value: float, unit: str) -> str:
        delta = last_value - first_value
        if abs(delta) < 1e-9:
            return "与起始点相比基本持平。"

        direction = "增加" if delta > 0 else "减少"
        change_text = f"较起始点{direction}{abs(delta):.2f}{unit}"
        if abs(first_value) > 1e-9 and first_value * last_value > 0:
            pct = abs(delta / first_value) * 100
            return f"最新一期{change_text}，变动幅度约为 {pct:.2f}%。"
        return f"最新一期{change_text}。"

    def summarize_trend(self, company: Company, metric: Dict[str, str], points: List[Tuple[str, float]]) -> str:
        if len(points) == 1:
            only_label = self.format_point_label(points[0][0])
            display_unit = self.choose_display_unit(metric["field"], [points[0][1]], metric["unit"])
            only_value = self.convert_for_display(metric["field"], points[0][1], display_unit)
            return (
                f"{company.stock_abbr}目前仅有一个可用数据点，暂不宜据此判断长期趋势。"
                f"截至{only_label}，{metric['name']}为 {only_value:.2f}{display_unit}。"
            )

        labels = [label for label, _ in points]
        pretty_labels = [self.format_point_label(label) for label in labels]
        raw_values = [value for _, value in points]
        display_unit = self.choose_display_unit(metric["field"], raw_values, metric["unit"])
        values = [self.convert_for_display(metric["field"], value, display_unit) for value in raw_values]
        first_value = values[0]
        last_value = values[-1]
        min_idx = values.index(min(values))
        max_idx = values.index(max(values))

        if min_idx not in (0, len(values) - 1) and values[-1] > values[min_idx]:
            trend_desc = "整体呈先下滑后修复的走势"
            stage_desc = f"区间内曾在{pretty_labels[min_idx]}降至阶段低点，随后逐步回升。"
        elif max_idx not in (0, len(values) - 1) and values[-1] < values[max_idx] * 0.9:
            trend_desc = "整体呈冲高后回落的走势"
            stage_desc = f"区间内在{pretty_labels[max_idx]}达到阶段高点后有所回落。"
        elif last_value > first_value * 1.1:
            trend_desc = "整体呈上升趋势"
            stage_desc = f"从{pretty_labels[0]}到{pretty_labels[-1]}，总体表现为持续抬升。"
        elif last_value < first_value * 0.9:
            trend_desc = "整体呈下降趋势"
            stage_desc = f"从{pretty_labels[0]}到{pretty_labels[-1]}，总体表现为逐步走弱。"
        else:
            trend_desc = "整体呈波动走势"
            stage_desc = "各期数据存在一定波动，但方向性不算特别强。"

        change_desc = self.summarize_change(first_value, last_value, display_unit)

        return (
            f"从{pretty_labels[0]}到{pretty_labels[-1]}的可比口径样本看，"
            f"{company.stock_abbr}的{metric['name']}{trend_desc}。"
            f"{stage_desc}"
            f"起始点为{pretty_labels[0]}的 {values[0]:.2f}{display_unit}，"
            f"最新点为{pretty_labels[-1]}的 {values[-1]:.2f}{display_unit}；"
            f"区间最低点出现在{pretty_labels[min_idx]}（{values[min_idx]:.2f}{display_unit}），"
            f"最高点出现在{pretty_labels[max_idx]}（{values[max_idx]:.2f}{display_unit}）。"
            f"{change_desc}"
        )

    def build_trend_chart_stem(self, question_id: str, company: Company, metric: Dict[str, str], points: List[Tuple[str, float]]) -> str:
        if not question_id.startswith("CHAT"):
            return question_id

        payload = {
            "stock_code": company.stock_code,
            "metric": metric["field"],
            "points": points,
        }
        digest = hashlib.md5(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:12]
        return f"CHAT_{company.stock_code}_{metric['field']}_{digest}"

    def create_line_chart(self, image_path: Path, company: Company, metric: Dict[str, str], points: List[Tuple[str, float]]) -> str:
        labels = [label for label, _ in points]
        raw_values = [value for _, value in points]
        display_unit = self.choose_display_unit(metric["field"], raw_values, metric["unit"])
        values = [self.convert_for_display(metric["field"], value, display_unit) for value in raw_values]

        plt.figure(figsize=(9, 4.8))
        plt.plot(labels, values, marker="o", linewidth=2, color="#2E75B6")
        plt.title(f"{company.stock_abbr}{metric['name']}变化趋势")
        plt.xlabel("报告期")
        plt.ylabel(display_unit)
        plt.grid(alpha=0.25, linestyle="--")
        plt.tight_layout()
        plt.savefig(image_path, dpi=160, format="jpg")
        plt.close()
        return f"./result/{image_path.name}"

    def create_bar_chart(self, image_path: Path, company: Company, metric: Dict[str, str], points: List[Tuple[str, float]]) -> str:
        labels = [label for label, _ in points]
        raw_values = [value for _, value in points]
        display_unit = self.choose_display_unit(metric["field"], raw_values, metric["unit"])
        values = [self.convert_for_display(metric["field"], value, display_unit) for value in raw_values]

        plt.figure(figsize=(9, 4.8))
        bars = plt.bar(labels, values, color="#70AD47")
        plt.title(f"{company.stock_abbr}{metric['name']}区间对比")
        plt.xlabel("报告期")
        plt.ylabel(display_unit)
        plt.grid(axis="y", alpha=0.25, linestyle="--")
        for bar, value in zip(bars, values):
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{value:.1f}", ha="center", va="bottom", fontsize=8)
        plt.tight_layout()
        plt.savefig(image_path, dpi=160, format="jpg")
        plt.close()
        return f"./result/{image_path.name}"

    def get_or_create_trend_images(self, question_id: str, company: Company, metric: Dict[str, str], points: List[Tuple[str, float]]) -> List[str]:
        stem = self.build_trend_chart_stem(question_id, company, metric, points)
        line_path = self.result_dir / f"{stem}_1.jpg"
        bar_path = self.result_dir / f"{stem}_2.jpg"

        images: List[str] = []
        if line_path.exists():
            images.append(f"./result/{line_path.name}")
        else:
            images.append(self.create_line_chart(line_path, company, metric, points))

        if bar_path.exists():
            images.append(f"./result/{bar_path.name}")
        else:
            images.append(self.create_bar_chart(bar_path, company, metric, points))

        return images

    def answer_trend_query(self, question_id: str, intent: Dict) -> Tuple[str, str, List[str]]:
        company = intent["company"]
        metric = intent["metric"]
        sql = self.sql_for_trend(company, metric)
        rows = self.execute(
            f"SELECT report_year, report_period, {metric['field']} FROM {metric['table']} "
            "WHERE stock_code=%s "
            "ORDER BY report_year, CASE report_period "
            "WHEN 'Q1' THEN 1 WHEN 'HY' THEN 2 WHEN 'Q3' THEN 3 WHEN 'FY' THEN 4 ELSE 9 END",
            (company.stock_code,),
        )
        if not rows:
            return f"数据库中没有查到{company.stock_abbr}的{metric['name']}趋势数据。", sql, []

        points = self.compress_trend_rows(rows, metric["field"])
        points = self.filter_trend_points(
            points,
            intent.get("start_year"),
            intent.get("end_year"),
            intent.get("recent_count"),
        )
        if not points:
            return f"数据库中没有查到{company.stock_abbr}满足条件的{metric['name']}趋势数据。", sql, []
        content = self.summarize_trend(company, metric, points)
        images = self.get_or_create_trend_images(question_id, company, metric, points)
        return content, sql, images

    def update_intent(self, intent: Dict, text: str):
        company = self.find_company(text)
        metric = self.find_metric(text)
        year, period = self.parse_period(text)
        range_start, range_end_or_count = self.parse_year_range(text)

        if company:
            intent["company"] = company
        if metric:
            intent["metric"] = metric
        if year:
            intent["year"] = year
        if period:
            intent["period"] = period
        if range_start is not None and range_end_or_count is not None:
            intent["start_year"] = range_start
            intent["end_year"] = range_end_or_count
        elif range_start is None and range_end_or_count is not None:
            intent["recent_count"] = range_end_or_count
        if any(keyword in text for keyword in PROFIT_COLLOQUIAL_KEYWORDS):
            intent["profit_like"] = True
        if self.is_latest_query(text):
            intent["latest"] = True
        if self.is_trend_query(text):
            intent["query_type"] = "trend"
        elif "query_type" not in intent:
            intent["query_type"] = "point"

    def handle_conversation(self, question_id: str, conversation: List[Dict[str, str]]) -> Tuple[List[Dict], List[str], str]:
        state: Dict = {}
        answers = []
        executed_sql = []
        image_paths: List[str] = []

        for turn in conversation:
            answer_item, sql, images, _ = self.handle_turn(question_id, state, turn["Q"])
            answers.append(answer_item)
            if sql:
                executed_sql.append(sql)
            image_paths.extend(images)

        chart_type = "折线图、柱状图" if len(image_paths) >= 2 else ("折线图" if image_paths else "无")
        return answers, executed_sql, chart_type

    def handle_turn(self, question_id: str, state: Dict, question_text: str) -> Tuple[Dict, Optional[str], List[str], str]:
        self.update_intent(state, question_text)

        missing_required = not state.get("company") or not state.get("metric")
        if state.get("query_type", "point") == "point":
            period_missing = not state.get("year") or not state.get("period")
            if missing_required or (period_missing and not state.get("latest")):
                content = self.ask_clarification(state)
                return {"Q": question_text, "A": {"content": content}}, None, [], "无"

            content, sql = self.answer_point_query(state)
            return {"Q": question_text, "A": {"content": content}}, sql, [], "无"

        if missing_required:
            content = self.ask_clarification(state)
            return {"Q": question_text, "A": {"content": content}}, None, [], "无"

        content, sql, images = self.answer_trend_query(question_id, state)
        answer_body = {"content": content}
        if images:
            answer_body["image"] = images
        chart_type = "折线图、柱状图" if len(images) >= 2 else ("折线图" if images else "无")
        return {"Q": question_text, "A": answer_body}, sql, images, chart_type

    def next_chat_question_id(self) -> str:
        question_id = f"CHAT{self.chat_chart_counter:03d}"
        self.chat_chart_counter += 1
        return question_id


def main():
    args = parse_args()
    attachment1 = Path(args.attachment1)
    attachment4 = Path(args.attachment4)
    output_path = Path(args.output)
    result_dir = Path(args.result_dir)

    companies = load_companies(attachment1)
    questions = load_questions(attachment4)

    assistant = Task2Assistant(
        companies=companies,
        db_config={
            "host": args.db_host,
            "instance": args.db_instance,
            "port": args.db_port,
            "user": args.db_user,
            "password": args.db_password,
            "database": args.db_name,
        },
        result_dir=result_dir,
    )

    result_rows: List[List[str]] = []
    json_dump_path = output_path.with_suffix(".json")
    all_answers = {}
    try:
        if args.interactive:
            state: Dict = {}
            print("已进入智能问数助手对话模式。")
            print("输入 /reset 可清空上下文，输入 /exit 可退出。")
            while True:
                user_text = input("\n你：").strip()
                if not user_text:
                    continue
                if user_text.lower() in {"/exit", "exit", "quit"}:
                    print("助手：本次对话已结束。")
                    break
                if user_text.lower() == "/reset":
                    state.clear()
                    print("助手：上下文已清空，你可以开始新的问题。")
                    continue

                question_id = assistant.next_chat_question_id()
                answer_item, sql, images, chart_type = assistant.handle_turn(question_id, state, user_text)
                answer = answer_item["A"]
                print(f"助手：{answer['content']}")
                if sql:
                    print(f"SQL：{sql}")
                if images:
                    print(f"图表（{chart_type}）：")
                    for image in images:
                        print(f"  {image}")
            return

        for row in questions:
            qid = row["编号"].strip()
            raw_question = row["问题"].strip()
            conversation = json.loads(raw_question)

            answers, sql_list, chart_type = assistant.handle_conversation(qid, conversation)
            answer_json = json.dumps(answers, ensure_ascii=False)
            sql_text = "\n\n".join(dict.fromkeys(sql_list)) if sql_list else "无"

            result_rows.append([qid, raw_question, sql_text, chart_type, answer_json])
            all_answers[qid] = answers

        headers = ["编号", "问题", "SQL查询语句", "图形格式", "回答"]
        write_simple_xlsx(output_path, headers, result_rows, sheet_name="result_2")
        json_dump_path.write_text(json.dumps(all_answers, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"任务二结果已生成：{output_path}")
        print(f"问答 JSON 调试文件：{json_dump_path}")
        print(f"图表输出目录：{result_dir}")
    finally:
        assistant.close()


if __name__ == "__main__":
    main()
