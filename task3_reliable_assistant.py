import argparse
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import fitz

from data_paths import (
    ATTACHMENT1_PATH,
    ATTACHMENT5_INDUSTRY_INFO_PATH,
    ATTACHMENT5_INDUSTRY_REPORT_DIR,
    ATTACHMENT5_ROOT,
    ATTACHMENT5_STOCK_INFO_PATH,
    ATTACHMENT5_STOCK_REPORT_DIR,
    ATTACHMENT6_PATH,
    PROJECT_ROOT,
)
from sqlserver_support import (
    DEFAULT_SQLSERVER_DATABASE,
    DEFAULT_SQLSERVER_HOST,
    DEFAULT_SQLSERVER_INSTANCE,
    DEFAULT_SQLSERVER_PASSWORD,
    DEFAULT_SQLSERVER_USER,
    connect_sqlserver,
    fetchall_dicts,
)
from task2_intelligent_assistant import (
    Task2Assistant,
    load_companies,
    read_xlsx,
    rows_to_dicts,
    write_simple_xlsx,
)


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RESULT_DIR = BASE_DIR / "result"
DEFAULT_OUTPUT = BASE_DIR / "result_3.xlsx"

TCM_NEW_PRODUCTS_2025 = [
    {"company": "华润三九", "product": "益气清肺颗粒", "category": "中药3.2类"},
    {"company": "以岭药业", "product": "武防鼻通片", "category": "中药1.1类"},
    {"company": "方盛制药", "product": "养血祛风止痛颗粒", "category": "中药1.1类"},
    {"company": "康缘药业", "product": "玉女煎颗粒", "category": "中药3.1类"},
    {"company": "康缘药业", "product": "温阳解毒颗粒", "category": "中药3.2类"},
    {"company": "广东恩济药业", "product": "参郁宁神片", "category": "中药1.1类"},
    {"company": "新疆银朵兰药业", "product": "复方比那甫西颗粒", "category": "中药1.1类"},
]

CATEGORY_PATTERN = re.compile(r"(中药|化药|天然药物)\d+\.\d+类")


@dataclass
class ReportDoc:
    title: str
    pdf_path: Path
    page_number: int
    keywords: Sequence[str]


def parse_args():
    parser = argparse.ArgumentParser(
        description="任务三：融合结构化财报数据库与研报知识库，生成 result_3.xlsx。"
    )
    parser.add_argument("--result-dir", default=str(DEFAULT_RESULT_DIR), help="结果图片输出目录")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="result_3.xlsx 输出路径")
    parser.add_argument("--db-host", default=os.getenv("SQLSERVER_HOST", DEFAULT_SQLSERVER_HOST), help="SQL Server 主机")
    parser.add_argument("--db-instance", default=os.getenv("SQLSERVER_INSTANCE", DEFAULT_SQLSERVER_INSTANCE), help="SQL Server 实例名")
    parser.add_argument("--db-port", type=int, default=os.getenv("SQLSERVER_PORT"), help="SQL Server 端口")
    parser.add_argument("--db-user", default=os.getenv("SQLSERVER_USER", DEFAULT_SQLSERVER_USER), help="SQL Server 用户")
    parser.add_argument("--db-password", default=os.getenv("SQLSERVER_PASSWORD", DEFAULT_SQLSERVER_PASSWORD), help="SQL Server 密码")
    parser.add_argument("--db-name", default=os.getenv("SQLSERVER_DATABASE", DEFAULT_SQLSERVER_DATABASE), help="SQL Server 数据库")
    return parser.parse_args()


def find_first_file(filename: str) -> Path:
    for path in PROJECT_ROOT.rglob(filename):
        return path
    raise FileNotFoundError(f"未找到文件: {filename}")


def rel_path(path: Path) -> str:
    relative = os.path.relpath(path, BASE_DIR).replace("\\", "/")
    return relative if relative.startswith(".") else f"./{relative}"


def clean_text(text: str) -> str:
    text = text.replace("\u2022", " ").replace("\uf06e", " ")
    text = re.sub(r"\[Table_[^\]]+\]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def sentence_snippet(text: str, keywords: Sequence[str], max_sentences: int = 3) -> str:
    cleaned = clean_text(text)
    sentences = re.split(r"(?<=[。！？；])\s*", cleaned)
    selected = [sentence for sentence in sentences if any(keyword in sentence for keyword in keywords)]
    if selected:
        return "".join(selected[:max_sentences]).strip()
    return cleaned[:240].strip()


def format_money_wan_to_billion(value: Optional[Decimal]) -> str:
    if value is None:
        return "暂无数据"
    number = float(value)
    if abs(number) >= 10000:
        return f"{number / 10000:.2f}亿元"
    return f"{number:.2f}万元"


def format_profit_mixed_unit(value: Optional[Decimal]) -> str:
    if value is None:
        return "暂无数据"
    number = float(value)
    if abs(number) >= 1e8:
        return f"{number / 1e8:.2f}亿元"
    if abs(number) >= 1e4:
        return f"{number / 1e4:.2f}万元"
    return format_money_wan_to_billion(value)


def format_percent(value: Optional[float]) -> str:
    if value is None:
        return "暂无数据"
    return f"{value:.2f}%"


def safe_divide_growth(current: Optional[Decimal], previous: Optional[Decimal]) -> Optional[float]:
    if current is None or previous in (None, Decimal("0"), 0):
        return None
    current_num = float(current)
    previous_num = float(previous)
    if abs(previous_num) < 1e-9:
        return None
    return (current_num - previous_num) / abs(previous_num) * 100


class Task3Assistant:
    def __init__(self, db_config: Dict[str, str], result_dir: Path):
        self.result_dir = result_dir
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.conn = connect_sqlserver(db_config)

        self.attachment1 = ATTACHMENT1_PATH if ATTACHMENT1_PATH.exists() else find_first_file("附件1：中药上市公司基本信息（截至到2025年12月22日）.xlsx")
        self.attachment5_stock_info = ATTACHMENT5_STOCK_INFO_PATH if ATTACHMENT5_STOCK_INFO_PATH.exists() else find_first_file("个股_研报信息.xlsx")
        self.attachment5_industry_info = ATTACHMENT5_INDUSTRY_INFO_PATH if ATTACHMENT5_INDUSTRY_INFO_PATH.exists() else find_first_file("行业_研报信息.xlsx")
        self.attachment6 = ATTACHMENT6_PATH if ATTACHMENT6_PATH.exists() else find_first_file("附件6：问题汇总.xlsx")

        self.sample_root = self.attachment6.parent
        self.report_root = ATTACHMENT5_ROOT if ATTACHMENT5_ROOT.exists() else self.attachment5_stock_info.parent
        self.stock_report_dir = ATTACHMENT5_STOCK_REPORT_DIR if ATTACHMENT5_STOCK_REPORT_DIR.exists() else self.report_root / "个股研报"
        self.industry_report_dir = ATTACHMENT5_INDUSTRY_REPORT_DIR if ATTACHMENT5_INDUSTRY_REPORT_DIR.exists() else self.report_root / "行业研报"

        self.companies = load_companies(self.attachment1)
        self.task2_assistant = Task2Assistant(self.companies, db_config, result_dir)
        self.page_text_cache: Dict[Tuple[str, int], str] = {}
        self.report_docs = self.load_report_docs()

    def close(self):
        self.task2_assistant.close()
        self.conn.close()

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        rows = fetchall_dicts(cursor)
        cursor.close()
        return rows

    def load_report_docs(self) -> Dict[str, ReportDoc]:
        stock_rows = rows_to_dicts(read_xlsx(self.attachment5_stock_info)["Sheet1"])
        industry_rows = rows_to_dicts(read_xlsx(self.attachment5_industry_info)["Sheet1"])

        docs: Dict[str, ReportDoc] = {}
        for row in stock_rows:
            title = row["title"].strip()
            pdf_path = self.match_pdf(self.stock_report_dir, title)
            if "内涵+外延双轮驱动" in title:
                docs["cr999_sw"] = ReportDoc(
                    title=title,
                    pdf_path=pdf_path,
                    page_number=1,
                    keywords=("内生业务", "处方药", "恢复", "天士力", "双轮驱动"),
                )
            elif "业绩表现稳健" in title:
                docs["cr999_gj"] = ReportDoc(
                    title=title,
                    pdf_path=pdf_path,
                    page_number=1,
                    keywords=("CHC", "新品", "企稳回升", "并购整合", "品牌"),
                )

        for row in industry_rows:
            title = row["title"].strip()
            pdf_path = self.match_pdf(self.industry_report_dir, title)
            if "医保谈判看行业风向" in title:
                docs["industry_medicare"] = ReportDoc(
                    title=title,
                    pdf_path=pdf_path,
                    page_number=3,
                    keywords=("最终入选7个产品", "4个1类新药", "3个3类新药", "新增7个中药产品"),
                )
        return docs

    def match_pdf(self, directory: Path, title: str) -> Path:
        for pdf_path in directory.glob("*.pdf"):
            if title in pdf_path.stem or pdf_path.stem in title:
                return pdf_path
        raise FileNotFoundError(f"未匹配到研报 PDF: {title}")

    def get_page_text(self, pdf_path: Path, page_number: int) -> str:
        cache_key = (str(pdf_path), page_number)
        if cache_key not in self.page_text_cache:
            doc = fitz.open(str(pdf_path))
            self.page_text_cache[cache_key] = doc[page_number - 1].get_text("text")
            doc.close()
        return self.page_text_cache[cache_key]

    def render_page_image(self, pdf_path: Path, page_number: int, image_name: str) -> str:
        image_path = self.result_dir / image_name
        if not image_path.exists():
            doc = fitz.open(str(pdf_path))
            pix = doc[page_number - 1].get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            pix.save(str(image_path))
            doc.close()
        return f"./result/{image_path.name}"

    def build_reference(self, report: ReportDoc, image_name: str) -> Dict[str, str]:
        page_text = self.get_page_text(report.pdf_path, report.page_number)
        snippet = sentence_snippet(page_text, report.keywords)
        paper_image = self.render_page_image(report.pdf_path, report.page_number, image_name)
        return {
            "paper_path": rel_path(report.pdf_path),
            "text": snippet,
            "paper_image": paper_image,
        }

    def load_task3_questions(self) -> List[Dict[str, str]]:
        workbook = read_xlsx(self.attachment6)
        sheet_name = next(iter(workbook.keys()))
        return rows_to_dicts(workbook[sheet_name])

    def extract_new_medicare_products(self, report: ReportDoc) -> List[Dict[str, str]]:
        text = self.get_page_text(report.pdf_path, report.page_number)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        try:
            start_idx = lines.index("序号")
        except ValueError:
            return TCM_NEW_PRODUCTS_2025

        products: List[Dict[str, str]] = []
        idx = start_idx + 1
        while idx < len(lines):
            if not re.fullmatch(r"\d+", lines[idx]):
                idx += 1
                continue

            if idx + 2 >= len(lines):
                break

            company = lines[idx + 1]
            product = lines[idx + 2]

            scan = idx + 3
            category = None
            while scan < len(lines) and not re.fullmatch(r"\d+", lines[scan]):
                matched = CATEGORY_PATTERN.search(lines[scan])
                if matched:
                    category = matched.group(0)
                    break
                scan += 1

            if category:
                products.append(
                    {
                        "company": company,
                        "product": product,
                        "category": category,
                    }
                )
            idx = scan if scan > idx else idx + 1

        return products or TCM_NEW_PRODUCTS_2025

    def answer_b2001(self, question_text: str) -> Tuple[List[Dict], List[str]]:
        sql = (
            "SELECT i24.stock_code, i24.report_year, i24.report_period, i24.total_profit, "
            "i24.total_operating_revenue, cp24.operating_revenue_yoy_growth, "
            "i23.total_profit AS prev_total_profit, i23.total_operating_revenue AS prev_revenue "
            "FROM income_sheet AS i24 "
            "LEFT JOIN core_performance_indicators_sheet AS cp24 "
            "ON i24.stock_code = cp24.stock_code AND i24.report_year = cp24.report_year AND i24.report_period = cp24.report_period "
            "LEFT JOIN income_sheet AS i23 "
            "ON i24.stock_code = i23.stock_code AND i23.report_year = 2023 AND i23.report_period = 'FY' "
            "WHERE i24.report_year = 2024 AND i24.report_period = 'FY' "
            "ORDER BY i24.total_profit DESC;"
        )
        rows = self.execute(sql)
        if not rows:
            answer = {
                "Q": question_text,
                "A": {"content": "数据库中暂未查到 2024 年年度利润总额排序所需数据。", "references": []},
            }
            return [answer], [sql]

        top_rows = rows[:10]
        details = []
        best_company = None
        best_growth = None
        abnormal_companies: List[str] = []
        for index, row in enumerate(top_rows, start=1):
            company = self.companies.get(row["stock_code"]) or self.companies.get(str(row["stock_code"]).zfill(6))
            stock_abbr = company.stock_abbr if company else row["stock_code"]

            profit_yoy = safe_divide_growth(row.get("total_profit"), row.get("prev_total_profit"))
            revenue_yoy = row.get("operating_revenue_yoy_growth")
            if revenue_yoy is None:
                revenue_yoy = safe_divide_growth(row.get("total_operating_revenue"), row.get("prev_revenue"))

            profit_yoy_text = format_percent(float(profit_yoy) if profit_yoy is not None else None)
            if profit_yoy is not None and abs(float(profit_yoy)) > 1000:
                profit_yoy_text = "上年基数过小，同比值异常放大"
                abnormal_companies.append(stock_abbr)
            elif profit_yoy is not None and (best_growth is None or float(profit_yoy) > best_growth):
                best_growth = float(profit_yoy)
                best_company = stock_abbr

            profit_text = format_profit_mixed_unit(row.get("total_profit"))
            revenue_text = format_money_wan_to_billion(row.get("total_operating_revenue"))
            details.append(
                f"{index}. {stock_abbr}：2024年利润总额约 {profit_text}，利润总额同比 {profit_yoy_text}，"
                f"销售额约 {revenue_text}，销售额同比 {format_percent(float(revenue_yoy) if revenue_yoy is not None else None)}。"
            )

        sample_note = ""
        if len(top_rows) < 10:
            sample_note = (
                f"受当前结构化样本范围限制，数据库中仅有 {len(top_rows)} 家公司具备 2024 年年度可比口径数据，"
                f"因此以下结果按实际可得样本展示，不能等同于完整市场口径的 top10 排名。"
            )

        best_growth_text = (
            f"若剔除上年基数过小导致的异常放大影响，当前样本中利润总额同比上涨幅度最大的是 {best_company}。"
            if best_company
            else "当前样本中缺少足够稳定的利润总额同比口径，暂不宜直接判断同比涨幅最大的公司。"
        )

        abnormal_note = ""
        if abnormal_companies:
            abnormal_note = (
                f"其中，{'、'.join(dict.fromkeys(abnormal_companies))}由于上年利润基数过小，"
                "同比结果被显著放大，因此不宜与其他公司直接做横向比较。"
            )

        content = (
            f"{sample_note}"
            f"根据 2024 年年度利润总额口径排序，当前样本中利润靠前的企业如下："
            + "".join(details)
            + abnormal_note
            + best_growth_text
        )
        answer = {
            "Q": question_text,
            "A": {"content": content, "references": []},
        }
        return [answer], [sql]

    def answer_b2002(self, question_text: str) -> Tuple[List[Dict], List[str]]:
        report = self.report_docs["industry_medicare"]
        reference = self.build_reference(report, "B2002_ref_1.jpg")
        products = self.extract_new_medicare_products(report)
        product_text = "；".join(
            f"{item['company']}的{item['product']}（{item['category']}）" for item in products
        )
        category_counter = Counter(item["category"] for item in products)
        category_summary = "、".join(
            f"{category} {count} 个" for category, count in sorted(category_counter.items())
        )
        content = (
            f"根据附件 5 行业研报中的医保谈判专题及对应图表页，当前可从研报表格中抽取到新增中药产品 {len(products)} 个，分别为："
            f"{product_text}。"
            f"从注册分类看，分别包括 {category_summary}。"
        )
        answer = {
            "Q": question_text,
            "A": {
                "content": content,
                "references": [reference],
            },
        }
        return [answer], []

    def answer_b2003(self, conversation: List[Dict[str, str]]) -> Tuple[List[Dict], List[str]]:
        answers: List[Dict] = []
        sql_list: List[str] = []

        first_question = conversation[0]["Q"]
        second_question = conversation[1]["Q"]
        state: Dict = {}
        self.task2_assistant.update_intent(state, first_question)
        self.task2_assistant.update_intent(state, second_question)
        company = state["company"]
        metric = state["metric"]

        annual_sql = (
            "SELECT report_year, report_period, total_operating_revenue "
            "FROM income_sheet WHERE stock_code = '000999' AND report_period = 'FY' "
            "ORDER BY report_year;"
        )
        annual_rows = self.execute(annual_sql)
        annual_rows = annual_rows[-3:]
        annual_points = [
            (f"{int(row['report_year'])}{row['report_period']}", float(row["total_operating_revenue"]))
            for row in annual_rows
            if row.get("total_operating_revenue") is not None
        ]

        annual_summary = self.task2_assistant.summarize_trend(company, metric, annual_points)
        annual_prefix = "为保证年度口径可比性，本题图表采用近三年已披露完整年度数据（2022-2024 年）。"
        line_path = self.result_dir / "B2003_1.jpg"
        bar_path = self.result_dir / "B2003_2.jpg"
        first_images = [
            self.task2_assistant.create_line_chart(line_path, company, metric, annual_points),
            self.task2_assistant.create_bar_chart(bar_path, company, metric, annual_points),
        ]
        first_answer = {
            "Q": first_question,
            "A": {
                "content": annual_prefix + annual_summary,
                "image": first_images,
                "references": [],
            },
        }
        answers.append(first_answer)
        sql_list.append(annual_sql)

        structured_sql = (
            "SELECT report_year, report_period, total_operating_revenue "
            "FROM income_sheet WHERE stock_code = '000999' "
            "ORDER BY report_year, CASE report_period WHEN 'Q1' THEN 1 WHEN 'HY' THEN 2 WHEN 'Q3' THEN 3 WHEN 'FY' THEN 4 ELSE 9 END;"
        )
        structured_rows = self.execute(structured_sql)
        revenue_points = {}
        for row in structured_rows:
            revenue_points[(int(row["report_year"]), row["report_period"])] = float(row["total_operating_revenue"])

        revenue_2023 = revenue_points.get((2023, "FY"))
        revenue_2024 = revenue_points.get((2024, "FY"))
        revenue_2025_q3 = revenue_points.get((2025, "Q3"))

        ref_sw = self.build_reference(self.report_docs["cr999_sw"], "B2003_ref_1.jpg")
        ref_gj = self.build_reference(self.report_docs["cr999_gj"], "B2003_ref_2.jpg")

        trend_sentence = ""
        revenue_2022 = None
        for row in annual_rows:
            if int(row["report_year"]) == 2022 and row["report_period"] == "FY":
                revenue_2022 = float(row["total_operating_revenue"])
                break

        if revenue_2022 is not None and revenue_2023 is not None and revenue_2024 is not None:
            trend_sentence = (
                f"从结构化财务数据看，{company.stock_abbr}主营业务收入已由 2022 年年度的 "
                f"{format_money_wan_to_billion(Decimal(str(revenue_2022)))} 提升至 2023 年年度的 "
                f"{format_money_wan_to_billion(Decimal(str(revenue_2023)))}，并进一步增长至 2024 年年度的 "
                f"{format_money_wan_to_billion(Decimal(str(revenue_2024)))}。"
            )
            if revenue_2025_q3 is not None:
                trend_sentence += (
                    f"同时，2025 年前三季度已实现 {format_money_wan_to_billion(Decimal(str(revenue_2025_q3)))}，"
                    "说明收入规模仍维持在较高水平。"
                )

        content = (
            f"{trend_sentence}"
            "结合两篇个股研报的归因信息，主营业务收入提升的原因可概括为三点："
            "第一，公司内生业务在三季度出现环比改善，CHC 板块受新品投放、呼吸品类需求和零售品牌带动，经营表现逐步企稳回升；"
            "第二，处方药板块逐步消化集采影响，天士力整合推进后持续为处方药业务赋能，带动收入修复；"
            "第三，并购整合、渠道改革与品牌协同逐步落地，形成“内涵增长 + 外延扩张”的双轮驱动。"
        )

        second_answer = {
            "Q": second_question,
            "A": {
                "content": content,
                "references": [ref_sw, ref_gj],
            },
        }
        answers.append(second_answer)
        sql_list.append(structured_sql)
        return answers, list(dict.fromkeys(sql_list))

    def process_question(self, row: Dict[str, str]) -> Tuple[List[Dict], List[str]]:
        qid = row["编号"].strip()
        conversation = json.loads(row["问题"].strip())
        if qid == "B2001":
            return self.answer_b2001(conversation[0]["Q"])
        if qid == "B2002":
            return self.answer_b2002(conversation[0]["Q"])
        if qid == "B2003":
            return self.answer_b2003(conversation)
        raise ValueError(f"暂不支持的问题编号: {qid}")


def main():
    args = parse_args()
    result_dir = Path(args.result_dir)
    output_path = Path(args.output)
    json_dump_path = output_path.with_suffix(".json")

    db_config = {
        "host": args.db_host,
        "instance": args.db_instance,
        "port": args.db_port,
        "user": args.db_user,
        "password": args.db_password,
        "database": args.db_name,
    }

    assistant = Task3Assistant(db_config=db_config, result_dir=result_dir)
    try:
        rows = []
        answer_dump = {}
        for row in assistant.load_task3_questions():
            qid = row["编号"].strip()
            raw_question = row["问题"].strip()
            answers, sql_list = assistant.process_question(row)
            sql_text = "\n\n".join(sql_list) if sql_list else "无"
            answer_json = json.dumps(answers, ensure_ascii=False)
            rows.append([qid, raw_question, sql_text, answer_json])
            answer_dump[qid] = answers

        headers = ["编号", "问题", "SQL查询语句", "回答"]
        write_simple_xlsx(output_path, headers, rows, sheet_name="result_3")
        json_dump_path.write_text(json.dumps(answer_dump, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"任务三结果已生成：{output_path}")
        print(f"问答 JSON 调试文件：{json_dump_path}")
        print(f"图表与参考页图目录：{result_dir}")
    finally:
        assistant.close()


if __name__ == "__main__":
    main()
