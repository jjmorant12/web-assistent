import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from data_paths import ATTACHMENT1_PATH, ATTACHMENT2_REPORTS_ROOT
from financial_data_processor_complete import FinancialDataProcessorComplete, collect_pdf_files
from task2_intelligent_assistant import write_simple_xlsx


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "result"
DEFAULT_JSON = "task1_result_nodb.json"
DEFAULT_XLSX = "task1_result_nodb.xlsx"


def _build_processor_without_db() -> FinancialDataProcessorComplete:
    processor = object.__new__(FinancialDataProcessorComplete)
    processor.db_config = {}
    processor.connection = None
    processor.cursor = None
    processor._active_text_cache_key = None
    processor._regex_findall_cache = {}
    processor._data_extract_cache = {}
    processor.company_abbr_mapping = processor.load_company_abbr_mapping(Path(ATTACHMENT1_PATH))
    return processor


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        return str(value)


def _extract_basic_identity(processor: FinancialDataProcessorComplete, pdf_file: str, text_content: str) -> Dict[str, Any]:
    stock_code = processor.extract_stock_code_from_text(text_content)
    if not stock_code:
        stock_code_match = re.search(r"(\d{6})", os.path.basename(pdf_file))
        stock_code = stock_code_match.group(1) if stock_code_match else ""

    raw_stock_abbr = processor.extract_stock_abbr_from_text(text_content)
    if not raw_stock_abbr:
        filename = os.path.basename(pdf_file)
        chinese_name_match = re.search(r"(.+?)[_（(].*\.pdf", filename, re.IGNORECASE)
        raw_stock_abbr = chinese_name_match.group(1) if chinese_name_match else stock_code

    stock_abbr, _ = processor.normalize_stock_abbr(
        raw_stock_abbr,
        stock_code,
        processor.company_abbr_mapping,
    )

    report_info = processor.extract_report_info_from_text(text_content) or {}
    return {
        "stock_code": stock_code,
        "stock_abbr": stock_abbr or raw_stock_abbr or stock_code,
        "raw_stock_abbr": raw_stock_abbr,
        "report_period": report_info.get("report_period") or "",
        "report_year": report_info.get("report_year") or "",
    }


def process_pdfs_nodb(
    pdf_path: str,
    output_dir: Path,
    *,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / DEFAULT_JSON
    xlsx_path = output_dir / DEFAULT_XLSX

    processor = _build_processor_without_db()
    pdf_files = collect_pdf_files(str(pdf_path))
    if limit and limit > 0:
        pdf_files = pdf_files[:limit]

    records: List[Dict[str, Any]] = []
    logs: List[str] = [f"扫描到 {len(pdf_files)} 个待处理 PDF。"]
    for idx, pdf_file in enumerate(pdf_files, start=1):
        record: Dict[str, Any] = {
            "source_pdf": str(pdf_file),
            "status": "failed",
            "message": "",
        }
        try:
            logs.append(f"[{idx}/{len(pdf_files)}] 处理：{os.path.basename(pdf_file)}")
            text_content = processor.extract_text_from_pdf(pdf_file)
            identity = _extract_basic_identity(processor, pdf_file, text_content)
            if not identity["stock_code"] or not identity["report_period"] or not identity["report_year"]:
                record.update(identity)
                record["status"] = "skipped"
                record["message"] = "缺少股票代码或报告期信息"
                records.append(record)
                continue

            financial_data = processor.extract_financial_data(
                text_content,
                stock_code=identity["stock_code"],
                stock_abbr=identity["stock_abbr"],
                report_period=identity["report_period"],
                report_year=identity["report_year"],
            )
            record.update({key: _json_safe(value) for key, value in financial_data.items() if not key.startswith("_")})
            record["status"] = "success"
            record["message"] = "提取完成，未连接数据库"
            records.append(record)
        except Exception as exc:
            record["message"] = str(exc)
            records.append(record)
            logs.append(f"处理失败：{exc}")

    all_keys: List[str] = []
    preferred = ["source_pdf", "status", "message", "stock_code", "stock_abbr", "report_year", "report_period"]
    for key in preferred:
        if any(key in record for record in records):
            all_keys.append(key)
    for record in records:
        for key in record:
            if key not in all_keys:
                all_keys.append(key)

    rows = [[record.get(key, "") for key in all_keys] for record in records]
    write_simple_xlsx(xlsx_path, all_keys, rows, sheet_name="task1_nodb")
    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = {
        "total": len(pdf_files),
        "success": sum(1 for record in records if record.get("status") == "success"),
        "failed": sum(1 for record in records if record.get("status") == "failed"),
        "skipped": sum(1 for record in records if record.get("status") == "skipped"),
    }
    logs.append(f"无数据库任务一完成：{stats}")
    return {
        "ok": True,
        "stats": stats,
        "logs": logs,
        "items": records,
        "json_path": json_path,
        "xlsx_path": xlsx_path,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="任务一无数据库版：解析财报 PDF 并导出 JSON/XLSX。")
    parser.add_argument("--pdf-path", default=str(ATTACHMENT2_REPORTS_ROOT), help="PDF 文件或目录路径")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少个 PDF，留空表示全部处理")
    return parser.parse_args()


def main():
    args = parse_args()
    result = process_pdfs_nodb(args.pdf_path, Path(args.output_dir), limit=args.limit)
    print(json.dumps({k: str(v) if isinstance(v, Path) else v for k, v in result.items() if k != "items"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
