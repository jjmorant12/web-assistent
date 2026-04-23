import json
import logging
import os
import re
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from financial_data_processor_complete import (
    FinancialDataProcessorComplete,
    collect_pdf_files,
    logger as task1_logger,
)
from task2_intelligent_assistant import write_simple_xlsx
from task3_reliable_assistant import Task3Assistant


class ListLogHandler(logging.Handler):
    def __init__(self, messages: List[str]):
        super().__init__()
        self.messages = messages

    def emit(self, record: logging.LogRecord):
        try:
            self.messages.append(self.format(record))
        except Exception:
            self.messages.append(record.getMessage())


def append_log(messages: List[str], text: str):
    messages.append(text)


def run_task1_ingest(pdf_path: str, db_config: Dict[str, Any]) -> Dict[str, Any]:
    logs: List[str] = []
    processed: List[Dict[str, Any]] = []
    stats = {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    log_handler = ListLogHandler(logs)
    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    task1_logger.addHandler(log_handler)

    processor: Optional[FinancialDataProcessorComplete] = None
    try:
        append_log(logs, "开始初始化财报入库任务。")
        processor = FinancialDataProcessorComplete(db_config)
        processor.create_tables()
        append_log(logs, "数据库连接成功，表结构已检查/创建。")

        normalized_pdf_path = os.path.abspath(pdf_path)
        pdf_files = collect_pdf_files(normalized_pdf_path)
        stats["total"] = len(pdf_files)
        append_log(logs, f"扫描完成，共找到 {len(pdf_files)} 个 PDF 文件。")

        for idx, pdf_file in enumerate(pdf_files, start=1):
            item = {
                "path": pdf_file,
                "status": "failed",
                "message": "",
                "stock_code": "",
                "stock_abbr": "",
                "report_period": "",
                "report_year": "",
            }
            append_log(logs, f"[{idx}/{len(pdf_files)}] 处理文件：{os.path.basename(pdf_file)}")
            try:
                text_content = processor.extract_text_from_pdf(pdf_file)
                stock_code = processor.extract_stock_code_from_text(text_content)
                if not stock_code:
                    stock_code_match = re.search(r"(\d{6})", os.path.basename(pdf_file))
                    if stock_code_match:
                        stock_code = stock_code_match.group(1)
                        append_log(logs, f"从文件名补提股票代码：{stock_code}")
                if not stock_code:
                    stats["skipped"] += 1
                    item["status"] = "skipped"
                    item["message"] = "未提取到股票代码"
                    processed.append(item)
                    append_log(logs, "跳过：未提取到股票代码。")
                    continue

                stock_abbr = processor.extract_stock_abbr_from_text(text_content)
                if not stock_abbr:
                    filename = os.path.basename(pdf_file)
                    chinese_name_match = re.search(r"(.+?)[_（(].*\.pdf", filename, re.IGNORECASE)
                    stock_abbr = chinese_name_match.group(1) if chinese_name_match else stock_code
                    append_log(logs, f"从文件名补提股票简称：{stock_abbr}")

                report_info = processor.extract_report_info_from_text(text_content)
                report_period = report_info.get("report_period") if report_info else None
                report_year = report_info.get("report_year") if report_info else None
                if not report_period or not report_year:
                    stats["skipped"] += 1
                    item["status"] = "skipped"
                    item["message"] = "未提取到完整报告期/年份"
                    item["stock_code"] = stock_code
                    item["stock_abbr"] = stock_abbr
                    processed.append(item)
                    append_log(logs, "跳过：未提取到完整报告期/年份。")
                    continue

                financial_data = processor.extract_financial_data(
                    text_content,
                    stock_code=stock_code,
                    stock_abbr=stock_abbr,
                    report_period=report_period,
                    report_year=report_year,
                )
                insert_ok = processor.insert_data_to_db(financial_data)

                item.update(
                    {
                        "stock_code": stock_code,
                        "stock_abbr": stock_abbr,
                        "report_period": report_period,
                        "report_year": report_year,
                        "status": "success" if insert_ok else "failed",
                        "message": "入库成功" if insert_ok else "入库失败",
                        "record_quality": financial_data.get("_record_quality", "normal"),
                        "quality_flags": financial_data.get("_quality_flags", []),
                        "consistency_checks": financial_data.get("_consistency_checks", {}),
                    }
                )
                processed.append(item)

                if insert_ok:
                    stats["success"] += 1
                    append_log(logs, f"入库成功：{stock_code} {stock_abbr} {report_year} {report_period}")
                else:
                    stats["failed"] += 1
                    append_log(logs, f"入库失败：{stock_code} {stock_abbr} {report_year} {report_period}")

            except Exception as exc:
                stats["failed"] += 1
                item["status"] = "failed"
                item["message"] = str(exc)
                processed.append(item)
                append_log(logs, f"处理失败：{pdf_file}")
                append_log(logs, traceback.format_exc())

        append_log(
            logs,
            (
                f"任务一处理完成：总文件 {stats['total']}，成功 {stats['success']}，"
                f"失败 {stats['failed']}，跳过 {stats['skipped']}。"
            ),
        )
        return {"ok": True, "stats": stats, "logs": logs, "items": processed}
    except Exception as exc:
        append_log(logs, f"任务一执行失败：{exc}")
        append_log(logs, traceback.format_exc())
        return {"ok": False, "stats": stats, "logs": logs, "items": processed, "error": str(exc)}
    finally:
        task1_logger.removeHandler(log_handler)
        if processor:
            try:
                processor.close_connection()
            except Exception:
                pass


def run_task3_generation(
    db_config: Dict[str, Any],
    result_dir: Path,
    output_path: Path,
) -> Dict[str, Any]:
    logs: List[str] = []
    question_results: List[Dict[str, Any]] = []
    json_dump_path = output_path.with_suffix(".json")
    assistant: Optional[Task3Assistant] = None

    try:
        append_log(logs, "开始生成任务三结果。")
        assistant = Task3Assistant(db_config=db_config, result_dir=result_dir)

        rows = []
        answer_dump = {}
        for row in assistant.load_task3_questions():
            qid = row["编号"].strip()
            raw_question = row["问题"].strip()
            append_log(logs, f"处理任务三问题：{qid}")
            answers, sql_list = assistant.process_question(row)
            sql_text = "\n\n".join(sql_list) if sql_list else "无"
            answer_json = json.dumps(answers, ensure_ascii=False)
            rows.append([qid, raw_question, sql_text, answer_json])
            answer_dump[qid] = answers
            question_results.append(
                {
                    "id": qid,
                    "question": raw_question,
                    "answers": answers,
                    "sql_list": sql_list,
                }
            )

        headers = ["编号", "问题", "SQL查询语句", "回答"]
        write_simple_xlsx(output_path, headers, rows, sheet_name="result_3")
        json_dump_path.write_text(json.dumps(answer_dump, ensure_ascii=False, indent=2), encoding="utf-8")

        append_log(logs, f"任务三结果已写入：{output_path}")
        append_log(logs, f"任务三 JSON 已写入：{json_dump_path}")
        return {
            "ok": True,
            "logs": logs,
            "items": question_results,
            "output_path": output_path,
            "json_path": json_dump_path,
        }
    except Exception as exc:
        append_log(logs, f"任务三执行失败：{exc}")
        append_log(logs, traceback.format_exc())
        return {"ok": False, "logs": logs, "items": question_results, "error": str(exc)}
    finally:
        if assistant:
            assistant.close()
