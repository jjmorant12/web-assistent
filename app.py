import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from data_paths import ATTACHMENT2_REPORTS_ROOT
from financial_data_processor_nodb import process_pdfs_nodb
from sqlserver_support import default_sqlserver_config
from task2_intelligent_assistant import (
    DEFAULT_ATTACHMENT1,
    DEFAULT_RESULT_DIR as TASK2_RESULT_DIR,
    DEFAULT_RESULT_XLSX as TASK2_RESULT_XLSX,
    Task2Assistant,
    load_companies,
)
from task3_reliable_assistant import DEFAULT_OUTPUT as TASK3_RESULT_XLSX
from web_adapters import run_task1_ingest, run_task3_generation


BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR / "result"
TASK3_RESULT_JSON = BASE_DIR / "result_3.json"
TASK2_RESULT_JSON = BASE_DIR / "result_2.json"
DEFAULT_TASK1_PDF_DIR = ATTACHMENT2_REPORTS_ROOT
DEFAULT_TASK1_PDF_DISPLAY = "财务报告"
DEPLOY_DATA_DIR = BASE_DIR / "deploy_data"
TASK1_DEMO_JSON = DEPLOY_DATA_DIR / "task1_result_nodb.json"
TASK1_DEMO_XLSX = DEPLOY_DATA_DIR / "task1_result_nodb.xlsx"


st.set_page_config(
    page_title="财报智能问数演示版",
    page_icon="📊",
    layout="wide",
)


def build_db_config() -> Dict[str, Any]:
    base_config = default_sqlserver_config()
    with st.sidebar:
        st.header("数据库配置")
        st.caption("默认读取当前项目使用的 SQL Server 配置，可在此处临时调整。")
        host = st.text_input("Host", value=base_config["host"])
        instance = st.text_input("Instance", value=base_config["instance"])
        port_text = st.text_input("Port", value=str(base_config["port"] or ""))
        user = st.text_input("User", value=base_config["user"])
        password = st.text_input("Password", value=base_config["password"], type="password")
        database = st.text_input("Database", value=base_config["database"])

    return {
        "host": host.strip(),
        "instance": instance.strip(),
        "port": int(port_text) if port_text.strip() else None,
        "user": user.strip(),
        "password": password,
        "database": database.strip(),
    }


def build_runtime_config() -> Tuple[str, Dict[str, Any]]:
    env_mode = os.getenv("TAIDIBEI_APP_MODE", "").strip().lower()
    default_db_mode = env_mode in {"db", "database", "local"}
    with st.sidebar:
        st.header("运行模式")
        mode_label = st.radio(
            "请选择运行方式",
            ["部署演示模式（无数据库）", "本地完整模式（SQL Server）"],
            index=1 if default_db_mode else 0,
        )
        st.caption("云端部署建议使用无数据库模式；本地需要完整入库/实时查询时再切换 SQL Server 模式。")

    if mode_label.startswith("本地完整"):
        return "db", build_db_config()
    return "demo", {}


def file_download_button(label: str, path: Path, mime: str):
    if path.exists():
        with path.open("rb") as fh:
            st.download_button(label=label, data=fh.read(), file_name=path.name, mime=mime)
    else:
        st.caption(f"文件暂不存在：{path.name}")


def resolve_result_path(path_str: str) -> Optional[Path]:
    if not path_str:
        return None
    path = Path(path_str)
    if path.is_absolute() and path.exists():
        return path
    relative = path_str[2:] if path_str.startswith("./") else path_str
    candidate = BASE_DIR / relative
    if candidate.exists():
        return candidate
    candidate = RESULT_DIR / Path(relative).name
    if candidate.exists():
        return candidate
    return None


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def iter_answer_items(answer_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for qid, turns in answer_data.items():
        if not isinstance(turns, list):
            continue
        for turn in turns:
            if isinstance(turn, dict):
                item = dict(turn)
                item["id"] = qid
                items.append(item)
    return items


def find_demo_answer(question: str, answer_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    normalized_question = question.strip()
    if not normalized_question:
        return None
    for item in iter_answer_items(answer_data):
        if item.get("Q", "").strip() == normalized_question:
            return item
    for item in iter_answer_items(answer_data):
        if normalized_question in item.get("Q", "") or item.get("Q", "") in normalized_question:
            return item
    return None


def resolve_task1_pdf_dir(display_value: str) -> Path:
    normalized = (display_value or "").strip()
    if normalized in {"", DEFAULT_TASK1_PDF_DISPLAY, "示例数据 / 附件2：财务报告"}:
        return Path(DEFAULT_TASK1_PDF_DIR)
    return Path(normalized)


def get_task2_assistant(
    attachment1: Path,
    db_config: Dict[str, Any],
    result_dir: Path,
) -> Task2Assistant:
    config_signature = (
        str(attachment1),
        json.dumps(db_config, sort_keys=True, ensure_ascii=False),
        str(result_dir),
    )
    current_signature = st.session_state.get("task2_assistant_signature")
    if current_signature != config_signature:
        old_assistant = st.session_state.get("task2_assistant")
        if old_assistant is not None:
            try:
                old_assistant.close()
            except Exception:
                pass
        companies = load_companies(attachment1)
        st.session_state["task2_assistant"] = Task2Assistant(
            companies=companies,
            db_config=db_config,
            result_dir=result_dir,
        )
        st.session_state["task2_assistant_signature"] = config_signature
        st.session_state["task2_state"] = {}
        st.session_state["task2_history"] = []
    return st.session_state["task2_assistant"]


def render_answer_block(answer_body: Dict[str, Any]):
    st.write(answer_body.get("content", ""))

    images = answer_body.get("image", [])
    if images:
        st.markdown("图表：")
        for image in images:
            image_path = resolve_result_path(image)
            if image_path:
                st.image(str(image_path), use_container_width=True)
            else:
                st.caption(f"未找到图片：{image}")

    references = answer_body.get("references", [])
    if references:
        st.markdown("参考依据：")
        for idx, ref in enumerate(references, start=1):
            with st.expander(f"参考 {idx}", expanded=True):
                if ref.get("paper_path"):
                    st.caption(f"研报路径：{ref['paper_path']}")
                if ref.get("text"):
                    st.write(ref["text"])
                if ref.get("paper_image"):
                    image_path = resolve_result_path(ref["paper_image"])
                    if image_path:
                        st.image(str(image_path), use_container_width=True)
                    else:
                        st.caption(f"未找到参考图片：{ref['paper_image']}")


def render_home_page(mode: str):
    st.title("上市公司财报智能问数系统")
    if mode == "db":
        st.caption("本地完整模式：连接 SQL Server，保留任务一、任务二、任务三完整流程。")
    else:
        st.caption("部署演示模式：不连接 SQL Server，使用本地结果文件展示任务一、任务二、任务三。")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("任务一", "财报入库")
        st.write("本地完整模式可写入 SQL Server；部署演示模式可解析 PDF 并导出 JSON/XLSX。")
    with col2:
        st.metric("任务二", "智能问数")
        st.write("输入自然语言问题，返回文本答案、SQL 和图表结果。")
    with col3:
        st.metric("任务三", "可靠性增强")
        st.write("融合研报知识库与结构化数据，输出可追溯结果和参考依据。")

    st.markdown("### 快速开始")
    st.markdown(
        "\n".join(
            [
                "1. 先在左侧选择运行模式：云端部署默认使用无数据库模式。",
                "2. 进入“财报入库”，输入 PDF 目录并执行任务一。",
                "3. 进入“智能问数”，直接输入自然语言问题进行演示。",
                "4. 进入“可靠性增强分析”，点击按钮生成任务三结果。",
            ]
        )
    )

    st.markdown("### 推荐演示顺序")
    st.markdown(
        "\n".join(
            [
                "- 先演示任务一：展示财报入库入口和运行日志。",
                "- 再演示任务二：先问点查题，再问趋势题，显示 SQL 和图表。",
                "- 最后演示任务三：展示结果摘要、参考图片和文件下载。",
            ]
        )
    )

    st.markdown("### 任务二示例问题")
    sample_questions = [
        "金花股份2025年第三季度利润总额是多少",
        "金花股份最新一期净利润是多少",
        "金花股份近几年的利润总额变化趋势是什么样的",
        "华润三九近三年的主营业务收入情况怎么样",
    ]
    for q in sample_questions:
        st.code(q, language="text")


def render_task1_page(db_config: Dict[str, Any], use_db: bool):
    st.title("财报入库")
    if use_db:
        st.caption("本地完整模式：沿用当前任务一逻辑，批量解析 PDF 并写入 SQL Server。")
    else:
        st.caption("部署演示模式：不连接 SQL Server，解析 PDF 后导出 JSON/XLSX；如果云端未打包 PDF，则展示内置演示结果。")

    pdf_dir_display = st.text_input("PDF 目录路径", value=DEFAULT_TASK1_PDF_DISPLAY)
    max_files = 1
    if not use_db:
        max_files = st.number_input("演示处理文件数", min_value=1, max_value=20, value=1, step=1)
    col1, col2 = st.columns([1, 1])
    with col1:
        start = st.button("开始执行任务一", type="primary", use_container_width=True)
    with col2:
        clear = st.button("清空本页结果", use_container_width=True)

    if clear:
        st.session_state.pop("task1_result", None)
        st.rerun()

    if start:
        pdf_dir = resolve_task1_pdf_dir(pdf_dir_display)
        if use_db:
            with st.spinner("正在执行任务一入库，请稍候..."):
                result = run_task1_ingest(str(pdf_dir), db_config)
        else:
            pdf_path = Path(pdf_dir)
            if pdf_path.exists():
                with st.spinner("正在执行任务一无数据库导出，请稍候..."):
                    result = process_pdfs_nodb(str(pdf_path), RESULT_DIR, limit=int(max_files))
            elif TASK1_DEMO_JSON.exists() or (RESULT_DIR / "task1_result_nodb.json").exists():
                demo_json = TASK1_DEMO_JSON if TASK1_DEMO_JSON.exists() else RESULT_DIR / "task1_result_nodb.json"
                demo_xlsx = TASK1_DEMO_XLSX if TASK1_DEMO_XLSX.exists() else RESULT_DIR / "task1_result_nodb.xlsx"
                demo_items = json.loads(demo_json.read_text(encoding="utf-8"))
                result = {
                    "ok": True,
                    "stats": {"total": len(demo_items)},
                    "logs": ["默认 PDF 目录在当前部署环境中不可访问，已展示随项目打包的任务一演示结果。"],
                    "items": demo_items,
                    "json_path": demo_json,
                    "xlsx_path": demo_xlsx,
                }
            else:
                result = {
                    "ok": False,
                    "stats": {"total": 0},
                    "logs": [f"默认 PDF 目录不可访问：{pdf_dir}"],
                    "items": [],
                    "error": "未找到 PDF 数据，也未找到内置任务一演示结果。",
                }
        st.session_state["task1_result"] = result

    result = st.session_state.get("task1_result")
    if result:
        stats = result.get("stats", {})
        st.metric("总文件数", stats.get("total", 0))

        if result.get("ok"):
            st.success("任务一执行完成。")
        else:
            st.error(result.get("error", "任务一未完成，请查看运行日志。"))

        with st.expander("运行日志", expanded=False):
            st.text("\n".join(result.get("logs", [])))

        if not use_db:
            st.markdown("### 结果文件下载")
            col1, col2 = st.columns(2)
            with col1:
                file_download_button("下载 task1_result_nodb.xlsx", Path(result.get("xlsx_path", RESULT_DIR / "task1_result_nodb.xlsx")), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with col2:
                file_download_button("下载 task1_result_nodb.json", Path(result.get("json_path", RESULT_DIR / "task1_result_nodb.json")), "application/json")


def render_task2_demo_page():
    st.title("智能问数")
    st.caption("部署演示模式：不连接 SQL Server，读取已生成的 result_2.json/xlsx 和图表文件进行展示。")

    answer_data = load_json_file(TASK2_RESULT_JSON)
    demo_items = iter_answer_items(answer_data)
    if not demo_items:
        st.warning("未找到 result_2.json，暂无法展示任务二演示结果。")
        return

    sample_questions = [item.get("Q", "") for item in demo_items if item.get("Q")]
    selected = st.selectbox("选择一个已生成的问题", sample_questions)
    question = st.text_area("也可以输入问题；无数据库模式会优先匹配已生成结果", value=selected)

    if st.button("提交问题", type="primary", use_container_width=True):
        answer_item = find_demo_answer(question, answer_data)
        if answer_item:
            st.session_state.setdefault("task2_demo_history", []).append(answer_item)
        else:
            st.info("部署演示模式只展示已生成的真实结果。未匹配到该问题，请从示例问题中选择。")

    history = st.session_state.get("task2_demo_history", [])
    if not history and demo_items:
        history = [demo_items[0]]

    st.markdown("### 演示结果")
    for item in history:
        with st.container(border=True):
            st.markdown(f"**问题编号：** {item.get('id', '')}")
            st.markdown(f"**你：** {item.get('Q', '')}")
            st.markdown("**助手：**")
            render_answer_block(item.get("A", {}))

    st.markdown("### 结果文件下载")
    col1, col2 = st.columns(2)
    with col1:
        file_download_button(
            "下载 result_2.xlsx",
            Path(TASK2_RESULT_XLSX),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with col2:
        file_download_button("下载 result_2.json", Path(TASK2_RESULT_JSON), "application/json")


def render_task2_page(db_config: Dict[str, Any], use_db: bool):
    if not use_db:
        render_task2_demo_page()
        return

    st.title("智能问数")
    st.caption("沿用任务二问数逻辑，支持上下文对话、SQL 展示和图表回显。")

    attachment1 = Path(st.text_input("公司信息附件路径", value=str(DEFAULT_ATTACHMENT1)))
    result_dir = Path(st.text_input("图表输出目录", value=str(TASK2_RESULT_DIR)))
    result_dir.mkdir(parents=True, exist_ok=True)

    assistant = get_task2_assistant(attachment1, db_config, result_dir)

    sample_questions = [
        "金花股份2025年第三季度利润总额是多少",
        "金花股份利润总额是多少",
        "2025年第三季度的",
        "金花股份近几年的利润总额变化趋势是什么样的",
    ]
    with st.expander("示例问题", expanded=False):
        for q in sample_questions:
            st.code(q, language="text")

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("重置上下文", use_container_width=True):
            st.session_state["task2_state"] = {}
            st.session_state["task2_history"] = []
            st.success("上下文已清空。")
    with col2:
        if st.button("清空对话记录", use_container_width=True):
            st.session_state["task2_history"] = []
            st.success("对话记录已清空。")

    with st.form("task2_form", clear_on_submit=True):
        question = st.text_area(
            "请输入自然语言问题",
            placeholder="例如：金花股份2025年第三季度利润总额是多少",
        )
        submitted = st.form_submit_button("提交问题", type="primary", use_container_width=True)

    if submitted and question.strip():
        try:
            state = st.session_state.get("task2_state", {})
            question_id = assistant.next_chat_question_id()
            answer_item, sql, images, chart_type = assistant.handle_turn(question_id, state, question.strip())
            st.session_state["task2_state"] = state
            st.session_state.setdefault("task2_history", []).append(
                {
                    "question": question.strip(),
                    "answer_item": answer_item,
                    "sql": sql,
                    "images": images,
                    "chart_type": chart_type,
                }
            )
        except Exception as exc:
            st.error(f"任务二执行失败：{exc}")

    history: List[Dict[str, Any]] = st.session_state.get("task2_history", [])
    if history:
        st.markdown("### 对话记录")
        for item in history:
            with st.container(border=True):
                st.markdown(f"**你：** {item['question']}")
                st.markdown("**助手：**")
                render_answer_block(item["answer_item"]["A"])
                if item.get("sql"):
                    with st.expander("SQL", expanded=False):
                        st.code(item["sql"], language="sql")
                if item.get("chart_type") and item["chart_type"] != "无":
                    st.caption(f"图表类型：{item['chart_type']}")

    st.markdown("### 结果文件下载")
    col1, col2 = st.columns(2)
    with col1:
        file_download_button(
            "下载 result_2.xlsx",
            Path(TASK2_RESULT_XLSX),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with col2:
        file_download_button("下载 result_2.json", Path(TASK2_RESULT_JSON), "application/json")


def render_task3_demo_page():
    st.title("可靠性增强分析")
    st.caption("部署演示模式：不连接 SQL Server，展示已生成的 result_3.json/xlsx、参考图片和证据链。")

    result_data = load_json_file(TASK3_RESULT_JSON)
    if not result_data:
        st.warning("未找到 result_3.json，暂无法展示任务三演示结果。")
        return

    st.success("任务三演示结果已加载。")
    st.markdown("### 结果摘要")
    for qid, answers in result_data.items():
        with st.container(border=True):
            st.markdown(f"### {qid}")
            if isinstance(answers, list):
                for answer in answers:
                    if isinstance(answer, dict):
                        st.write(answer.get("Q", ""))
                        render_answer_block(answer.get("A", {}))

    st.markdown("### 结果文件下载")
    col1, col2 = st.columns(2)
    with col1:
        file_download_button(
            "下载 result_3.xlsx",
            Path(TASK3_RESULT_XLSX),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with col2:
        file_download_button("下载 result_3.json", Path(TASK3_RESULT_JSON), "application/json")


def render_task3_page(db_config: Dict[str, Any], use_db: bool):
    if not use_db:
        render_task3_demo_page()
        return

    st.title("可靠性增强分析")
    st.caption("沿用任务三批处理逻辑，生成 result_3.xlsx 并展示知识库证据链。")

    result_dir = Path(st.text_input("任务三结果目录", value=str(RESULT_DIR), key="task3_result_dir"))
    output_path = Path(st.text_input("任务三输出文件", value=str(TASK3_RESULT_XLSX), key="task3_output"))
    result_dir.mkdir(parents=True, exist_ok=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        run = st.button("生成任务三结果", type="primary", use_container_width=True)
    with col2:
        clear = st.button("清空本页结果", use_container_width=True)

    if clear:
        st.session_state.pop("task3_result", None)
        st.rerun()

    if run:
        with st.spinner("正在执行任务三，请稍候..."):
            result = run_task3_generation(db_config, result_dir, output_path)
        st.session_state["task3_result"] = result

    result = st.session_state.get("task3_result")
    if result:
        if result.get("ok"):
            st.success("任务三执行完成。")
        else:
            st.error(result.get("error", "任务三执行失败。"))

        with st.expander("运行日志", expanded=True):
            st.text("\n".join(result.get("logs", [])))

        st.markdown("### 结果摘要")
        for item in result.get("items", []):
            with st.container(border=True):
                st.markdown(f"### {item['id']}")
                st.write(item["question"])
                if item.get("sql_list"):
                    with st.expander("SQL", expanded=False):
                        for sql in item["sql_list"]:
                            st.code(sql, language="sql")
                for answer in item.get("answers", []):
                    render_answer_block(answer["A"])

        st.markdown("### 结果文件下载")
        col1, col2 = st.columns(2)
        with col1:
            file_download_button(
                "下载 result_3.xlsx",
                Path(result.get("output_path", output_path)),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with col2:
            file_download_button(
                "下载 result_3.json",
                Path(result.get("json_path", TASK3_RESULT_JSON)),
                "application/json",
            )


def main():
    mode, db_config = build_runtime_config()
    use_db = mode == "db"

    with st.sidebar:
        st.divider()
        page = st.radio(
            "页面导航",
            ["首页", "财报入库", "智能问数", "可靠性增强分析"],
            index=0,
        )
        st.caption("建议演示时按 首页 → 财报入库 → 智能问数 → 可靠性增强分析 的顺序进行。")

    if page == "首页":
        render_home_page(mode)
    elif page == "财报入库":
        render_task1_page(db_config, use_db)
    elif page == "智能问数":
        render_task2_page(db_config, use_db)
    else:
        render_task3_page(db_config, use_db)


if __name__ == "__main__":
    main()
