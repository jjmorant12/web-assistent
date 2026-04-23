from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent


def _env_path(name: str):
    value = os.getenv(name)
    if not value:
        return None
    return Path(value).expanduser()


SAMPLE_DATA_ROOT = PROJECT_ROOT / "泰迪杯B题" / "B题-示例数据" / "示例数据"
FORMAL_DATA_ROOT = PROJECT_ROOT / "泰迪杯B题" / "全部数据" / "正式数据"
DATA_ROOT = _env_path("TAIDIBEI_DATA_ROOT") or SAMPLE_DATA_ROOT


def _resolve_path(*candidates: str) -> Path:
    for candidate in candidates:
        if not candidate:
            continue
        path = DATA_ROOT / candidate
        if path.exists():
            return path
    return DATA_ROOT / candidates[0]


ATTACHMENT1_PATH = _resolve_path(
    "附件1：中药上市公司基本信息（截至到2025年12月22日）.xlsx",
)

ATTACHMENT2_REPORTS_ROOT = _resolve_path(
    "附件2：财务报告",
)

ATTACHMENT4_PATH = _resolve_path(
    "附件4：问题汇总.xlsx",
)

ATTACHMENT5_ROOT = _resolve_path(
    "附件5：研报数据",
)

ATTACHMENT5_STOCK_INFO_PATH = ATTACHMENT5_ROOT / "个股_研报信息.xlsx"
ATTACHMENT5_INDUSTRY_INFO_PATH = ATTACHMENT5_ROOT / "行业_研报信息.xlsx"
ATTACHMENT5_PDF_ROOT = ATTACHMENT5_ROOT
ATTACHMENT5_STOCK_REPORT_DIR = ATTACHMENT5_ROOT / "个股研报"
ATTACHMENT5_INDUSTRY_REPORT_DIR = ATTACHMENT5_ROOT / "行业研报"

ATTACHMENT6_PATH = _resolve_path(
    "附件6：问题汇总.xlsx",
)
