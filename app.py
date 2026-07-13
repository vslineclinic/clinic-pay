"""
병원 정산 3-Way 차이 추적기 (채널 합계 대사 중심)

목표 (재정의):
  ★ 결제채널별(카드/현금+이체/플랫폼) 3개 파일 합계 차이 산출
  ★ 차이금액을 설명할 후보 환자·거래 추적 (잘못 기입/누락 즉시 수정)

원칙:
  - 채널 합계 차이 → 의심 후보(소수) → AI 한 줄 진단 흐름
  - 1:1 매칭은 후보 식별 도구로만 사용 (메인 산출물 아님)
  - AI 입력: 차트번호·승인번호로 3개 파일을 join한 통합 raw 구조 (≤ ~8KB / ~3K토큰)
    → AI가 단순 비교가 아닌 cross-file 추적 분석 수행
  - 출력 ≤ 900토큰 / Gemini 무료 한도(2.5-flash-lite: 250K TPM) 내
"""

import importlib
import io
import re
import unicodedata
from datetime import datetime
from itertools import combinations

import pandas as pd
import streamlit as st


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 지점별 일일마감 구글시트 연동 설정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 일일마감을 파일 업로드 대신 구글 스프레드시트에서 바로 읽어오기 위한 설정.
#
# [사전 준비]
#   1) 각 지점의 일일마감 구글 스프레드시트를 열고 [공유] →
#      "링크가 있는 모든 사용자"를 "뷰어"로 설정 (별도 인증키 불필요).
#   2) 아래 CLINIC_DAILY_SHEETS 에 {지점명: 스프레드시트 URL(또는 ID)} 를 입력.
#   3) 각 스프레드시트의 워크시트(탭) 이름은 날짜여야 함 (기본 형식: 26.06.02).
#
# URL을 코드(깃)에 남기기 싫으면, Streamlit Secrets 의 [clinic_daily_sheets]
# 섹션에 동일하게 적으면 그 값이 우선 적용된다. 예) .streamlit/secrets.toml
#   [clinic_daily_sheets]
#   "강남점" = "https://docs.google.com/spreadsheets/d/XXXX/edit"
#   "분당점" = "https://docs.google.com/spreadsheets/d/YYYY/edit"

# 시트 탭(워크시트) 이름의 날짜 형식. 예) 2026-06-02 → "26.06.02"
DAILY_SHEET_DATE_FMT = "%y.%m.%d"

# 차트마감(베가스)↔일일마감 교차검증 임계값.
# 두 파일의 '차트번호' 일치율이 이 값 미만이면 다른 지점/다른 날짜 파일로 보고
# 합계·통계를 일절 표시하지 않는다(타 지점 데이터 엿보기 방지).
# 같은 지점·같은 날짜면 보통 0.9 이상, 다른 지점/날짜면 0에 가깝다 → 0.6이면 안전.
CROSS_CHECK_MIN_RATE = 0.6

# 지점명 → 스프레드시트 URL(또는 ID). 사용하는 지점만 채우면 된다(빈 값은 목록에서 숨김).
# 탭 이름 형식은 지점·시기마다 달라도 자동 매칭한다(26.06.02 / 06.02 / 6.2 / 26.6.2 …).
# ※ 정확한 분석을 위해 각 날짜 탭은 표준 양식(templates/일일마감_표준양식 참고)으로 입력할 것.
#    표준 양식이 아닌 탭은 합계가 실제와 다를 수 있고, 앱에서 경고가 표시된다.
#
# 지점이 스프레드시트를 새 파일로 옮긴 경우(이전 날짜는 옛 시트, 이후는 새 시트),
# 값에 URL 대신 [(적용 시작일 "YYYY-MM-DD", URL), ...] 목록을 적는다.
# 분석 날짜가 시작일 이후(당일 포함)인 첫 항목의 URL이 쓰이므로 **최신 시작일을 위에**,
# 마지막 항목의 시작일을 ""(빈 문자열)로 두면 그 이전 전체 날짜의 기본 시트가 된다.
# (Secrets로 줄 때도 동일: "엔디어트" = [["2026-06-12", "URL"], ["", "URL"]])
CLINIC_DAILY_SHEETS = {
    "인천점": "https://docs.google.com/spreadsheets/d/1FJwllsTCVbmtorRr_0XcYMCQ49yym2aXHj8pdrP9inU/edit",
    "잠실점": "https://docs.google.com/spreadsheets/d/18wHlyD85V-KrTortCt7u4JAbFOn08WJtJJGT1oAm5kA/edit",
    # 엔디어트는 26.06.12부터 새 스프레드시트 사용, 그 이전 날짜는 기존 시트에서 읽는다.
    "엔디어트": [
        ("2026-06-12", "https://docs.google.com/spreadsheets/d/1e1BMgUAF_GGAAJBib8gVAEVoJC6ZjunQ3K399tdc_mA/edit"),
        ("", "https://docs.google.com/spreadsheets/d/1YdruwnAghZARwLALGD9rvbDZsnw4b4FKSt6l3_sLgd4/edit"),
    ],
    "강남점": "https://docs.google.com/spreadsheets/d/1eNEp8zo27whawGPrb5y7uifZzhXVKpsp5hgpTlDv0UY/edit",
    "일산점": "https://docs.google.com/spreadsheets/d/1Z--Ps4mds67l95g4V2AKW5y-RjESFxDNct18CgSaK8c/edit",
    "압구정": "https://docs.google.com/spreadsheets/d/1cum7KVfY1TIkXKBjkIpd5GldAT6dd48if-rn03wce0U/edit",
}


def get_clinic_daily_sheets():
    """지점→스프레드시트 매핑을 반환. Streamlit Secrets에 설정이 있으면 우선 적용."""
    try:
        if "clinic_daily_sheets" in st.secrets:
            sec = dict(st.secrets["clinic_daily_sheets"])
            if sec:
                return sec
    except Exception:
        pass
    return dict(CLINIC_DAILY_SHEETS)


def sheet_entry_configured(entry):
    """지점 시트 설정값(단일 URL 또는 [(시작일, URL), ...])에 URL이 하나라도 있는지."""
    if isinstance(entry, (list, tuple)):
        return any(len(it) > 1 and str(it[1]).strip() for it in entry)
    return bool(str(entry or "").strip())


def resolve_daily_sheet_url(entry, picked_date):
    """지점 시트 설정값에서 분석 날짜(picked_date, date)에 적용할 URL/ID를 고른다.

    단일 URL(문자열)이면 그대로 반환. [(시작일 "YYYY-MM-DD", URL), ...] 목록이면
    시작일 내림차순으로 보아 picked_date가 시작일 이후(당일 포함)인 첫 항목의 URL을
    반환한다(빈 시작일 = 그 이전 전체의 기본값). 날짜 비교는 ISO 문자열 순서를 그대로
    이용하므로 시작일은 반드시 0패딩된 YYYY-MM-DD 형식이어야 한다.
    """
    if not isinstance(entry, (list, tuple)):
        return entry
    items = sorted(
        (it for it in entry if len(it) > 1 and str(it[1]).strip()),
        key=lambda it: str(it[0] or "").strip(), reverse=True,
    )
    day = picked_date.isoformat() if picked_date is not None else ""
    for start, url in ((str(it[0] or "").strip(), it[1]) for it in items):
        if not start or day >= start:
            return url
    # 모든 항목에 시작일이 있는데 picked_date가 그보다 이전 → 가장 오래된 시트로.
    return items[-1][1] if items else None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 유틸리티
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def clean_money(x):
    if pd.isna(x):
        return 0
    try:
        return int(float(str(x).replace(",", "").replace("₩", "").replace(" ", "")))
    except Exception:
        return 0


def clean_no(x):
    if pd.isna(x) or str(x).strip() in ("", "nan", "NaN"):
        return ""
    # NFKC: 전각숫자(１５６０…) → 반각. 차트 결제메모에 승인번호를 전각으로 입력한
    # 경우 한솔 승인번호(반각)와 문자열이 달라 매칭이 조용히 실패하던 문제 방지.
    s = unicodedata.normalize("NFKC", str(x))
    return re.sub(r"\D", "", s.split(".")[0])


def _appr_key(x):
    """승인번호 매칭용 정규화 키: clean_no 후 '선행 0' 제거.

    차트(베가스 EMR) 결제메모는 승인번호를 보통 8자리로 0패딩해 적지만(예: 873971→
    "00873971", 8272602→"08272602"), 한솔페이는 선행 0이 없는 자연수("873971")로 적는다.
    선행 0을 제거하지 않으면 같은 거래인데도 문자열이 달라 승인번호 매칭이 조용히 실패하고,
    그 미매칭 잔여 건이 금액이 비슷한 '남의 거래'와 오타 의심 쌍으로 잘못 묶인다(허위 오류 —
    예: 홍규진 차트 409,000이 김준영의 한솔 419,000과 '한 자리 오타'로 오특정).
    한솔 승인번호는 선행 0이 없어 이 정규화가 무해(no-op)하므로 양쪽을 같은 키로 맞춘다.
    승인번호는 그 자체가 정수 식별자라 선행 0은 자릿수 표기 차이일 뿐 의미가 없다.
    (차트번호·카드번호는 선행 0이 의미를 가질 수 있어 이 키를 쓰지 않는다.)
    """
    s = clean_no(x)
    return s.lstrip("0") or ("0" if s else "")


def clean_name(x):
    if pd.isna(x):
        return ""
    return re.sub(r"[\s\-\*]", "", str(x)).strip()


def _parse_clock(val):
    """시간 문자열 → (분, 'HH:MM:SS').

    'HH:MM[:SS]' 콜론형 · 'HHMMSS'/'HHMM' 숫자형 · 'YYYY-MM-DD HH:MM:SS' 날짜+시간
    혼용을 모두 정확히 처리한다. (기존 zfill(6)+앞자리 슬라이스 방식은 날짜가 붙거나
    초가 없는 'HH:MM' 형식에서 시각을 잘못 읽던 문제가 있었음)
    """
    if pd.isna(val):
        return 0, ""
    s = str(val).strip()
    # 1) 콜론 형식 우선 — 날짜가 앞에 와도 마지막 시:분[:초] 매치를 시각으로 사용
    matches = re.findall(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", s)
    if matches:
        hh, mm, ss = matches[-1]
        hh, mm, ss = int(hh), int(mm), int(ss) if ss else 0
    else:
        # 엑셀이 시간을 숫자로 저장한 '1430.0' 같은 형식은 소수부를 버리고 정수부만 사용
        d = re.sub(r"\D", "", s.split(".")[0])
        if not d:
            return 0, ""
        if len(d) > 6:        # 날짜+HHMMSS → 뒤 6자리만 시각
            d = d[-6:]
        if len(d) <= 4:       # HHMM → 초는 00
            d = d.zfill(4) + "00"
        else:                 # HHMMSS (5자리는 앞 0 보정)
            d = d.zfill(6)
        hh, mm, ss = int(d[:2]), int(d[2:4]), int(d[4:6])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return 0, ""
    ss = ss if 0 <= ss <= 59 else 0
    return hh * 60 + mm, f"{hh:02d}:{mm:02d}:{ss:02d}"


def norm_date(val):
    """날짜 값을 'YYYY-MM-DD'로 정규화. 실패하면 "".

    차트마감 수납일 '2026-06-10(수)', 한솔 거래일 '260610'(YYMMDD 숫자),
    '2026.06.10' / '26-06-10' / datetime 혼용을 모두 처리한다.
    """
    if pd.isna(val):
        return ""
    if isinstance(val, (pd.Timestamp, datetime)):
        return f"{val:%Y-%m-%d}"
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        # YYMMDD 6자리 (엑셀이 숫자로 저장한 '260610.0' 포함)
        m = re.search(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)", s.split(".")[0])
        if not m:
            # YY.MM.DD 구분자형
            m = re.search(r"(?<!\d)(\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})(?!\d)", s)
        if not m:
            return ""
        y, mo, d = 2000 + int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return ""
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _extract_card_company(pay_str):
    """결제수단 문자열에서 카드사명 추출 ('카드-삼성카드' → '삼성')"""
    if pd.isna(pay_str):
        return ""
    s = str(pay_str).strip()
    m = re.match(r"카드[\s\-\:\(\[]*(.+?)[\)\]\s]*$", s)
    if not m:
        return ""
    name = m.group(1).strip()
    name = re.sub(r"카드$", "", name).strip()
    return name


def _norm_card_company(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"카드$", "", s)
    s = re.sub(r"\s+", "", s)
    return s.lower()


def card_company_match(a, b):
    """카드사명 완전/포함 매칭(예: '현대', '현대카드')."""
    na, nb = _norm_card_company(a), _norm_card_company(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def _read_excel_auto(buf, **kwargs):
    """Try openpyxl first (.xlsx), then xlrd (.xls 97-2003), then calamine (.xlsb/.xls)."""
    try:
        return pd.read_excel(buf, engine="openpyxl", **kwargs)
    except Exception:
        pass
    if hasattr(buf, "seek"):
        buf.seek(0)
    try:
        return pd.read_excel(buf, engine="xlrd", **kwargs)
    except Exception:
        pass
    if hasattr(buf, "seek"):
        buf.seek(0)
    # calamine 엔진 시도 (xlsb, 일부 xls/xlsx 호환)
    try:
        return pd.read_excel(buf, engine="calamine", **kwargs)
    except Exception:
        pass
    if hasattr(buf, "seek"):
        buf.seek(0)
    return pd.read_excel(buf, **kwargs)


def _try_read_as_html(raw_bytes):
    """xls/xlsx 확장자이지만 실제로는 HTML 테이블인 파일을 읽는다."""
    head = raw_bytes[:1024]
    # BOM 제거
    for bom in (b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff"):
        if head.startswith(bom):
            head = head[len(bom):]
            break
    head_str = head.decode("utf-8", errors="ignore").strip().lower()
    if not any(tag in head_str for tag in ("<html", "<table", "<tr", "<!doctype")):
        return None
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            text = raw_bytes.decode(enc)
            tables = pd.read_html(io.StringIO(text), header=None)
            if tables:
                return tables[0]
        except Exception:
            continue
    return None


def _try_read_as_csv(raw_bytes):
    """xls/xlsx 확장자이지만 실제로는 CSV/TSV인 파일을 읽는다."""
    head = raw_bytes[:512]
    # ZIP(xlsx) 또는 OLE2(xls) 시그니처가 있으면 CSV가 아님
    if head.startswith(b"PK") or head.startswith(b"\xd0\xcf\x11\xe0"):
        return None
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            text = raw_bytes.decode(enc)
            first_lines = text.strip().split("\n")[:5]
            if not first_lines:
                return None
            # 탭 또는 콤마 구분 탐지
            for sep in (",", "\t"):
                counts = [line.count(sep) for line in first_lines if line.strip()]
                if counts and min(counts) >= 1:
                    return pd.read_csv(io.StringIO(text), sep=sep, header=None, encoding=enc)
        except Exception:
            continue
    return None


def load_file(f, password=None, default_password="vsline99!!"):
    fname = f.name.lower()
    if fname.endswith(".csv"):
        try:
            return pd.read_csv(f, encoding="utf-8")
        except UnicodeDecodeError:
            f.seek(0)
            return pd.read_csv(f, encoding="cp949")

    raw = f.read()
    f.seek(0)

    last_error = None
    user_pw = password.strip() if isinstance(password, str) and password.strip() else None

    # 파일이 암호화되어 있는지 감지
    def _is_encrypted():
        """msoffcrypto로 파일 암호화 여부를 확인한다."""
        if importlib.util.find_spec("msoffcrypto") is None:
            return False
        try:
            ms = importlib.import_module("msoffcrypto")
            office = ms.OfficeFile(io.BytesIO(raw))
            return office.is_encrypted()
        except Exception:
            return False

    def _try_decrypt(pw):
        """msoffcrypto 복호화 시도 후 엑셀 읽기"""
        if importlib.util.find_spec("msoffcrypto") is None:
            raise ValueError("암호화된 엑셀 처리를 위해 msoffcrypto-tool 설치가 필요합니다.")
        ms = importlib.import_module("msoffcrypto")
        office = ms.OfficeFile(io.BytesIO(raw))
        office.load_key(password=pw)
        decrypted = io.BytesIO()
        office.decrypt(decrypted)
        decrypted.seek(0)
        return _read_excel_auto(decrypted, header=None)

    encrypted = _is_encrypted()

    # 1단계: 사용자가 비밀번호를 입력한 경우 → 복호화 시도
    if user_pw is not None:
        try:
            return _try_decrypt(user_pw)
        except Exception as e:
            last_error = e

    # 2단계: 비암호화 파일 직접 읽기 (.xlsx / .xls / .xlsb)
    try:
        return _read_excel_auto(io.BytesIO(raw), header=None)
    except Exception as e:
        last_error = e

    # 3단계: 암호화된 파일인 경우에만 기본 비밀번호로 복호화 시도
    if encrypted:
        if user_pw != default_password:
            try:
                return _try_decrypt(default_password)
            except Exception as e:
                last_error = e

        # 4단계: 추가 기본 비밀번호들 시도
        extra_passwords = ["1234", "0000", "1111", "password"]
        for pw in extra_passwords:
            if pw == user_pw or pw == default_password:
                continue
            try:
                return _try_decrypt(pw)
            except Exception:
                continue

    # 5단계: 확장자는 xls/xlsx이지만 실제로 HTML 테이블인 경우
    result = _try_read_as_html(raw)
    if result is not None:
        return result

    # 6단계: 확장자는 xls/xlsx이지만 실제로 CSV/TSV인 경우
    result = _try_read_as_csv(raw)
    if result is not None:
        return result

    # 7단계: 마지막으로 다양한 인코딩으로 CSV 재시도 (확장자 무관)
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-16"):
        try:
            text = raw.decode(enc)
            for sep in (",", "\t", "|"):
                try:
                    df = pd.read_csv(io.StringIO(text), sep=sep, header=None, encoding=enc)
                    if len(df.columns) >= 2 and len(df) >= 2:
                        return df
                except Exception:
                    continue
        except Exception:
            continue

    if encrypted:
        raise ValueError(f"암호화된 파일입니다. 올바른 비밀번호를 입력해 주세요. ({last_error})")
    else:
        raise ValueError(f"지원하지 않는 파일 형식입니다. 엑셀(.xlsx, .xls, .xlsb) 또는 CSV 파일을 업로드해 주세요. ({last_error})")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 구글시트 연동 (일일마감)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _extract_sheet_id(url_or_id):
    """구글 스프레드시트 URL 또는 ID 문자열에서 스프레드시트 ID를 추출."""
    if not url_or_id:
        return None
    s = str(url_or_id).strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", s)
    if m:
        return m.group(1)
    # URL이 아니라 ID만 들어온 경우 (구글 ID는 보통 영숫자/_/- 로 20자 이상)
    if re.fullmatch(r"[a-zA-Z0-9_-]{20,}", s):
        return s
    return None


def _fetch_url_bytes(url, timeout=20):
    """URL 본문을 bytes로 가져온다. 접근/네트워크 오류는 명확한 예외로 변환."""
    import urllib.error
    import urllib.request

    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (compatible; clinic-pay)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise PermissionError(
                "구글시트 접근 권한이 없습니다. 스프레드시트 공유를 "
                "'링크가 있는 모든 사용자 - 뷰어'로 설정했는지 확인하세요."
            ) from e
        if e.code == 404:
            raise LookupError("스프레드시트를 찾을 수 없습니다. URL을 확인하세요.") from e
        raise
    except urllib.error.URLError as e:
        raise ConnectionError(
            f"네트워크 오류로 구글시트를 불러오지 못했습니다: {getattr(e, 'reason', e)}"
        ) from e


def _gviz_csv_url(sid, sheet_name):
    """특정 워크시트(탭)를 CSV로 내보내는 gviz URL."""
    from urllib.parse import quote

    return (
        f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq"
        f"?tqx=out:csv&headers=0&sheet={quote(sheet_name, safe='')}"
    )


def _export_csv_url(sid, gid):
    """특정 워크시트(gid)를 CSV로 내보내는 'export' URL.

    gviz(out:csv)는 열 타입을 추론하면서 '숫자 열'의 텍스트 머리글(카드/현금/이체 등)을
    빈칸으로 떨어뜨린다(일산·강남 등 결제수단 머리글이 통째로 사라짐 → 금액열 라벨 소실).
    반면 이 export 엔드포인트는 셀 원본을 그대로 내보내므로 머리글이 보존된다.
    다만 탭을 '이름'이 아니라 'gid(숫자)'로만 지정할 수 있어 _sheet_gid_map이 필요하다.
    """
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"


# 스프레드시트 edit 페이지 부트스트랩에 들어 있는 {탭이름 ↔ gid} 인코딩.
# 형식: \"<gid>\",[{\"1\":[[0,0,\"<탭이름>\"]   (JSON이 한 번 더 escape된 형태)
_GID_NAME_RE = re.compile(r'\\"(\d{4,})\\",\[\{\\"1\\":\[\[0,0,\\"([^"\\]+)\\"')


def _sheet_gid_map(sid, timeout=20):
    """{탭이름: gid} 매핑을 edit 페이지에서 추출. 실패 시 빈 dict.

    export(머리글 보존) 경로는 gid가 필요한데, 공개(링크 보기) 시트에서 gid 목록을 얻는
    표준 무인증 API가 없어 edit 페이지 부트스트랩을 파싱한다. 구글이 내부 포맷을 바꾸면
    매핑이 비어 자동으로 gviz 경로로 폴백하므로(load_gsheet_daily 참고) 안전하다.
    """
    try:
        html = _fetch_url_bytes(
            f"https://docs.google.com/spreadsheets/d/{sid}/edit", timeout
        ).decode("utf-8", "ignore")
    except Exception:
        return {}
    out = {}
    for gid, name in _GID_NAME_RE.findall(html):
        out.setdefault(name.strip(), gid)  # 같은 이름이 여럿이면 첫 번째 채택
    return out


def _classify_gviz(raw_bytes):
    """gviz 응답 종류 판별 → ('csv', None) | ('error', msg) | ('html', None) | ('empty', None)."""
    head = raw_bytes[:2048].decode("utf-8", errors="ignore").lstrip()
    low = head.lower()
    # gviz 오류 봉투: /*O_o*/ google.visualization.Query.setResponse({... "status":"error" ...})
    if low.startswith("/*o_o*/") or "setresponse" in low:
        m = re.search(r'"message"\s*:\s*"([^"]*)"', head)
        return ("error", m.group(1) if m else "")
    # 로그인/권한 안내 HTML 페이지
    if low.startswith("<!doctype") or low.startswith("<html") or "<html" in low[:200]:
        return ("html", None)
    if not raw_bytes.strip():
        return ("empty", None)
    return ("csv", None)


def _gviz_csv_to_grid(raw_bytes):
    """gviz CSV(bytes) → raw DataFrame(header=None).

    일일마감 시트는 헤더 위 제목 행 때문에 행마다 열 수가 다를 수 있다(ragged).
    pd.read_csv는 첫 행 기준 열 수를 강제해 깨지므로, csv로 직접 읽어 최대 열 수에
    맞춰 패딩한 직사각형 그리드를 만든다. dtype=object로 고정해 엑셀 업로드 경로와
    동일한 형태를 유지한다(일부 pandas의 PyArrow string 추론으로 인한 합산 오류 방지).
    """
    text = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            text = raw_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("구글시트 CSV 응답을 디코딩하지 못했습니다.")
    import csv as _csv

    rows = list(_csv.reader(io.StringIO(text)))
    width = max((len(r) for r in rows), default=0)
    if width == 0:
        raise ValueError("구글시트 응답이 비어 있습니다. 시트에 데이터가 있는지 확인하세요.")
    grid = [r + [""] * (width - len(r)) for r in rows]
    return pd.DataFrame(grid, dtype=object)


def _parse_gviz_csv(raw_bytes, sheet_name=""):
    """단일 gviz 응답(bytes) → raw DataFrame. 오류/HTML/빈 응답은 사람이 읽는 예외로 변환."""
    kind, msg = _classify_gviz(raw_bytes)
    if kind == "error":
        low = (msg or "").lower()
        if not msg or any(k in low for k in ("invalid_query", "sheet", "not found", "범위")):
            raise LookupError(
                f"구글시트에서 '{sheet_name}' 탭을 찾을 수 없습니다. 날짜 탭 이름을 확인하세요."
                + (f" ({msg})" if msg else "")
            )
        raise PermissionError(
            "구글시트 조회 오류. 공유 설정 또는 시트 이름을 확인하세요." + (f" ({msg})" if msg else "")
        )
    if kind == "html":
        raise PermissionError(
            "구글시트에 접근할 수 없습니다. 스프레드시트 공유를 "
            "'링크가 있는 모든 사용자 - 뷰어'로 설정했는지 확인하세요."
        )
    if kind == "empty":
        raise ValueError("구글시트 응답이 비어 있습니다. 시트에 데이터가 있는지 확인하세요.")
    return _gviz_csv_to_grid(raw_bytes)


def _candidate_tab_names(d):
    """날짜 d(date)에 대해 지점별로 쓰일 법한 탭 이름 후보를 우선순위대로 생성.

    실측된 형식(지점·시기마다 다름):
      26.06.02(인천·잠실 최근) / 06.02(엔디어트 최근) / 6.2(엔디어트 과거) /
      26.6.2·2026.6.2(잠실 과거) / '26.06.02 일일마감'(인천 변형) / 6월2일 /
      '26년 06월 02일'(압구정).
    실제 어느 탭이 존재하는지는 load_gsheet_daily가 폴백 비교로 가려낸다.
    """
    y2, Y = d.strftime("%y"), d.strftime("%Y")
    mm, dd = f"{d.month:02d}", f"{d.day:02d}"
    m, dy = str(d.month), str(d.day)
    cands = [
        d.strftime(DAILY_SHEET_DATE_FMT),  # 26.06.02
        f"{mm}.{dd}",                      # 06.02
        f"{m}.{dy}",                       # 6.2
        f"{y2}.{m}.{dy}",                  # 26.6.2
        f"{Y}.{mm}.{dd}",                  # 2026.06.02
        f"{Y}.{m}.{dy}",                   # 2026.6.2
        f"{y2}.{mm}.{dd} 일일마감",         # 인천 '25.11.04 일일마감' 변형
        f"{y2}년 {mm}월 {dd}일",            # 압구정 '26년 06월 02일'
        f"{m}월{dy}일",                     # 6월2일
    ]
    seen, out = set(), []
    for c in cands:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def load_gsheet_daily(url_or_id, picked_date, timeout=20, cache=None):
    """지점 스프레드시트 URL/ID + 날짜(date) → (raw DataFrame, 매칭된 탭 이름).

    url_or_id에는 단일 URL/ID 외에 CLINIC_DAILY_SHEETS의 [(시작일, URL), ...] 목록도
    그대로 줄 수 있다 — 시트를 새 파일로 이전한 지점은 picked_date에 맞는 시트가
    자동 선택된다(resolve_daily_sheet_url). 기간 분석처럼 날짜마다 호출돼도 시트별
    캐시 키(sid 기준)가 분리돼 있어 두 시트가 섞이지 않는다.

    탭 이름 형식이 지점마다 달라 _candidate_tab_names의 여러 후보를 순서대로 시도한다.

    1순위 export(format=csv&gid=): 셀 원본을 그대로 받아 결제수단 머리글(카드/현금/이체
      등)이 보존된다. gviz는 '숫자 열'의 텍스트 머리글을 타입 추론 중 떨궈(일산·강남 등)
      금액열 라벨이 통째로 사라지는 문제가 있다. export는 탭을 gid로만 지정할 수 있어
      _sheet_gid_map으로 {탭이름: gid}을 한 번 얻어(캐싱) 후보와 정확히 일치하는 탭을 고른다.
    2순위 gviz(out:csv) 폴백: gid 매핑을 못 얻거나(구글 포맷 변경) 후보가 매핑에 없을 때.
      gviz는 탭을 못 찾으면 오류 대신 '첫 시트'를 반환하므로, 존재할 수 없는 이름(sentinel)
      응답과 비교해 폴백(=해당 날짜 탭 없음)을 가려낸다 — 잘못된 시트를 조용히 읽는 사고 방지.
    cache(dict)를 주면 지점별 gid 매핑·폴백 시그니처·직전 성공 형식을 재사용해 호출을 줄인다.
    """
    import hashlib

    sid = _extract_sheet_id(resolve_daily_sheet_url(url_or_id, picked_date))
    if not sid:
        raise ValueError(
            "스프레드시트 URL/ID를 인식할 수 없습니다. 올바른 구글시트 링크인지 확인하세요."
        )
    if cache is None:
        cache = {}

    cands = _candidate_tab_names(picked_date)

    def _permission_html():
        return PermissionError(
            "구글시트에 접근할 수 없습니다. 스프레드시트 공유를 "
            "'링크가 있는 모든 사용자 - 뷰어'로 설정했는지 확인하세요."
        )

    # ── 1순위: export(gid) — 머리글 보존 ───────────────────────────────
    gm_key = f"gidmap::{sid}"
    if gm_key not in cache:
        cache[gm_key] = _sheet_gid_map(sid, timeout)
    gid_map = cache[gm_key]
    for name in cands:
        gid = gid_map.get(name)
        if gid is None:
            continue
        try:
            b = _fetch_url_bytes(_export_csv_url(sid, gid), timeout)
        except PermissionError:
            raise
        except Exception:
            break  # 일시적 네트워크 등 → gviz 폴백으로
        kind = _classify_gviz(b)[0]
        if kind == "html":
            raise _permission_html()
        if kind == "csv":
            return _gviz_csv_to_grid(b), name

    # ── 2순위(폴백): gviz(out:csv) + sentinel 폴백 탐지 ─────────────────
    def _sig(b):
        return hashlib.md5(b).hexdigest()

    # 폴백(첫 시트) 시그니처 확보 — 지점별 1회 (존재할 수 없는 이름으로 조회)
    fb_key = f"fb::{sid}"
    if fb_key in cache:
        fb_sig = cache[fb_key]
    else:
        fb_sig = None
        try:
            sb = _fetch_url_bytes(_gviz_csv_url(sid, "__no_such_tab__zz_9z9z9z"), timeout)
            if _classify_gviz(sb)[0] == "csv":
                fb_sig = _sig(sb)
        except Exception:
            fb_sig = None
        cache[fb_key] = fb_sig

    # 후보 형식 시도 (직전에 성공한 형식 인덱스를 먼저)
    order = list(range(len(cands)))
    pref = cache.get(f"idx::{sid}")
    if isinstance(pref, int) and 0 <= pref < len(cands):
        order = [pref] + [i for i in order if i != pref]

    for i in order:
        name = cands[i]
        # 권한/네트워크/404는 형식 문제가 아니므로 그대로 전달(다음 후보 무의미)
        b = _fetch_url_bytes(_gviz_csv_url(sid, name), timeout)
        kind = _classify_gviz(b)[0]
        if kind == "html":
            raise _permission_html()
        if kind != "csv":
            continue
        if fb_sig is not None and _sig(b) == fb_sig:
            continue  # 폴백(첫 시트) → 이 이름의 탭은 존재하지 않음
        cache[f"idx::{sid}"] = i
        return _gviz_csv_to_grid(b), name

    raise LookupError(
        f"{picked_date.strftime('%Y-%m-%d')} 날짜의 시트 탭을 찾을 수 없습니다. "
        f"시도한 이름: {' / '.join(cands)}. "
        "해당 날짜 탭이 있는지, 탭 이름 형식이 맞는지 확인하세요."
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 파서
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def parse_hansol(raw):
    """한솔페이 파싱: 헤더 자동탐지, 시간 파싱, 거절/취소 분류"""
    # CSV처럼 이미 컬럼 헤더가 있는 경우를 우선 처리
    raw.columns = [str(c).strip().replace("\n", "") for c in raw.columns]
    has_header = any(c in raw.columns for c in ["금액", "거래금액", "결제금액"])

    if has_header:
        df = raw.copy().reset_index(drop=True)
    else:
        hdr = 0
        for i, row in raw.iterrows():
            if row.astype(str).str.contains("금액|승인번호|카드번호", na=False).any():
                hdr = i
                break
        df = raw.iloc[hdr + 1:].copy()
        df.columns = [str(c).strip().replace("\n", "") for c in raw.iloc[hdr]]
        df = df.reset_index(drop=True)

    amount_col = next((c for c in ["금액", "거래금액", "결제금액"] if c in df.columns), None)
    if amount_col is None:
        st.error(f"한솔페이 파일에서 금액 컬럼을 찾을 수 없습니다. (현재 컬럼: {', '.join(map(str, df.columns))})")
        return pd.DataFrame()

    if amount_col != "금액":
        df["금액"] = df[amount_col]

    df["금액"] = df["금액"].apply(clean_money)
    df = df[df["금액"] > 0].copy()

    if "승인번호" in df.columns:
        # _appr_key: 차트(0패딩)와 같은 키 공간으로 정규화(선행 0 제거). 한솔은 선행 0이
        # 없어 사실상 clean_no와 동일하나, 양쪽을 동일 키로 맞춰 승인번호 매칭을 보장한다.
        df["승인번호"] = df["승인번호"].apply(_appr_key)
        # 승인번호가 없는 건은 실제 결제가 이뤄지지 않은 미승인 건이므로 제외
        df = df[df["승인번호"].astype(str).str.strip() != ""].copy()

    # 시간 파싱
    df["시간_분"] = 0
    df["시간표시"] = ""
    tcol = next((c for c in ["시간", "거래시간", "승인시간"] if c in df.columns), None)
    if tcol:
        parsed = df[tcol].apply(_parse_clock)
        df["시간_분"] = parsed.apply(lambda x: x[0])
        df["시간표시"] = parsed.apply(lambda x: x[1])

    # 거래일 → 날짜(YYYY-MM-DD). 다일(월간) 파일을 일자별로 대사하기 위한 키.
    df["날짜"] = ""
    dcol = next((c for c in ["거래일", "거래일자", "승인일자", "거래날짜"] if c in df.columns), None)
    if dcol:
        df["날짜"] = df[dcol].apply(norm_date)

    # 거래상태 분류
    scol = next((c for c in ["거래상태", "상태"] if c in df.columns), None)
    df["tx_status"] = "정상"
    if scol:
        s = df[scol].astype(str)
        df.loc[s.str.contains("포인트사용승인", na=False), "tx_status"] = "정상"
        df.loc[s.str.contains("거절", na=False), "tx_status"] = "승인거절"
        df.loc[s.str.contains("포인트실패", na=False), "tx_status"] = "포인트실패"
        # 취소승인(=취소가 승인된 건)도 취소로 분류
        df.loc[s.str.contains("취소", na=False), "tx_status"] = "취소"
        # 취소거절: 취소 시도가 거절된 건 → 매출도 환불도 아님, 총합계 제외
        df.loc[s.str.contains("취소거절", na=False), "tx_status"] = "취소거절"
        df.loc[s.str.contains("취소.?거절|거절.?취소", na=False, regex=True), "tx_status"] = "취소거절"
        # 조회 건은 실제 결제/취소가 아닌 단순 조회이므로 제외
        # (포인트조회, 잔액조회, 원거래조회, 취소조회 등)
        df.loc[s.str.contains("조회", na=False), "tx_status"] = "조회"

    typcol = next((c for c in ["구분"] if c in df.columns), None)
    df["is_현금"] = False
    if typcol:
        df["is_현금"] = df[typcol].astype(str).str.contains("현금", na=False)

    # 발급사/매입사에 "현금"이 포함되면 현금영수증 → 카드 승인내역에서 제외
    for ccol in ["발급사", "매입사"]:
        if ccol in df.columns:
            df.loc[df[ccol].astype(str).str.contains("현금", na=False), "is_현금"] = True

    # K/S: K=현금영수증, S=카드 → 모두 유지 (is_현금으로 구분)

    # 카드사 정보 추출
    card_co_col = next((c for c in ["매입사", "카드사", "발급사", "카드종류"] if c in df.columns), None)
    df["카드사"] = ""
    if card_co_col:
        df["카드사"] = df[card_co_col].astype(str).str.replace("nan", "").str.strip()
        df["카드사"] = df["카드사"].apply(lambda x: re.sub(r"카드$", "", x).strip() if x else "")

    df["h_idx"] = range(len(df))
    return df


def _looks_like_chart_no(series):
    """주어진 열이 '차트번호'처럼 보이는지(값의 70%+ 가 3~7자리 정수) 판정.

    일부 지점 일일마감은 차트번호 열의 머리글 칸이 비어 있어 헤더명으로는 못 잡는다.
    그 무라벨 열이 진짜 차트번호인지(순서·금액·이름 등이 아닌지) 값 패턴으로 확인한다.
    """
    v = series.astype(str).str.strip()
    v = v[(v != "") & (v.str.lower() != "nan")]
    if len(v) < 3:
        return False
    digitish = v.str.replace(r"[.\-\s]", "", regex=True).str.fullmatch(r"\d{3,7}")
    return float(digitish.mean()) >= 0.7


# 지점별 결제수단 표기(약어·띄어쓰기·별칭) → 표준 채널명.
# 예: 결제단말기→카드, '나만의 닥터'→나만의닥터, 여신→여신티켓, '강.언'→강남언니.
_PAY_ALIAS = {
    "카드": "카드", "결제단말기": "카드", "단말기": "카드",
    "현금": "현금", "이체": "이체",
    "여신": "여신티켓", "여신티켓": "여신티켓", "여신앱": "여신티켓",
    "강남언니": "강남언니", "강언": "강남언니",
    "나만의닥터": "나만의닥터", "나닥": "나만의닥터",
    "제로페이": "제로페이",
    "기타지역화폐": "기타지역화폐", "지역화폐": "기타지역화폐",
    "알리페이": "알리페이", "알리": "알리페이",
    "위챗페이": "위챗페이", "위쳇페이": "위챗페이", "위챗": "위챗페이",
    "카카오페이": "카카오페이",
    # QR 간편결제(큐릭 단말) 및 플랫폼앱 — 카드 단말(한솔)을 거치지 않는 비카드 결제.
    "간편결제": "간편결제", "간편결제(큐릭)": "간편결제", "큐릭": "간편결제",
    "바비톡": "바비톡", "닥터나우": "닥터나우",
}

# 플랫폼(비카드·비현금) 결제 채널 목록. 총액·정산에서 '플랫폼합' 하나로 묶인다.
# 알리페이·위챗페이·카카오페이(QR·모바일 간편결제)는 제로페이처럼 카드 단말(한솔)을 거치지
# 않으므로 카드가 아니라 플랫폼으로 집계한다(카드로 넣으면 한솔 카드 대사에 허위 불일치 발생).
_PLATFORM_CHANNELS = [
    "여신티켓", "강남언니", "나만의닥터", "제로페이", "기타지역화폐",
    "알리페이", "위챗페이", "카카오페이",
    # 간편결제(큐릭) = QR 간편결제(알리/위챗 등 외국인 결제). 바비톡·닥터나우 = 플랫폼앱.
    "간편결제", "바비톡", "닥터나우",
]


def _platform_sum(frame):
    """플랫폼 채널 컬럼들의 행별 합계 Series. 없는 컬럼은 0으로 간주한다."""
    total = 0
    for c in _PLATFORM_CHANNELS:
        if c in frame.columns:
            total = total + frame[c]
    return total


def _channel_of(label):
    """셀 텍스트를 표준 결제 채널명으로 정규화(약어·공백·하이픈·점 무시). 아니면 None."""
    key = re.sub(r"\s", "", str(label)).replace("-", "").replace(".", "")
    return _PAY_ALIAS.get(key)


def _summary_channel_totals(raw, hdr):
    """지점 일일마감 하단의 '세로 요약블록'(채널명 + 오른쪽 합계)에서 {채널: 합계} 추출.

    지점마다 결제수단 열 머리글이 비거나(인천), 위치가 어긋나거나(엔디어트), 다른 이름
    (결제단말기)이라 머리글만으로는 금액열을 못 찾는다. 반면 거의 모든 지점 시트는 하단에
    채널별 합계를 세로로 적은 요약블록을 갖는다 — 이를 '정답'으로 삼아 금액열을 되찾는다.

    한 열에 채널명이 3개 이상 세로로 쌓인 경우만 요약블록으로 인정한다(표준 양식의 머리글
    행을 요약블록으로 오인하지 않도록). 합계는 라벨 오른쪽 '첫 비어있지 않은 셀'에서 읽어,
    더 우측의 무관한 지표(총 내원객·현금시재액 등) 혼입을 막는다. 같은 채널이 여러 번
    나오면(빈 복제블록 포함) 절댓값이 가장 큰 합계를 채택한다.
    """
    arr = raw.values
    nrow, ncol = raw.shape
    label_cols = [
        j for j in range(ncol)
        if sum(1 for i in range(nrow) if i != hdr and _channel_of(arr[i][j])) >= 3
    ]
    out = {}
    for j in label_cols:
        for i in range(nrow):
            if i == hdr:
                continue
            ch = _channel_of(arr[i][j])
            if not ch:
                continue
            tot = 0
            for k in range(j + 1, ncol):
                v = str(arr[i][k]).strip()
                if v in ("", "nan"):
                    continue
                tot = clean_money(v)
                break
            if abs(tot) > abs(out.get(ch, 0)):
                out[ch] = tot
    return out


def _relabel_pay_columns(df, summary_totals):
    """결제수단 머리글이 비거나·어긋나거나·다른 이름인 비표준 일일마감에서, 각 금액열의
    환자합계를 하단 요약블록 합계(summary_totals)와 정확히 대조해 표준 채널명으로
    (재)라벨링한다. 요약블록이 없는 표준 양식에는 아무 영향이 없다.

    합계가 큰 채널부터 '유일하게 일치하는' 열에 배정한다(소액 우연 충돌 방지). 차트번호·
    성명 등 식별열은 후보에서 제외해 금액열만 손댄다. 합계가 0인 채널은 매칭 불가하므로
    건너뛴다(해당 열이 비어 있어도 결과는 0으로 동일).
    """
    if not summary_totals:
        return df
    protect = {"차트번호", "성명", "구분", "내원순서", "HP뒷자리", "내원경로", "담당/결제"}
    sums = {}
    for j in range(df.shape[1]):
        if str(df.columns[j]).strip() in protect:
            continue
        s = int(df.iloc[:, j].apply(clean_money).sum())
        if s:
            sums[j] = s
    newcols = list(df.columns)
    used = set()
    for ch, t in sorted(summary_totals.items(), key=lambda kv: -abs(kv[1])):
        if not t:
            continue
        hits = [j for j, s in sums.items() if j not in used and s == t]
        if len(hits) == 1:
            newcols[hits[0]] = ch
            used.add(hits[0])
    df = df.copy()
    df.columns = newcols
    return df


def _uniquify_columns(cols):
    """중복 컬럼명을 고유하게 만든다(첫 등장은 원래 이름 유지, 이후 '.1','.2' 접미).

    지점 시트 머리글에 같은 이름(또는 빈칸)이 두 번 이상 나오면 DataFrame에
    중복 라벨이 생긴다. 이 경우 df[label] 이 Series 대신 DataFrame 을 반환해
    불리언 마스킹이 ValueError 로 죽거나 to_dict 가 'columns are not unique'
    경고와 함께 일부 열을 누락시킨다. 첫 등장 이름은 그대로 두어 pay_map 등의
    이름 기반 조회(예: '카드')는 정상 동작하도록 한다.
    """
    seen, out = {}, []
    for c in cols:
        c = str(c)
        if c in seen:
            seen[c] += 1
            out.append(f"{c}.{seen[c]}")
        else:
            seen[c] = 0
            out.append(c)
    return out


def parse_daily(raw):
    """일일마감 파싱: 동적 헤더, 결제수단별 금액, 환불/취소 내역 포함"""
    hdr = None
    for i, row in raw.iterrows():
        rs = row.astype(str).str.replace(r"\s", "", regex=True)
        if rs.str.contains("내원|차트번호|성명", na=False).sum() >= 2:
            hdr = i
            break
    if hdr is None:
        st.error("일일마감 파일에서 헤더를 찾을 수 없습니다.")
        return pd.DataFrame(), pd.DataFrame()

    df = raw.iloc[hdr + 1:].copy()
    cols = [str(c).strip().replace("\n", "") for c in raw.iloc[hdr]]
    # 무라벨 차트번호 열 복구: 여러 지점 시트가 '순서|구분|차트번호|성명' 구조인데
    # 차트번호 열의 머리글 칸만 비어 있다. '차트번호' 헤더가 따로 없고 '구분' 바로
    # 오른쪽 열이 무라벨이면서 숫자(3~7자리)면 그 열을 차트번호로 라벨링한다.
    # ('이름/차트번호 중복여부' 같은 decoy 열은 머리글이 있으므로 영향 없음.)
    if "차트번호" not in cols:
        gpos = next((i for i, c in enumerate(cols) if c.replace(" ", "") == "구분"), None)
        if (gpos is not None and gpos + 1 < len(cols) and cols[gpos + 1] == ""
                and _looks_like_chart_no(raw.iloc[hdr + 1:, gpos + 1])):
            cols[gpos + 1] = "차트번호"
    df.columns = _uniquify_columns(cols)
    df = df.reset_index(drop=True)

    # --- 환불/취소 섹션 탐지 및 분리 ---
    # 섹션 구분 행: "환불/취소", "환불 내역", "취소 내역" 등의 제목 행 탐지
    # 일반 데이터 행(차트번호가 숫자인 행)은 제외
    refund_hdr = None
    for i, row in df.iterrows():
        row_text = row.astype(str).str.replace(r"\s", "", regex=True).str.cat()
        if "환불" in row_text or "취소" in row_text:
            # 차트번호 컬럼이 유효한 숫자이면 일반 데이터 행이므로 건너뜀
            chart_val = str(row.iloc[0]).strip() if len(row) > 0 else ""
            if "차트번호" in df.columns:
                chart_val = str(row.get("차트번호", "")).strip()
            is_data_row = chart_val.replace("-", "").replace(" ", "").isdigit() and len(chart_val) >= 3
            if not is_data_row:
                refund_hdr = i
                break

    refund_df = pd.DataFrame()
    if refund_hdr is not None:
        # 환불 섹션 이전까지만 메인 데이터로 사용
        refund_raw = df.iloc[refund_hdr:].copy().reset_index(drop=True)
        df = df.iloc[:refund_hdr].copy().reset_index(drop=True)

        # 환불 섹션 내에서 헤더 행 찾기 (구분, 차트번호, 성명 등)
        r_hdr = None
        for i, row in refund_raw.iterrows():
            rs = row.astype(str).str.replace(r"\s", "", regex=True)
            if rs.str.contains("차트번호|성명", na=False).sum() >= 2:
                r_hdr = i
                break
        if r_hdr is not None:
            r_data = refund_raw.iloc[r_hdr + 1:].copy()
            r_data.columns = _uniquify_columns(
                [str(c).strip().replace("\n", "") for c in refund_raw.iloc[r_hdr]])
            r_data = r_data.reset_index(drop=True)
            # 빈 행 + 합계/총계 행 제거 (환자 이름은 숫자뿐일 수 없음 → 숫자만이면 합계행)
            if "성명" in r_data.columns:
                nm_r = r_data["성명"].astype(str).str.strip()
                is_num_name = nm_r.str.match(r"^[\d,\.\s\-]+$") & (nm_r != "")
                r_data = r_data[r_data["성명"].notna() & (nm_r != "") & ~is_num_name]
            if "차트번호" in r_data.columns:
                r_data = r_data[r_data["차트번호"].apply(lambda x: len(clean_no(x)) >= 3)]
            r_data = r_data.reset_index(drop=True)

            if not r_data.empty:
                if "차트번호" in r_data.columns:
                    r_data["차트번호"] = r_data["차트번호"].apply(clean_no)
                if "성명" in r_data.columns:
                    r_data["성명"] = r_data["성명"].apply(clean_name)

                pay_map_r = {
                    "카드": ["카드"], "현금": ["현금"], "이체": ["이체"],
                    "여신티켓": ["여신티켓", "여신"], "강남언니": ["강남언니"],
                    "나만의닥터": ["나만의닥터", "나만의 닥터", "기타-나만의닥터", "기타나만의닥터"],
                    "제로페이": ["제로페이"],
                    "기타지역화폐": ["기타-지역화폐", "기타지역화폐"],
                    "알리페이": ["알리페이"], "위챗페이": ["위챗페이", "위쳇페이"],
                    "카카오페이": ["카카오페이"],
                    "간편결제": ["간편결제(큐릭)", "간편결제", "큐릭"],
                    "바비톡": ["바비톡"], "닥터나우": ["닥터나우"],
                }
                for tgt, cands in pay_map_r.items():
                    mc = next((c for c in cands if c in r_data.columns), None)
                    r_data[tgt] = r_data[mc].apply(clean_money) if mc else 0

                r_data["플랫폼합"] = _platform_sum(r_data)
                r_data["총액"] = r_data["카드"] + r_data["현금"] + r_data["이체"] + r_data["플랫폼합"]
                refund_df = r_data

    # --- 메인 데이터 필터링 ---
    if "성명" in df.columns:
        nm_m = df["성명"].astype(str).str.strip()
        # 합계/소계/총계 행 제외: '합계'·'소계' 텍스트 또는 성명이 숫자뿐인 행(=합계행)
        is_num_name = nm_m.str.match(r"^[\d,\.\s\-]+$") & (nm_m != "")
        df = df[df["성명"].notna() & ~nm_m.str.contains("합계|소계", na=False) & ~is_num_name]
    # 차트번호가 비어있고 성명도 비어있는 총합계 행 제거
    if "차트번호" in df.columns:
        chart_valid = df["차트번호"].apply(lambda x: str(clean_no(x)).strip() != "")
        name_valid = df["성명"].notna() & (df["성명"].astype(str).str.strip() != "") if "성명" in df.columns else True
        df = df[chart_valid | name_valid]
    df = df.reset_index(drop=True)

    if "차트번호" in df.columns:
        df["차트번호"] = df["차트번호"].apply(clean_no)
    else:
        df["차트번호"] = ""
    if "성명" in df.columns:
        df["성명"] = df["성명"].apply(clean_name)
    else:
        df["성명"] = ""

    order_col = next((c for c in df.columns if "내원" in str(c) and "순서" in str(c)), None)
    if order_col is None:
        order_col = next((c for c in df.columns if "내원" in str(c)), None)
    if order_col and order_col in df.columns:
        df["내원순서"] = pd.to_numeric(df[order_col], errors="coerce")
    df["내원순서"] = df.get("내원순서", pd.Series(dtype=float))
    df["내원순서"] = df["내원순서"].fillna(pd.Series(range(1, len(df) + 1))).astype(int)

    # 비표준 지점 양식 대응: 결제수단 머리글이 비거나(인천)·위치가 어긋나거나(엔디어트)·
    # 다른 이름(결제단말기)이어도, 하단 세로 요약블록의 채널 합계와 각 금액열의 환자합계를
    # 대조해 표준 채널명으로 재라벨링한다. 표준 양식(요약블록 없음)에는 무영향.
    df = _relabel_pay_columns(df, _summary_channel_totals(raw, hdr))

    pay_map = {
        "카드": ["카드", "결제단말기", "단말기"], "현금": ["현금"], "이체": ["이체"],
        "여신티켓": ["여신티켓", "여신"], "강남언니": ["강남언니"],
        "나만의닥터": ["나만의닥터", "나만의 닥터", "기타-나만의닥터", "기타나만의닥터"],
        "제로페이": ["제로페이"],
        "기타지역화폐": ["기타-지역화폐", "기타지역화폐"],
        "알리페이": ["알리페이"], "위챗페이": ["위챗페이", "위쳇페이"],
        "카카오페이": ["카카오페이"],
        "간편결제": ["간편결제(큐릭)", "간편결제", "큐릭"],
        "바비톡": ["바비톡"], "닥터나우": ["닥터나우"],
    }
    for tgt, cands in pay_map.items():
        mc = next((c for c in cands if c in df.columns), None)
        df[tgt] = df[mc].apply(clean_money) if mc else 0

    df["플랫폼합"] = _platform_sum(df)
    df["총액"] = df["카드"] + df["현금"] + df["이체"] + df["플랫폼합"]

    # --- 메인 데이터 내 환불/취소 행 추출 (구분 컬럼 기준) ---
    # "구분" 컬럼에 "환불" 또는 "취소"가 포함된 행을 refund_df로 이동
    gubun_col = next((c for c in df.columns if str(c).replace(" ", "") == "구분"), None)
    if gubun_col and refund_df.empty:
        refund_mask = df[gubun_col].astype(str).str.contains("환불|취소", na=False)
        if refund_mask.any():
            refund_rows = df[refund_mask].copy().reset_index(drop=True)
            if "총액" not in refund_rows.columns or refund_rows["총액"].sum() == 0:
                # 총액이 없으면 결제수단 합계로 재계산
                refund_rows["총액"] = refund_rows["카드"] + refund_rows["현금"] + refund_rows["이체"] + refund_rows["플랫폼합"]
            refund_df = refund_rows
            df = df[~refund_mask].copy().reset_index(drop=True)

    df["d_idx"] = range(len(df))
    return df, refund_df


def daily_format_warning(daily):
    """파싱된 일일마감이 표준 양식과 호환되는지 점검 → 경고문(또는 None).

    지점마다 마감 양식이 달라(예: 차트번호가 없거나, '카드'가 '결제단말기'로,
    '나만의닥터'가 '나만의 닥터'로 표기) 합계가 실제보다 적게 잡히거나 환자 매칭이
    불가할 수 있다. 그런 신호를 감지해 사용자에게 알린다(분석은 막지 않음).
    """
    if daily is None or getattr(daily, "empty", True):
        return None
    no_chart = (
        daily["차트번호"].astype(str).str.strip().eq("").all()
        if "차트번호" in daily.columns else True
    )
    if no_chart:
        return (
            "이 일일마감에는 '차트번호'가 없어 차트마감과 환자 단위 매칭·추적이 불가합니다. "
            "또한 카드 등 일부 결제수단이 다른 컬럼명(예: '결제단말기', '나만의 닥터')으로 "
            "되어 있으면 합계가 실제보다 적게 잡힐 수 있습니다 — 지점 마감 양식이 표준과 "
            "다른 경우입니다. 해당 날짜 탭을 표준 양식(templates/일일마감_표준양식)으로 "
            "입력하면 정확히 분석됩니다. 그 전까지는 결과 수치를 원본과 대조해 확인하세요."
        )
    return None


def cross_check_daily_patient(daily, patient, min_rate=None):
    """일일마감↔차트마감이 같은 지점·같은 날짜인지 '차트번호' 겹침으로 검증.

    같은 지점·같은 날짜면 환자(차트번호)가 크게 겹치고, 다른 지점/날짜면 거의
    안 겹친다. 일치율 = |교집합| / min(일마 차트수, 차트 차트수) (포함율, 0~1).

    반환: (status, message, info)
      - 'ok'    : 통과 (message=None)
      - 'block' : 일치율이 기준 미만 → 다른 지점/날짜 파일로 보고 분석 차단
      - 'warn'  : 한쪽에 차트번호가 없어 대조 자체가 불가 → 경고만 하고 진행(전환기)
    """
    if min_rate is None:
        min_rate = CROSS_CHECK_MIN_RATE

    def _charts(df):
        if df is None or getattr(df, "empty", True) or "차트번호" not in df.columns:
            return set()
        s = df["차트번호"].astype(str).str.strip()
        return set(s[(s != "") & (s.str.lower() != "nan")].tolist())

    d, p = _charts(daily), _charts(patient)
    nd, npat = len(d), len(p)
    inter = len(d & p)
    rate = inter / min(nd, npat) if (nd and npat) else 0.0
    info = {"rate": rate, "inter": inter, "n_daily": nd, "n_patient": npat, "min_rate": min_rate}

    # 대조 불가(차트번호 없음) → 차단하지 않고 경고만 (표준화 전환기 편의)
    if nd == 0:
        return "warn", (
            "일일마감에 차트번호가 없어 차트마감과 대조(같은 지점·날짜 확인)를 할 수 "
            "없습니다. 표준 양식(차트번호 포함, templates/일일마감_표준양식)으로 입력하면 "
            "자동 검증됩니다. 지금 결과는 다른 지점/날짜일 수 있으니 원본과 대조해 확인하세요."
        ), info
    if npat == 0:
        return "warn", (
            "차트마감(베가스) 파일에서 차트번호를 읽지 못해 일일마감과 대조할 수 없습니다. "
            "올바른 차트 정산 파일인지 확인하세요."
        ), info
    # 양쪽 모두 차트번호 있음 → 일치율로 판정. 기준 미만이면 분석 차단.
    if rate < min_rate:
        return "block", (
            f"업로드한 차트마감이 선택한 일일마감과 일치하지 않습니다 "
            f"(차트번호 일치율 {rate * 100:.0f}% < 기준 {min_rate * 100:.0f}%). "
            "다른 지점이나 다른 날짜의 파일을 올린 것은 아닌지 확인하세요."
        ), info
    return "ok", None, info


# 결제메모 플랫폼 키워드 → 플랫폼명 매핑
_PLATFORM_KEYWORDS = {
    "강남언니": "강남언니", "강언": "강남언니",
    "나만의닥터": "나만의닥터", "나닥": "나만의닥터",
    "여신티켓": "여신티켓", "여신": "여신티켓",
}


def parse_patient(raw):
    """환자별집계 파싱: 결제수단 정밀분류"""
    hdr = 0
    for i, row in raw.iterrows():
        if row.astype(str).str.contains("차트번호|이름|결제수단", na=False).sum() >= 2:
            hdr = i
            break
    df = raw.iloc[hdr + 1:].copy()
    df.columns = [str(c).strip().replace("\n", "") for c in raw.iloc[hdr]]
    df = df.reset_index(drop=True)

    if "이름" in df.columns:
        df = df[df["이름"].notna() & ~df["이름"].astype(str).str.contains("합계", na=False)]
    df = df.reset_index(drop=True)

    # 차트번호/이름 컬럼이 없는 export 형식에서도 KeyError 없이 진행
    df["차트번호"] = df["차트번호"].apply(clean_no) if "차트번호" in df.columns else ""
    df["이름"] = df["이름"].apply(clean_name) if "이름" in df.columns else ""

    # 날짜(YYYY-MM-DD): 반드시 '수납일' 기준 — 한솔 거래일·일일마감과 같은 축.
    # (진료일은 수납일과 다를 수 있어[외상·선결제] 대사 키로 쓰면 안 된다.)
    df["날짜"] = ""
    if "수납일" in df.columns:
        df["날짜"] = df.loc[:, "수납일"].pipe(
            lambda s: (s.iloc[:, 0] if isinstance(s, pd.DataFrame) else s)
        ).apply(norm_date)

    amt_cols = [c for c in ["비급여(과세총금액)", "비급여(비과세)"] if c in df.columns]
    copay_cols = [c for c in df.columns if ("본부금" in str(c) or "본인부담" in str(c)) and "환불" not in str(c)]
    all_amt_cols = amt_cols + copay_cols
    for c in all_amt_cols:
        df[c] = df[c].apply(clean_money)
    df["본부금"] = df[copay_cols].sum(axis=1) if copay_cols else 0
    df["금액"] = df[all_amt_cols].sum(axis=1) if all_amt_cols else 0

    # ── 환불 전용 컬럼 파싱: 환불(과세총금액), 환불(비과세), 환불(본부금) ──
    refund_amt_cols = [c for c in ["환불(과세총금액)", "환불(비과세)"] if c in df.columns]
    refund_copay_cols = [c for c in df.columns if "환불" in str(c) and ("본부금" in str(c) or "본인부담" in str(c))]
    all_refund_cols = refund_amt_cols + refund_copay_cols
    for c in all_refund_cols:
        df[c] = df[c].apply(clean_money)
    df["환불금액합"] = df[all_refund_cols].sum(axis=1) if all_refund_cols else 0

    def _pick_first_series(frame, col):
        """중복 컬럼명이 있는 경우 첫 번째 컬럼만 Series로 반환"""
        if col not in frame.columns:
            return pd.Series(index=frame.index, dtype=object)
        data = frame.loc[:, col]
        return data.iloc[:, 0] if isinstance(data, pd.DataFrame) else data

    # 결제수단 정밀분류
    pay = _pick_first_series(df, "결제수단").astype(str)
    pay_norm = pay.str.lower().str.replace(r"[\s\-_/+·()\[\]]", "", regex=True)

    # 결제취소/환불 라인 탐지 (메모/비고의 단순 문의 문구는 제외)
    cancel_text_cols = [
        c for c in ["결제수단", "수납구분", "결제구분", "구분", "상태"]
        if c in df.columns
    ]
    cancel_text = pd.Series("", index=df.index, dtype=str)
    for c in cancel_text_cols:
        cancel_text = cancel_text + " " + _pick_first_series(df, c).astype(str)
    # 환불 전용 컬럼에 금액이 있는 행도 환불로 감지
    has_refund_amt = df["환불금액합"] > 0
    df["is_취소"] = cancel_text.str.contains(r"취소|환불", na=False) | has_refund_amt
    if df["is_취소"].any():
        # 비급여 금액이 0이고 환불 컬럼에만 금액이 있는 행 → 환불 금액으로 채움
        refund_only_mask = df["is_취소"] & (df["금액"].abs() == 0) & (df["환불금액합"] > 0)
        df.loc[refund_only_mask, "금액"] = df.loc[refund_only_mask, "환불금액합"]
        # 환불 전용 행의 본부금도 환불(본부금) 컬럼에서 가져옴
        if refund_copay_cols:
            refund_copay_only = df["is_취소"] & (df["본부금"].abs() == 0)
            refund_copay_sum = df[refund_copay_cols].sum(axis=1)
            df.loc[refund_copay_only & (refund_copay_sum > 0), "본부금"] = refund_copay_sum[refund_copay_only & (refund_copay_sum > 0)]
        df.loc[df["is_취소"], "금액"] = -df.loc[df["is_취소"], "금액"].abs()
        df.loc[df["is_취소"], "본부금"] = -df.loc[df["is_취소"], "본부금"].abs()

    # 취소/환불 행은 결제수단 컬럼에 "취소"/"환불"만 적혀 원래 결제수단이 누락되는
    # 경우가 있으므로, cancel_text(수납구분/결제구분 등 여러 컬럼 합산)도 함께 참조
    cancel_norm = cancel_text.str.lower().str.replace(r"[\s\-_/+·()\[\]]", "", regex=True)

    # 간편결제(큐릭): QR 간편결제(알리/위챗 등). 베가스가 '간편결제(현금영수증)'으로 적어도
    # 실제로는 카드 단말을 안 거치는 플랫폼 결제이므로 현금이 아니라 플랫폼으로 분류한다
    # (일일마감의 '간편결제(큐릭)' 컬럼과 대칭). '현금영수증' 글자 때문에 현금으로 새지 않게
    # cash_mask에서 명시적으로 제외한다.
    kanpyeon_mask = pay_norm.str.contains("간편결제", na=False)

    card_mask = (
        pay_norm.str.contains("카드", na=False)
        | (df["is_취소"] & cancel_norm.str.contains("카드", na=False))
    )
    cash_mask = (
        pay_norm.str.contains("현금", na=False)
        | pay_norm.str.contains("현금영수증", na=False)
        | pay_norm.str.contains("영수증", na=False)
        | (df["is_취소"] & cancel_norm.str.contains("현금|영수증", na=False)
           & ~cancel_norm.str.contains("카드", na=False))
    ) & ~kanpyeon_mask
    transfer_mask = (
        # '통장'은 부분일치로 잡는다: '결제취소-통장'/'환불-통장'(통장으로 환불)이
        # isin 완전일치에 걸리지 않아 '기타'로 새던 문제 수정(인천 5/11 -599,000 등).
        pay_norm.str.contains("통장", na=False)
        | pay_norm.str.contains("이체", na=False)
        | pay_norm.str.contains("계좌", na=False)
        | pay_norm.str.contains("입금", na=False)
        | (df["is_취소"] & cancel_norm.str.contains("이체|계좌|입금|통장", na=False)
           & ~cancel_norm.str.contains("카드|현금", na=False))
    )
    # 차트마감(베가스)은 플랫폼을 보통 '기타-…'로 적지만, 알리페이/위챗페이/카카오페이는
    # 접두 없이 그대로 적힐 수 있어 이름으로도 플랫폼으로 잡는다(일일마감 집계와 대칭 유지).
    platform_mask = (
        pay_norm.str.startswith("기타", na=False)
        # '환불-기타'(기타(여신 등) 결제의 환불)도 플랫폼으로: 양(+)의 '기타(기타)'가
        # 플랫폼으로 집계되므로 그 환불도 대칭으로 플랫폼에서 차감돼야 한다.
        | (df["is_취소"] & pay_norm.str.contains("기타", na=False))
        | pay_norm.str.contains("알리페이|위챗페이|위쳇페이|카카오페이", na=False)
        | kanpyeon_mask
    )

    df["분류"] = "기타"
    df.loc[card_mask, "분류"] = "카드"
    df.loc[cash_mask, "분류"] = "현금"
    # 현금/영수증+이체 같은 복합 표기는 현금/이체 합산 구간으로 들어가도록 우선 이체로 분류
    df.loc[transfer_mask, "분류"] = "이체"
    df.loc[platform_mask & ~card_mask & ~cash_mask & ~transfer_mask, "분류"] = "플랫폼"

    # 카드사 추출
    df["카드사"] = ""
    card_rows = df["분류"] == "카드"
    if card_rows.any():
        df.loc[card_rows, "카드사"] = pay[card_rows].apply(_extract_card_company)

    # 결제메모에서 승인번호 + 플랫폼 키워드 추출
    df["승인번호목록"] = [[] for _ in range(len(df))]
    df["플랫폼구분"] = ""
    mcol = next((c for c in ["결제메모", "승인번호", "메모"] if c in df.columns), None)
    if mcol:
        memo = _pick_first_series(df, mcol)

        def _parse_memo(text):
            """결제메모 파싱: 승인번호(5~10자리) 추출 + 플랫폼 키워드 감지
            구분자: 쉼표(,) / 슬래시(/) / 공백 모두 지원"""
            if pd.isna(text) or str(text).strip() in ("", "nan", "NaN"):
                return [], ""
            s = str(text).strip()
            # 플랫폼 키워드 감지 (강언→강남언니, 나닥→나만의닥터, 여신→여신티켓)
            platform = ""
            for kw, name in _PLATFORM_KEYWORDS.items():
                if kw in s:
                    platform = name
                    break
            # 승인번호 추출: 5~10자리 숫자 (앞뒤가 숫자가 아닌 경계)
            # 카드사/단말기별로 6~8자 외 5자/9~10자 케이스도 존재
            nums = re.findall(r"(?<!\d)\d{5,10}(?!\d)", s)
            return nums, platform

        parsed = memo.apply(_parse_memo)
        # 결제메모의 승인번호는 8자리 0패딩으로 적히는 경우가 많아(예: "00873971"),
        # 한솔(자연수 "873971")과 매칭되도록 _appr_key로 선행 0을 제거해 키를 정규화한다.
        df["승인번호목록"] = parsed.apply(lambda x: [_appr_key(a) for a in x[0]])
        df["플랫폼구분"] = parsed.apply(lambda x: x[1])
        # 플랫폼 키워드가 감지된 행 → 분류를 "플랫폼"으로 변경.
        # 단, 결제수단이 명시적으로 '카드'인 행(예: '환불-카드' + 메모 "카드 취소 후
        # 여신앱으로 변경")은 카드 단말(한솔)을 거친 거래이므로 카드 분류를 유지한다
        # — 메모는 결제수단 '변경 경위'를 적은 것일 뿐이다(엔디어트 5/26 -198,000 사례).
        plat_mask = (df["플랫폼구분"] != "") & (df["분류"] != "카드")
        df.loc[plat_mask, "분류"] = "플랫폼"
        df.loc[(df["플랫폼구분"] != "") & (df["분류"] == "카드"), "플랫폼구분"] = ""

    df["p_idx"] = range(len(df))
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 매칭 엔진
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _find_split_combo(items, target, rs, window):
    """분할결제 후보 탐색: items 중 r개(rs 순서)의 금액 합이 target과 같고
    시간 spread<=window 인 첫 조합을 반환(없으면 None).

    itertools.combinations 전수탐색과 '완전히 동일한 조합'을 고르되, 금액 인덱스로
    제3원소를 즉시 찾아 O(n^3)→O(n^2)로 가속한다(미매칭 다수 시 수십초~분 → 수초).
    items: [[h_idx, 금액, 시간_분], ...] (모두 금액>0 가정). 반환: 선택된 행들의 리스트.
    """
    from collections import defaultdict
    n = len(items)
    by_amt = defaultdict(list)            # 금액 → 인덱스 오름차순 목록
    for idx, it in enumerate(items):
        by_amt[it[1]].append(idx)

    def _spread_ok(idxs):
        ts = [items[k][2] for k in idxs]
        return (max(ts) - min(ts)) <= window

    for r in rs:
        if n < r:
            continue
        if r == 2:
            for i in range(n):
                for j in by_amt.get(target - items[i][1], ()):
                    if j > i and _spread_ok((i, j)):
                        return [items[i], items[j]]
        elif r == 3:
            for i in range(n):
                for j in range(i + 1, n):
                    need = target - items[i][1] - items[j][1]
                    if need <= 0:
                        continue
                    for k in by_amt.get(need, ()):
                        if k > j and _spread_ok((i, j, k)):
                            return [items[i], items[j], items[k]]
        else:                              # 일반 r 안전망(현재 미사용)
            for combo in combinations(range(n), r):
                if sum(items[c][1] for c in combo) == target and _spread_ok(combo):
                    return [items[c] for c in combo]
    return None


def run_matching(hansol, daily, patient):
    """
    10-Pass 매칭 (v3.0):
      P1: 승인번호 직접매칭
      P1b: 공동결제 합산 (1카드 N차트)
      P1c: 공유카드 매칭 (동일 카드번호, 다른 환자)
      P2: 유일 금액 1:1
      P2b: 카드사+금액 (동일금액 다건 → 카드사 구분)
      P3: 분할결제 2~3건 합 (시간근접)
      P3b: 본부금 기반 분할결제 (차트 본부금 힌트)
      P4: 시간-순서 상관 (동일금액 다건, 카드사 우선)
      P5: 현금영수증 + 이체
      P5b: 복합결제 매칭 (1인 다수단 → 카드+현금+이체 통합)
      P6: 한솔↔일마 결과 기반 한솔↔차트 크로스레퍼런스 재매칭
      P7: 분할결제 크로스레퍼런스
      P8: 차트 분할결제 보강
      P9: 누락건 소급재검토 (후속 정보로 이전 누락건 재매칭)
    """
    h_ok = hansol[hansol["tx_status"] == "정상"]
    h_card = h_ok[~h_ok["is_현금"]].copy()
    h_cash = h_ok[h_ok["is_현금"]].copy()
    d_card = daily[daily["카드"] > 0].copy()

    matched_h, matched_dc = set(), set()
    results = []

    def add(rule, conf, h_idxs, d_row, amount_override=None, note=""):
        for hi in h_idxs:
            hr = hansol[hansol["h_idx"] == hi].iloc[0]
            matched_amt = int(amount_override) if amount_override is not None else int(hr["금액"])
            results.append(dict(
                매칭규칙=rule, 확신도=conf,
                한솔_hidx=int(hr["h_idx"]),
                한솔_시간=hr.get("시간표시", ""),
                한솔_금액=matched_amt,
                한솔_원거래금액=int(hr["금액"]),
                한솔_카드번호=str(hr.get("카드번호", ""))[:12],
                한솔_카드사=str(hr.get("카드사", "")),
                한솔_승인번호=str(hr.get("승인번호", "")),
                한솔_유형="현금" if hr["is_현금"] else "카드",
                일마_순서=d_row["내원순서"], 일마_성명=d_row["성명"],
                일마_차트=d_row["차트번호"], 일마_카드=int(d_row["카드"]),
                비고=note,
            ))
            matched_h.add(hi)
        matched_dc.add(d_row["d_idx"])

    # 승인번호→차트번호 맵 (플랫폼 결제 제외 – 플랫폼은 한솔페이를 경유하지 않음)
    appr_map = {}
    for _, pr in patient.iterrows():
        if pr.get("플랫폼구분", ""):
            continue
        for a in pr["승인번호목록"]:
            aa = clean_no(a)
            if not aa:
                continue
            appr_map.setdefault(aa, set()).add(clean_no(pr["차트번호"]))

    # 차트→본부금/카드사 맵
    chart_info = {}
    for _, pr in patient.iterrows():
        ch = pr["차트번호"]
        if ch not in chart_info:
            chart_info[ch] = {"본부금": 0, "카드사_list": []}
        chart_info[ch]["본부금"] += int(pr.get("본부금", 0))
        card_co = str(pr.get("카드사", "")).strip()
        if card_co and card_co not in chart_info[ch]["카드사_list"]:
            chart_info[ch]["카드사_list"].append(card_co)

    # P1
    if appr_map:
        for _, hr in h_card.iterrows():
            if hr["h_idx"] in matched_h:
                continue
            a = hr.get("승인번호", "")
            if a and a in appr_map and len(appr_map[a]) == 1:
                ch = list(appr_map[a])[0]
                dc = d_card[(d_card["차트번호"] == ch) & (~d_card["d_idx"].isin(matched_dc))]
                if not dc.empty:
                    add("P1_승인번호", "🟢HIGH", [hr["h_idx"]], dc.iloc[0])

    # P1b - 동일 승인번호가 여러 차트에 기재된 합산결제 매칭
    # 예: A(52,800) + B(173,800)을 1회 226,600 결제한 경우
    for _, hr in h_card.iterrows():
        if hr["h_idx"] in matched_h:
            continue
        appr_no = clean_no(hr.get("승인번호", ""))
        charts = list(appr_map.get(appr_no, set()))
        if not appr_no or len(charts) < 2:
            continue
        # 방어: 승인번호 하나가 비정상적으로 많은 차트에 매핑되면(플레이스홀더/데이터
        # 오류 등) 아래 전수조합이 C(n,6)로 폭증해 워커가 세그폴트로 죽는다. 실제
        # 공동결제는 몇 개 차트에 그치므로 상한을 둔다.
        if len(charts) > 8:
            continue

        cand = d_card[(d_card["차트번호"].isin(charts)) & (~d_card["d_idx"].isin(matched_dc))].copy()
        # 일마에 같은 차트가 여러 줄로 나뉜 경우까지 고려하기 위해 최소 2건 이상이면 탐색
        if len(cand) < 2:
            continue

        target = int(hr["금액"])
        cand_rows = list(cand.to_dict("records"))
        # 후보 수(n)에 대해 조합 수가 C(n,2)+…+C(n,6) 로 급증한다(n=25면 24만,
        # n=60이면 5,600만, n=100이면 12억). 미매칭 한솔 건마다 반복되므로 상한을
        # 넘으면 전수탐색을 건너뛰어 조합 폭발/세그폴트를 방지한다.
        if len(cand_rows) > 24:
            continue
        chosen = None
        max_r = min(6, len(cand_rows))
        for r in range(2, max_r + 1):
            for combo in combinations(range(len(cand_rows)), r):
                rows = [cand_rows[k] for k in combo]
                if sum(int(x["카드"]) for x in rows) == target:
                    chosen = rows
                    break
            if chosen:
                break

        if not chosen:
            continue

        chosen_charts = sorted({str(x["차트번호"]) for x in chosen})
        for d_row in chosen:
            add(
                "P1b_공동결제합산",
                "🟢HIGH",
                [hr["h_idx"]],
                d_row,
                amount_override=int(d_row["카드"]),
                note=f"공동결제 승인번호 {appr_no} / 차트 {', '.join(chosen_charts)} (원거래 {target:,}원)",
            )

    # P1c - 공유카드 매칭: 동일 카드번호를 2인 이상이 사용한 경우
    # 한솔페이의 카드번호를 기반으로 같은 카드를 사용한 여러 환자를 시간순으로 매칭
    if "카드번호" in h_card.columns:
        # 미매칭 한솔 카드건 중 동일 카드번호가 여러 건인 경우 탐지
        h_unmatched_card = h_card[~h_card["h_idx"].isin(matched_h)].copy()
        if not h_unmatched_card.empty and "카드번호" in h_unmatched_card.columns:
            h_unmatched_card["카드번호_norm"] = h_unmatched_card["카드번호"].apply(lambda x: clean_no(x)[:12])
            # 승인번호→차트 맵에서 이미 매칭된 카드번호 수집
            matched_card_chart = {}  # card_no -> set(chart_no)
            for r in results:
                card_n = clean_no(r.get("한솔_카드번호", ""))[:12]
                chart_n = clean_no(r.get("일마_차트", ""))
                if card_n and chart_n:
                    matched_card_chart.setdefault(card_n, set()).add(chart_n)

            for card_no, group in h_unmatched_card.groupby("카드번호_norm"):
                if not card_no or len(group) < 1:
                    continue
                # 이 카드번호로 이미 매칭된 차트 확인
                known_charts = matched_card_chart.get(card_no, set())
                if not known_charts:
                    continue
                # 같은 카드를 사용하는 다른 차트(환자)의 미매칭 일마 건 탐색
                for _, hr in group.iterrows():
                    if hr["h_idx"] in matched_h:
                        continue
                    amt = int(hr["금액"])
                    # 같은 카드번호의 차트에 속한 미매칭 일마 건 중 금액 일치하는 건
                    for ch in known_charts:
                        dc = d_card[(d_card["차트번호"] == ch) & (d_card["카드"] == amt) & (~d_card["d_idx"].isin(matched_dc))]
                        if len(dc) == 1:
                            add("P1c_공유카드", "🟢HIGH", [hr["h_idx"]], dc.iloc[0],
                                note=f"카드번호 {card_no[-4:]} 공유 (차트 {ch})")
                            break
                    # 다른 차트(같은 카드번호를 쓰는 새로운 환자) 검색
                    if hr["h_idx"] not in matched_h:
                        dc_all = d_card[(d_card["카드"] == amt) & (~d_card["d_idx"].isin(matched_dc))]
                        if len(dc_all) == 1:
                            # 유일 금액이면서 같은 카드번호 → 높은 확신도
                            add("P1c_공유카드", "🟡MED", [hr["h_idx"]], dc_all.iloc[0],
                                note=f"카드번호 {card_no[-4:]} / 유일금액 매칭")

    # P2
    for _, dr in d_card.iterrows():
        if dr["d_idx"] in matched_dc:
            continue
        amt = dr["카드"]
        hc = h_card[(h_card["금액"] == amt) & (~h_card["h_idx"].isin(matched_h))]
        ds = d_card[(d_card["카드"] == amt) & (~d_card["d_idx"].isin(matched_dc))]
        if len(hc) == 1 and len(ds) == 1:
            add("P2_유일금액", "🟢HIGH", [hc.iloc[0]["h_idx"]], dr)

    # P2b - 카드사+금액 매칭 (동일금액 다건 → 카드사로 구분)
    for _, dr in d_card.iterrows():
        if dr["d_idx"] in matched_dc:
            continue
        amt = dr["카드"]
        ci = chart_info.get(dr["차트번호"], {})
        card_cos = ci.get("카드사_list", [])
        if not card_cos:
            continue
        hc = h_card[(h_card["금액"] == amt) & (~h_card["h_idx"].isin(matched_h))]
        if len(hc) < 1:
            continue
        for card_co in card_cos:
            if not card_co:
                continue
            hc_match = hc[hc["카드사"].str.contains(card_co, na=False, case=False, regex=False)]
            if len(hc_match) == 1:
                add("P2b_카드사+금액", "🟢HIGH", [hc_match.iloc[0]["h_idx"]], dr)
                break

    # P3
    for _, dr in d_card.iterrows():
        if dr["d_idx"] in matched_dc:
            continue
        target = dr["카드"]
        # 양수 금액만 존재하므로 target 초과 건은 어떤 합산조합에도 포함될 수 없음 →
        # 미리 제외해 조합 탐색량을 줄인다 (매칭 결과는 동일).
        avail = h_card[(~h_card["h_idx"].isin(matched_h)) & (h_card["금액"] <= target)][
            ["h_idx", "금액", "시간_분"]].values.tolist()
        combo = _find_split_combo(avail, target, [2, 3], 10)
        if combo:
            times = [it[2] for it in combo]
            spread = max(times) - min(times)
            idxs = [int(it[0]) for it in combo]
            add(f"P3_분할{len(combo)}건", "🟢HIGH" if spread <= 5 else "🟡MED", idxs, dr)

    # P3b - 본부금 기반 분할결제 (차트 본부금 정보로 정밀 분할 탐지)
    for _, dr in d_card.iterrows():
        if dr["d_idx"] in matched_dc:
            continue
        target = dr["카드"]
        ci = chart_info.get(dr["차트번호"], {})
        copay = ci.get("본부금", 0)
        if copay <= 0 or copay >= target:
            continue
        main_amt = target - copay
        avail = h_card[~h_card["h_idx"].isin(matched_h)]
        h_main = avail[avail["금액"] == main_amt]
        h_copay = avail[avail["금액"] == copay]
        if h_main.empty or h_copay.empty:
            continue
        best_pair, best_spread = None, 999
        for _, hm in h_main.iterrows():
            for _, hcp in h_copay.iterrows():
                if hm["h_idx"] == hcp["h_idx"]:
                    continue
                spread = abs(hm["시간_분"] - hcp["시간_분"])
                if spread < best_spread:
                    best_spread = spread
                    best_pair = (int(hm["h_idx"]), int(hcp["h_idx"]))
        if best_pair and best_spread <= 15:
            add("P3b_본부금분할", "🟢HIGH" if best_spread <= 5 else "🟡MED", list(best_pair), dr)

    # 시간 문자열 → 분 변환 유틸 (여러 패스에서 공용)
    def _t2m(ts):
        p = str(ts).split(":")
        return int(p[0]) * 60 + int(p[1]) if len(p) >= 2 else 0

    # P4
    confirmed = [(r["한솔_시간"], r["일마_순서"]) for r in results if r["확신도"] == "🟢HIGH" and r["한솔_시간"]]
    if confirmed:
        confirmed.sort()

        for _, dr in d_card.sort_values("내원순서").iterrows():
            if dr["d_idx"] in matched_dc:
                continue
            amt = dr["카드"]
            hc = h_card[(h_card["금액"] == amt) & (~h_card["h_idx"].isin(matched_h))]
            if hc.empty:
                continue

            do = dr["내원순서"]
            bef = [(t, o) for t, o in confirmed if o <= do]
            aft = [(t, o) for t, o in confirmed if o > do]
            if bef and aft:
                exp = _t2m(bef[-1][0]) + (_t2m(aft[0][0]) - _t2m(bef[-1][0])) * (do - bef[-1][1]) / max(aft[0][1] - bef[-1][1], 1)
            elif bef:
                exp = _t2m(bef[-1][0])
            elif aft:
                exp = _t2m(aft[0][0])
            else:
                exp = do * 5

            # 카드사 정보로 후보 축소
            ci = chart_info.get(dr["차트번호"], {})
            card_cos = ci.get("카드사_list", [])
            hc_filtered = hc
            if card_cos and len(hc) > 1:
                for card_co in card_cos:
                    if not card_co:
                        continue
                    hc_co = hc[hc["카드사"].str.contains(card_co, na=False, case=False, regex=False)]
                    if not hc_co.empty:
                        hc_filtered = hc_co
                        break
            best = hc_filtered.iloc[(hc_filtered["시간_분"] - exp).abs().argsort()[:1]]
            diff_m = abs(best.iloc[0]["시간_분"] - exp)
            add("P4_순서추정", "🟡MED" if diff_m <= 30 else "🔴LOW", [best.iloc[0]["h_idx"]], dr)

    # P5 - 현금영수증
    for _, dr in daily.iterrows():
        for amt_col, rule_tag in [("현금", "P5_현금영수증"), ("이체", "P5_이체→현금영수증")]:
            amt = dr.get(amt_col, 0)
            if amt <= 0:
                continue
            hc = h_cash[(h_cash["금액"] == amt) & (~h_cash["h_idx"].isin(matched_h))]
            if not hc.empty:
                hr = hc.iloc[0]
                results.append(dict(
                    매칭규칙=rule_tag, 확신도="🟢HIGH" if len(hc) == 1 else "🟡MED",
                    한솔_hidx=int(hr["h_idx"]),
                    한솔_시간=hr.get("시간표시", ""), 한솔_금액=int(amt),
                    한솔_원거래금액=int(hr["금액"]),
                    한솔_카드번호=str(hr.get("카드번호", "")),
                    한솔_카드사="",
                    한솔_승인번호=str(hr.get("승인번호", "")),
                    한솔_유형="현금영수증",
                    일마_순서=dr["내원순서"], 일마_성명=dr["성명"],
                    일마_차트=dr["차트번호"], 일마_카드=int(amt),
                    비고=f"일마_{amt_col}={amt:,}",
                ))
                matched_h.add(hr["h_idx"])

    # P5b - 복합결제 매칭: 1인이 카드+현금/이체로 결제한 경우
    # 일마에 카드+현금 또는 카드+이체가 모두 있는 환자의 현금/이체 부분을 한솔 현금영수증과 매칭
    for _, dr in daily.iterrows():
        cash_amt = dr.get("현금", 0)
        xfer_amt = dr.get("이체", 0)
        # 카드가 이미 매칭된 환자의 현금/이체 부분을 추가 매칭
        if dr["d_idx"] in matched_dc and (cash_amt > 0 or xfer_amt > 0):
            chart_no = clean_no(dr["차트번호"])
            for amt, rule_tag in [(cash_amt, "P5b_복합결제_현금"), (xfer_amt, "P5b_복합결제_이체")]:
                if amt <= 0:
                    continue
                hc = h_cash[(h_cash["금액"] == amt) & (~h_cash["h_idx"].isin(matched_h))]
                if not hc.empty:
                    # 시간 근접도로 최적 선택 (같은 환자의 카드 매칭 시간 참조)
                    ref_times = [r["한솔_시간"] for r in results
                                 if clean_no(r.get("일마_차트", "")) == chart_no and r.get("한솔_시간")]
                    if ref_times and len(hc) > 1:
                        avg_t = sum(_t2m(t) for t in ref_times) / len(ref_times)
                        best = hc.iloc[(hc["시간_분"] - avg_t).abs().argsort()[:1]]
                        hr = best.iloc[0]
                    else:
                        hr = hc.iloc[0]
                    results.append(dict(
                        매칭규칙=rule_tag, 확신도="🟢HIGH" if len(hc) == 1 else "🟡MED",
                        한솔_hidx=int(hr["h_idx"]),
                        한솔_시간=hr.get("시간표시", ""), 한솔_금액=int(amt),
                        한솔_원거래금액=int(hr["금액"]),
                        한솔_카드번호=str(hr.get("카드번호", "")),
                        한솔_카드사="",
                        한솔_승인번호=str(hr.get("승인번호", "")),
                        한솔_유형="현금영수증",
                        일마_순서=dr["내원순서"], 일마_성명=dr["성명"],
                        일마_차트=dr["차트번호"], 일마_카드=int(amt),
                        비고=f"복합결제 ({amt:,}원 {'현금' if '현금' in rule_tag else '이체'})",
                    ))
                    matched_h.add(hr["h_idx"])

    # P6 - Round2: 한솔↔일마 매칭 결과로 구축한 차트 레퍼런스 재활용
    match_df = pd.DataFrame(results)
    chart_card_refs, chart_company_refs = {}, {}
    if not match_df.empty:
        card_rows = match_df[match_df["한솔_유형"] == "카드"]
        for _, mr in card_rows.iterrows():
            ch = clean_no(mr.get("일마_차트", ""))
            if not ch:
                continue
            card_no = clean_no(mr.get("한솔_카드번호", ""))[:12]
            if card_no:
                chart_card_refs.setdefault(ch, set()).add(card_no)
            co = str(mr.get("한솔_카드사", "")).strip()
            if co:
                chart_company_refs.setdefault(ch, set()).add(co)

    for _, dr in d_card.iterrows():
        if dr["d_idx"] in matched_dc:
            continue
        chart_no = clean_no(dr["차트번호"])
        target = int(dr["카드"])
        hc = h_card[(h_card["금액"] == target) & (~h_card["h_idx"].isin(matched_h))]
        if hc.empty:
            continue

        # P6a: 차트별 레퍼런스 카드번호로 정밀 재매칭
        ref_cards = chart_card_refs.get(chart_no, set())
        if ref_cards:
            hc_ref = hc[hc["카드번호"].apply(lambda x: clean_no(x)[:12] in ref_cards)]
            if len(hc_ref) == 1:
                add("P6_차트레퍼런스카드번호", "🟢HIGH", [int(hc_ref.iloc[0]["h_idx"])], dr)
                continue

        # P6b: 환자별집계 카드사 + 레퍼런스 카드사 합성으로 후보 축소
        p_cos = chart_info.get(chart_no, {}).get("카드사_list", [])
        r_cos = list(chart_company_refs.get(chart_no, set()))
        card_cos = [*p_cos, *[c for c in r_cos if c not in p_cos]]
        if card_cos:
            hc_co = hc[hc["카드사"].apply(lambda x: any(card_company_match(x, c) for c in card_cos))]
            if len(hc_co) == 1:
                add("P6b_차트카드사보정", "🟡MED", [int(hc_co.iloc[0]["h_idx"])], dr)

    # P7 - 분할결제 크로스레퍼런스: 차트번호↔승인번호↔카드번호 자동매칭
    # 이미 매칭된 결과에서 차트번호별 카드번호 맵을 구축하고,
    # 차트정보에 승인번호가 없는 결제건도 카드번호+금액으로 자동 매칭
    match_df2 = pd.DataFrame(results)
    if not match_df2.empty and "카드번호" in hansol.columns:
        # 차트번호별 카드번호·카드사 레퍼런스 맵 재구축 (P6 이후 갱신)
        chart_card_map = {}   # chart_no → set of card_numbers
        chart_appr_map = {}   # chart_no → set of approval_numbers
        card_rows2 = match_df2[match_df2["한솔_유형"] == "카드"]
        for _, mr in card_rows2.iterrows():
            ch = clean_no(mr.get("일마_차트", ""))
            if not ch:
                continue
            card_no = clean_no(mr.get("한솔_카드번호", ""))[:12]
            appr_no = str(mr.get("한솔_승인번호", "")).strip()
            if card_no:
                chart_card_map.setdefault(ch, set()).add(card_no)
            if appr_no:
                chart_appr_map.setdefault(ch, set()).add(appr_no)

        # 차트정보(patient)에서 승인번호가 없는 카드결제건 탐지
        # → 같은 차트번호의 매칭된 카드번호로 한솔페이 미매칭 건과 매칭 시도
        for _, dr in d_card.iterrows():
            if dr["d_idx"] in matched_dc:
                continue
            chart_no = clean_no(dr["차트번호"])
            target = int(dr["카드"])

            # 이 차트번호에 대한 카드번호 레퍼런스가 있는지 확인
            ref_cards = chart_card_map.get(chart_no, set())
            if not ref_cards:
                continue

            hc = h_card[(h_card["금액"] == target) & (~h_card["h_idx"].isin(matched_h))]
            if hc.empty:
                continue

            # 카드번호 매칭
            if "카드번호" in hc.columns:
                hc_match = hc[hc["카드번호"].apply(lambda x: clean_no(x)[:12] in ref_cards)]
                if len(hc_match) == 1:
                    add("P7_분할레퍼런스카드번호", "🟢HIGH", [int(hc_match.iloc[0]["h_idx"])], dr)
                    continue
                elif len(hc_match) > 1:
                    # 시간 근접도로 최적 선택
                    ci = chart_info.get(chart_no, {})
                    # 같은 차트의 매칭된 시간 참조
                    ref_times = []
                    for _, mr in card_rows2[card_rows2["일마_차트"].apply(clean_no) == chart_no].iterrows():
                        t = mr.get("한솔_시간", "")
                        if t:
                            p = str(t).split(":")
                            if len(p) >= 2:
                                ref_times.append(int(p[0]) * 60 + int(p[1]))
                    if ref_times:
                        avg_time = sum(ref_times) / len(ref_times)
                        best = hc_match.iloc[(hc_match["시간_분"] - avg_time).abs().argsort()[:1]]
                        add("P7_분할레퍼런스카드번호", "🟡MED", [int(best.iloc[0]["h_idx"])], dr)
                        continue

            # 분할결제 탐지: 같은 카드번호의 미매칭 한솔 건 중 2~3건 합산 매칭
            for card_ref in ref_cards:
                if dr["d_idx"] in matched_dc:
                    break
                hc_by_card = h_card[
                    (~h_card["h_idx"].isin(matched_h)) &
                    (h_card["카드번호"].apply(lambda x: clean_no(x)[:12] == card_ref))
                ]
                if hc_by_card.empty:
                    continue
                items_list = hc_by_card[["h_idx", "금액", "시간_분"]].values.tolist()
                combo = _find_split_combo(items_list, target, [2, 3], 15)
                if combo:
                    times = [it[2] for it in combo]
                    spread = max(times) - min(times)
                    idxs = [int(it[0]) for it in combo]
                    conf = "🟢HIGH" if spread <= 5 else "🟡MED"
                    add(f"P7_분할레퍼런스{len(combo)}건", conf, idxs, dr)

    # P8 - 차트 분할결제 보강: 차트 승인번호 힌트로 한솔 미매칭 카드건 추가 연결
    # 일마감이 1건으로 뭉쳐 있어도(차트는 2건 이상 분할) 같은 차트로 매칭 보완
    match_df3 = pd.DataFrame(results)
    if not match_df3.empty:
        matched_chart_rows = match_df3[match_df3["한솔_유형"] == "카드"]
        if not matched_chart_rows.empty:
            chart_row_ref = {
                clean_no(r["일마_차트"]): r for _, r in matched_chart_rows.iterrows() if clean_no(r.get("일마_차트", ""))
            }
            daily_chart_ref = {
                clean_no(r["차트번호"]): r for _, r in d_card.iterrows() if clean_no(r.get("차트번호", ""))
            }
            for _, hr in h_card[~h_card["h_idx"].isin(matched_h)].iterrows():
                appr = clean_no(hr.get("승인번호", ""))
                if not appr or appr not in appr_map:
                    continue
                chart_candidates = list(appr_map.get(appr, set()))
                if len(chart_candidates) != 1:
                    continue
                chart_no = clean_no(chart_candidates[0])
                if not chart_no:
                    continue
                base_row = chart_row_ref.get(chart_no)
                if base_row is not None:
                    d_row = {
                        "내원순서": base_row.get("일마_순서", ""),
                        "성명": base_row.get("일마_성명", ""),
                        "차트번호": base_row.get("일마_차트", chart_no),
                        "카드": int(base_row.get("일마_카드", int(hr["금액"]))),
                        "d_idx": -1,
                    }
                    add("P8_차트분할보강", "🟡MED", [int(hr["h_idx"])], d_row)
                    continue

                dr = daily_chart_ref.get(chart_no)
                if dr is not None:
                    add("P8_차트분할보강", "🟡MED", [int(hr["h_idx"])], dr)

    # P9 - 누락건 소급재검토: 모든 패스 완료 후 축적된 정보로 미매칭 건 재검토
    # 이전 패스에서 정보 부족으로 누락됐지만, 이후 매칭으로 확보된 정보(카드번호, 카드사, 시간대)로 재매칭
    match_df_final = pd.DataFrame(results)
    if not match_df_final.empty:
        # 전체 매칭 결과에서 차트별 카드번호·카드사 레퍼런스 최종 구축
        final_card_refs = {}   # chart_no -> set(card_no)
        final_co_refs = {}     # chart_no -> set(card_company)
        final_time_refs = {}   # chart_no -> [time_in_minutes]
        card_rows_final = match_df_final[match_df_final.get("한솔_유형", pd.Series(dtype=str)) == "카드"] if "한솔_유형" in match_df_final.columns else pd.DataFrame()

        for _, mr in card_rows_final.iterrows():
            ch = clean_no(mr.get("일마_차트", ""))
            if not ch:
                continue
            card_no = clean_no(mr.get("한솔_카드번호", ""))[:12]
            co = str(mr.get("한솔_카드사", "")).strip()
            t = mr.get("한솔_시간", "")
            if card_no:
                final_card_refs.setdefault(ch, set()).add(card_no)
            if co:
                final_co_refs.setdefault(ch, set()).add(co)
            if t:
                p = str(t).split(":")
                if len(p) >= 2:
                    final_time_refs.setdefault(ch, []).append(int(p[0]) * 60 + int(p[1]))

        # 미매칭 일마 카드건 재검토
        for _, dr in d_card.iterrows():
            if dr["d_idx"] in matched_dc:
                continue
            chart_no = clean_no(dr["차트번호"])
            target = int(dr["카드"])
            hc = h_card[(h_card["금액"] == target) & (~h_card["h_idx"].isin(matched_h))]
            if hc.empty:
                continue

            # P9a: 카드번호 레퍼런스로 재매칭 (승인번호 없어도 카드번호로 연결)
            ref_cards = final_card_refs.get(chart_no, set())
            if ref_cards and "카드번호" in hc.columns:
                hc_ref = hc[hc["카드번호"].apply(lambda x: clean_no(x)[:12] in ref_cards)]
                if len(hc_ref) == 1:
                    add("P9a_소급_카드번호", "🟢HIGH", [int(hc_ref.iloc[0]["h_idx"])], dr,
                        note="소급재검토: 후속매칭 카드번호 일치")
                    continue
                elif len(hc_ref) > 1:
                    # 시간 근접도로 선택
                    ref_times = final_time_refs.get(chart_no, [])
                    if ref_times:
                        avg_t = sum(ref_times) / len(ref_times)
                        best = hc_ref.iloc[(hc_ref["시간_분"] - avg_t).abs().argsort()[:1]]
                        add("P9a_소급_카드번호", "🟡MED", [int(best.iloc[0]["h_idx"])], dr,
                            note="소급재검토: 카드번호+시간근접")
                        continue

            # P9b: 카드사 레퍼런스로 재매칭
            ref_cos = final_co_refs.get(chart_no, set())
            p_cos = chart_info.get(chart_no, {}).get("카드사_list", [])
            all_cos = list(ref_cos) + [c for c in p_cos if c not in ref_cos]
            if all_cos and len(hc) > 1:
                hc_co = hc[hc["카드사"].apply(lambda x: any(card_company_match(x, c) for c in all_cos))]
                if len(hc_co) == 1:
                    add("P9b_소급_카드사", "🟡MED", [int(hc_co.iloc[0]["h_idx"])], dr,
                        note="소급재검토: 후속매칭 카드사 일치")
                    continue

            # P9c: 분할결제 소급 - 같은 카드번호의 미매칭 건 2~3건 합산
            if ref_cards and "카드번호" in h_card.columns:
                for card_ref in ref_cards:
                    if dr["d_idx"] in matched_dc:
                        break
                    hc_by_card = h_card[
                        (~h_card["h_idx"].isin(matched_h)) &
                        (h_card["카드번호"].apply(lambda x: clean_no(x)[:12] == card_ref))
                    ]
                    if len(hc_by_card) < 2:
                        continue
                    items_list = hc_by_card[["h_idx", "금액", "시간_분"]].values.tolist()
                    combo = _find_split_combo(items_list, target, [2, 3], 15)
                    if combo:
                        times = [it[2] for it in combo]
                        spread = max(times) - min(times)
                        idxs = [int(it[0]) for it in combo]
                        conf = "🟢HIGH" if spread <= 5 else "🟡MED"
                        add(f"P9c_소급_분할{len(combo)}건", conf, idxs, dr,
                            note=f"소급재검토: 카드번호 {card_ref[-4:]} 분할")

        # P9d: 미매칭 한솔건 소급 - 차트 승인번호가 없지만 카드번호가 다른 차트에 매칭된 경우
        for _, hr in h_card[~h_card["h_idx"].isin(matched_h)].iterrows():
            if "카드번호" not in hr.index:
                continue
            card_no = clean_no(hr.get("카드번호", ""))[:12]
            if not card_no:
                continue
            # 이 카드번호가 매칭된 차트 확인
            linked_charts = set()
            for ch, refs in final_card_refs.items():
                if card_no in refs:
                    linked_charts.add(ch)
            if not linked_charts:
                continue
            amt = int(hr["금액"])
            for ch in linked_charts:
                dc = d_card[(d_card["차트번호"] == ch) & (d_card["카드"] == amt) & (~d_card["d_idx"].isin(matched_dc))]
                if len(dc) == 1:
                    add("P9d_소급_한솔카드번호", "🟡MED", [int(hr["h_idx"])], dc.iloc[0],
                        note=f"소급재검토: 카드번호 {card_no[-4:]} → 차트 {ch}")
                    break

    return pd.DataFrame(results), matched_h, matched_dc


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# P1: 한솔 ↔ 차트마감 카드결제 차이 (★최우선 목표)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_p1(match_df, patient, daily):
    """차트번호 단위 차트 카드금액 vs 한솔 매칭 카드금액 비교."""
    p_card = patient[patient["분류"] == "카드"].copy() if "분류" in patient.columns else patient.iloc[0:0]
    if not p_card.empty:
        p_agg = p_card.groupby("차트번호").agg(
            차트카드=("금액", "sum"),
            차트건수=("금액", "count"),
            이름=("이름", "first"),
            차트카드사=("카드사", lambda x: ",".join(sorted({str(v).strip() for v in x if str(v).strip()}))),
            차트승인번호=("승인번호목록", lambda x: ",".join(sorted({a for lst in x for a in (lst if isinstance(lst, list) else [])}))),
        ).reset_index()
    else:
        p_agg = pd.DataFrame(columns=["차트번호", "차트카드", "차트건수", "이름", "차트카드사", "차트승인번호"])

    if not match_df.empty and "한솔_유형" in match_df.columns:
        hc = match_df[match_df["한솔_유형"] == "카드"].copy()
        hc["차트번호"] = hc["일마_차트"].apply(clean_no)
        h_agg = hc.groupby("차트번호").agg(
            한솔카드=("한솔_금액", "sum"),
            한솔건수=("한솔_금액", "count"),
            한솔카드사=("한솔_카드사", lambda x: ",".join(sorted({str(v).strip() for v in x if str(v).strip()}))),
            한솔승인번호=("한솔_승인번호", lambda x: ",".join(sorted({str(v).strip() for v in x if str(v).strip()}))),
        ).reset_index()
    else:
        h_agg = pd.DataFrame(columns=["차트번호", "한솔카드", "한솔건수", "한솔카드사", "한솔승인번호"])

    d_card = daily[daily["카드"] > 0].copy()
    if not d_card.empty:
        d_agg = d_card.groupby("차트번호").agg(
            일마카드=("카드", "sum"),
            성명=("성명", "first"),
        ).reset_index()
    else:
        d_agg = pd.DataFrame(columns=["차트번호", "일마카드", "성명"])

    all_charts = set(p_agg["차트번호"]) | set(h_agg["차트번호"]) | set(d_agg["차트번호"])
    out = pd.DataFrame({"차트번호": sorted(all_charts)})
    out = out.merge(p_agg, on="차트번호", how="left").merge(h_agg, on="차트번호", how="left").merge(d_agg, on="차트번호", how="left")

    for c, d in [("차트카드", 0), ("한솔카드", 0), ("일마카드", 0),
                 ("차트건수", 0), ("한솔건수", 0)]:
        out[c] = out[c].fillna(d).astype(int)
    out["이름"] = out["이름"].fillna(out["성명"]).fillna("")
    for c in ["차트카드사", "한솔카드사", "한솔승인번호", "차트승인번호"]:
        if c in out.columns:
            out[c] = out[c].fillna("")

    # 차이 = 차트 - 한솔 (양수: 차트가 더 큼 → 한솔누락 / 음수: 한솔이 더 큼 → 차트누락)
    out["차이"] = out["차트카드"] - out["한솔카드"]
    diff = out[out["차이"] != 0].copy()
    diff["_abs"] = diff["차이"].abs()
    diff = diff.sort_values("_abs", ascending=False).drop(columns=["_abs"]).reset_index(drop=True)
    return out, diff


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# P2: 한솔 ↔ 일일마감 미매칭 카드건
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_p2(hansol, daily, matched_h, matched_dc):
    h_ok = hansol[hansol["tx_status"] == "정상"]
    h_card_um = h_ok[(~h_ok["is_현금"]) & (~h_ok["h_idx"].isin(matched_h))].copy()
    d_card_um = daily[(daily["카드"] > 0) & (~daily["d_idx"].isin(matched_dc))].copy()
    return h_card_um, d_card_um


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 합계 (한솔/일마/차트) - 환불은 카테고리별 net
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_totals(hansol, daily, daily_refund, patient):
    if hansol is not None and not hansol.empty and "tx_status" in hansol.columns:
        h_ok = hansol[hansol["tx_status"] == "정상"]
        h_cancel = hansol[hansol["tx_status"] == "취소"]
        h_card = int(h_ok[~h_ok["is_현금"]]["금액"].sum()) - int(h_cancel[~h_cancel["is_현금"]]["금액"].sum())
        h_cash = int(h_ok[h_ok["is_현금"]]["금액"].sum()) - int(h_cancel[h_cancel["is_현금"]]["금액"].sum())
    else:
        h_card = None
        h_cash = None

    def _dref(c):
        return int(daily_refund[c].sum()) if not daily_refund.empty and c in daily_refund.columns else 0
    d_card = int(daily["카드"].sum()) - _dref("카드")
    d_cash = int(daily["현금"].sum()) - _dref("현금")
    d_xfer = int(daily["이체"].sum()) - _dref("이체")
    d_plat = int(daily["플랫폼합"].sum()) - _dref("플랫폼합")

    p_normal = patient[~patient["is_취소"]] if "is_취소" in patient.columns else patient
    p_cancel = patient[patient["is_취소"]] if "is_취소" in patient.columns else patient.iloc[0:0]

    def _pby(cat):
        n = int(p_normal[p_normal["분류"] == cat]["금액"].sum()) if not p_normal.empty else 0
        c = abs(int(p_cancel[p_cancel["분류"] == cat]["금액"].sum())) if not p_cancel.empty else 0
        return n - c

    return {
        "h_card": h_card, "h_cash": h_cash,
        "d_card": d_card, "d_cashxfer": d_cash + d_xfer, "d_plat": d_plat,
        "p_card": _pby("카드"), "p_cashxfer": _pby("현금") + _pby("이체"), "p_plat": _pby("플랫폼"),
        "_has_hansol": h_card is not None,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 채널 합계 대사 (메인 산출물)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_channel_recon(totals):
    """결제채널별 합계 + 차이값 산출. 한솔 없으면 일마↔차트만."""
    has_h = totals.get("_has_hansol", True)
    h_card = totals["h_card"]
    h_cash = totals["h_cash"]
    rows = [
        {
            "채널": "카드",
            "한솔": h_card if has_h else None,
            "일마": totals["d_card"],
            "차트": totals["p_card"],
            "한솔-차트": (h_card - totals["p_card"]) if has_h else None,
            "한솔-일마": (h_card - totals["d_card"]) if has_h else None,
            "일마-차트": totals["d_card"] - totals["p_card"],
        },
        {
            "채널": "현금+이체",
            "한솔": h_cash if has_h else None,
            "일마": totals["d_cashxfer"],
            "차트": totals["p_cashxfer"],
            "한솔-차트": (h_cash - totals["p_cashxfer"]) if has_h else None,
            "한솔-일마": (h_cash - totals["d_cashxfer"]) if has_h else None,
            "일마-차트": totals["d_cashxfer"] - totals["p_cashxfer"],
        },
        {
            "채널": "플랫폼",
            "한솔": None,
            "일마": totals["d_plat"],
            "차트": totals["p_plat"],
            "한솔-차트": None,
            "한솔-일마": None,
            "일마-차트": totals["d_plat"] - totals["p_plat"],
        },
    ]
    return pd.DataFrame(rows)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 기간(다일) 대사: 한솔(거래일) ↔ 차트마감(수납일) 일자별 합계 비교
#   월 단위 파일 2개만으로 어느 날에 오류가 있는지 즉시 표시하고,
#   차이가 있는 날만 승인번호·금액 매칭으로 원인 거래를 추출한다.
#   ※ 기준 축은 차트마감의 '수납일'(진료일 아님) = 한솔 '거래일'.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _hansol_day_net(h_day, cash):
    """해당일 한솔 순매출(정상 - 취소). cash=True면 현금영수증, False면 카드."""
    sel = h_day[h_day["is_현금"] == cash] if "is_현금" in h_day.columns else h_day
    ok = int(sel[sel["tx_status"] == "정상"]["금액"].sum())
    cancel = int(sel[sel["tx_status"] == "취소"]["금액"].sum())
    return ok - cancel


def _patient_day_net(p_day, cats):
    """해당일 차트 분류(cats) 순수납액(일반 - |환불|)."""
    sel = p_day[p_day["분류"].isin(cats)]
    normal = int(sel[~sel["is_취소"]]["금액"].sum())
    cancel = abs(int(sel[sel["is_취소"]]["금액"].sum()))
    return normal - cancel


def compute_period_recon(hansol, patient):
    """일자별 한솔↔차트 대사표 DataFrame.

    컬럼: 날짜 / 한솔카드 / 차트카드 / 카드차이 / 한솔현금영수증 / 차트현금+이체 /
          현금차이 / 차트플랫폼 / 건수(한솔·차트)
    카드차이 = 한솔카드 - 차트카드, 현금차이 = 한솔현금영수증 - 차트(현금+이체).
    한솔 현금영수증은 현금·이체 중 영수증 발행분만 잡히므로 현금차이는 참고 지표
    (영수증 미발행 현금이 있는 지점은 차트가 더 클 수 있음).
    """
    h = hansol if (hansol is not None and not hansol.empty and "날짜" in hansol.columns) else pd.DataFrame()
    p = patient if (patient is not None and not patient.empty and "날짜" in patient.columns) else pd.DataFrame()
    h_days = set(h["날짜"][h["날짜"] != ""]) if not h.empty else set()
    p_days = set(p["날짜"][p["날짜"] != ""]) if not p.empty else set()
    rows = []
    for day in sorted(h_days | p_days):
        h_day = h[h["날짜"] == day] if not h.empty else pd.DataFrame(columns=h.columns if not h.empty else [])
        p_day = p[p["날짜"] == day] if not p.empty else pd.DataFrame(columns=p.columns if not p.empty else [])
        h_card = _hansol_day_net(h_day, cash=False) if not h_day.empty else 0
        h_cash = _hansol_day_net(h_day, cash=True) if not h_day.empty else 0
        p_card = _patient_day_net(p_day, ["카드"]) if not p_day.empty else 0
        p_cashx = _patient_day_net(p_day, ["현금", "이체"]) if not p_day.empty else 0
        p_plat = _patient_day_net(p_day, ["플랫폼"]) if not p_day.empty else 0
        rows.append({
            "날짜": day,
            "한솔카드": h_card, "차트카드": p_card, "카드차이": h_card - p_card,
            "한솔현금영수증": h_cash, "차트현금+이체": p_cashx, "현금차이": h_cash - p_cashx,
            "차트플랫폼": p_plat,
            "한솔건수": int(len(h_day)), "차트건수": int(len(p_day)),
        })
    return pd.DataFrame(rows)


def find_period_day_detail(hansol, patient, day):
    """차이가 있는 날의 원인 거래 추출: (미매칭 한솔 DF, 미매칭 차트 DF).

    1차: 차트 결제메모 승인번호 ↔ 한솔 승인번호 (카드·현금영수증 공통)
    2차: 동일 승인번호의 한솔 정상+취소 쌍(매출 후 당일취소 → net 0) 상쇄
    3차: 잔여 건 금액 유일 매칭(차트 |금액| = 한솔 금액, 취소↔환불 방향 일치)
    남는 건만 사람이 보면 된다(보통 0~3건).
    """
    h_day = hansol[(hansol["날짜"] == day) & (hansol["tx_status"].isin(["정상", "취소"]))].copy()
    # 플랫폼은 한솔을 거치지 않으므로 제외. '기타'는 분류 실패(=오류 후보)라 포함.
    p_day = patient[(patient["날짜"] == day)
                    & (patient["분류"].isin(["카드", "현금", "이체", "기타"]))].copy()

    # 1차: 승인번호 매칭
    h_by_appr = {}
    if "승인번호" in h_day.columns:
        for _, hr in h_day.iterrows():
            a = clean_no(hr.get("승인번호", ""))
            if a:
                h_by_appr.setdefault(a, []).append(hr["h_idx"])
    matched_h, matched_p = set(), set()
    for pi, pr in p_day.iterrows():
        for a in pr.get("승인번호목록", []):
            a = clean_no(a)
            if a in h_by_appr:
                matched_h.update(h_by_appr[a])
                matched_p.add(pi)

    un_h = h_day[~h_day["h_idx"].isin(matched_h)]
    # 2차: 동일 승인번호 정상+취소 쌍 상쇄 (당일 결제 후 당일 취소 → 차트에 흔적 없음이 정상)
    if "승인번호" in un_h.columns:
        appr_status = un_h.groupby(un_h["승인번호"].apply(clean_no))["tx_status"].agg(set)
        self_cancel = {a for a, sts in appr_status.items() if a and {"정상", "취소"} <= sts}
        if self_cancel:
            paired = un_h[un_h["승인번호"].apply(clean_no).isin(self_cancel)]
            matched_h.update(paired["h_idx"].tolist())
            un_h = h_day[~h_day["h_idx"].isin(matched_h)]
    un_p = p_day[~p_day.index.isin(matched_p)]

    # 3차: 금액 유일 매칭 (방향 일치: 한솔 정상↔차트 일반, 한솔 취소↔차트 환불)
    for pi, pr in un_p.iterrows():
        amt = abs(int(pr["금액"]))
        want_cancel = bool(pr["is_취소"])
        cand = un_h[(un_h["금액"] == amt)
                    & (un_h["tx_status"] == ("취소" if want_cancel else "정상"))]
        if len(cand) == 1:
            matched_h.add(cand.iloc[0]["h_idx"])
            matched_p.add(pi)
            un_h = h_day[~h_day["h_idx"].isin(matched_h)]
    un_p = p_day[~p_day.index.isin(matched_p)]

    h_cols = [c for c in ["날짜", "시간표시", "매입사", "카드사", "금액", "승인번호",
                          "tx_status", "is_현금", "카드번호"] if c in un_h.columns]
    p_cols = [c for c in ["날짜", "차트번호", "이름", "결제수단", "분류", "금액",
                          "is_취소", "결제메모", "수납자"] if c in un_p.columns]
    return un_h[h_cols].reset_index(drop=True), un_p[p_cols].reset_index(drop=True)


def _amount_typo_kind(a, b):
    """두 금액이 '같은 건의 입력 오타' 관계로 보이는지 판정 → 설명 문자열(아니면 "").

    숫자 문자열 편집거리 1(OSA: 치환/삽입/삭제/인접전치)만 인정해 우연 일치를 배제한다.
      - 자릿수 추가/누락 : 50,000 ↔ 500,000 (0 하나 더/덜 입력)
      - 한 자리 오타     : 40,000 ↔ 49,000
      - 인접 자리 뒤바뀜 : 120,000 ↔ 210,000
    """
    a, b = abs(_safe_int(a)), abs(_safe_int(b))
    if a == 0 or b == 0 or a == b:
        return ""
    sa, sb = str(a), str(b)
    if _verif_osa(sa, sb) != 1:
        return ""
    if len(sa) != len(sb):
        return "자릿수 추가/누락 의심"
    if sorted(sa) == sorted(sb):
        return "인접 자리 뒤바뀜 의심"
    return "한 자리 오타 의심"


_PERIOD_PAIR_COLS = ["차트번호", "환자", "차트금액", "한솔금액", "차이(한솔-차트)",
                     "한솔시각", "한솔카드사", "한솔승인번호", "추정원인"]


def pair_period_typo_suspects(un_h, un_p):
    """미설명 한솔↔차트 잔여 건에서 '같은 건의 입력 오류'로 의심되는 쌍을 자동 특정.

    승인번호·금액 매칭(find_period_day_detail) 후 남은 건은 보통 소수의 진짜 오류다.
    그중 아래 패턴의 쌍을 1:1로 묶어 '어느 건(어떤 환자·어떤 거래)이 오류인지'를
    바로 짚는다 — 별개의 누락 2건으로 오인하지 않도록:
      ① 금액 오타 패턴 (자릿수 추가/누락 · 한 자리 오타 · 자리 뒤바뀜, 같은 방향)
      ② 금액 동일·방향 상이 (한솔 정상 ↔ 차트 환불 등 — 환불방향/부호 오류 의심)
    반환: DataFrame(_PERIOD_PAIR_COLS) — 한솔은 PG 원본이므로 보통 차트 쪽이 오류.
    """
    if (un_h is None or un_h.empty or un_p is None or un_p.empty):
        return pd.DataFrame(columns=_PERIOD_PAIR_COLS)
    rows = []
    used_h = set()
    for _, pr in un_p.iterrows():
        p_amt = _safe_int(pr.get("금액", 0))
        want_cancel = bool(pr.get("is_취소", False))
        best = None  # (우선순위, 편집거리, |금액차|, h_index, h_row, 원인)
        for hi, hr in un_h.iterrows():
            if hi in used_h:
                continue
            h_amt = _safe_int(hr.get("금액", 0))
            h_cancel = str(hr.get("tx_status", "")) == "취소"
            if h_cancel == want_cancel:
                kind = _amount_typo_kind(p_amt, h_amt)
                if not kind:
                    continue
                cand = (0, _verif_osa(str(abs(p_amt)), str(abs(h_amt))),
                        abs(abs(h_amt) - abs(p_amt)), hi, hr, f"금액 {kind}")
            elif abs(h_amt) == abs(p_amt):
                cand = (1, 0, 0, hi, hr, "환불방향 불일치 의심(금액 동일·방향 상이)")
            else:
                continue
            if best is None or cand[:3] < best[:3]:
                best = cand
        if best is None:
            continue
        _, _, _, hi, hr, cause = best
        used_h.add(hi)
        h_signed = (-abs(_safe_int(hr.get("금액", 0)))
                    if str(hr.get("tx_status", "")) == "취소" else _safe_int(hr.get("금액", 0)))
        rows.append({
            "차트번호": pr.get("차트번호", ""),
            "환자": pr.get("이름", ""),
            "차트금액": p_amt,
            "한솔금액": h_signed,
            "차이(한솔-차트)": h_signed - p_amt,
            "한솔시각": hr.get("시간표시", ""),
            "한솔카드사": hr.get("카드사", ""),
            "한솔승인번호": hr.get("승인번호", ""),
            "추정원인": cause,
        })
    return pd.DataFrame(rows, columns=_PERIOD_PAIR_COLS)


def _daily_day_channel_totals(daily, daily_refund=None):
    """일일마감(+환불 행) 하루치 → 채널별 net 합계 {'카드','현금이체','플랫폼'}."""
    def _s(df, c):
        if df is None or getattr(df, "empty", True) or c not in df.columns:
            return 0
        return int(df[c].sum())
    return {
        "카드": _s(daily, "카드") - _s(daily_refund, "카드"),
        "현금이체": (_s(daily, "현금") + _s(daily, "이체")
                  - _s(daily_refund, "현금") - _s(daily_refund, "이체")),
        "플랫폼": _s(daily, "플랫폼합") - _s(daily_refund, "플랫폼합"),
    }


def augment_period_with_gsheet(table, patient, url_or_id, cache=None,
                               loader=None, progress=None):
    """기간 분석표(table)에 선택 지점의 구글시트 일일마감 합계·차이 컬럼을 추가한다.

    날짜마다 해당 일일마감 탭을 불러와 카드/현금+이체/플랫폼 net 합계를 구하고
    차트마감과의 차이(일마-차트)를 계산한다. 아래 사유의 날짜는 **비교에서 제외**하고
    사유를 남긴다(나머지 날짜는 정상 분석):
      - '시트없음'      : 해당 날짜 탭이 스프레드시트에 없음 → 입력 여부 확인 필요
      - '접근권한없음'   : 공유 설정 문제
      - '읽기실패'      : 네트워크/양식 인식 실패
      - '대조불일치(…)' : 차트번호 겹침이 기준 미만 → 다른 지점이거나 날짜를 잘못
                          입력한 시트로 판단(오차가 과도) → 오염 방지 위해 제외
    일마에 차트번호가 없어 환자단위 대조가 불가한 날은 합계 비교는 수행하되
    '일마비고'에 표시만 한다.

    반환: (table, skipped {날짜: 사유}, day_data {날짜: (daily, refund)})
    """
    from datetime import date as _date

    if loader is None:
        loader = load_gsheet_daily
    if cache is None:
        cache = {}
    table = table.copy()
    n = len(table)
    gs_cols = {"일마카드": [], "일마현금+이체": [], "일마플랫폼": [],
               "일마-차트카드차이": [], "일마-차트현금차이": [], "일마비고": []}
    skipped, day_data = {}, {}

    for i, (_, row) in enumerate(table.iterrows()):
        day = str(row["날짜"])
        if progress:
            progress(i, n, day)
        reason, note = "", ""
        daily, refund = None, None
        try:
            d_obj = _date.fromisoformat(day)
        except ValueError:
            reason = "날짜형식오류"
            d_obj = None
        if not reason:
            try:
                raw, _tab = loader(url_or_id, d_obj, cache=cache)
                daily, refund = parse_daily(raw)
                if daily is None or daily.empty:
                    reason = "시트양식인식실패"
            except LookupError:
                reason = "시트없음"
            except PermissionError:
                reason = "접근권한없음"
            except Exception:
                reason = "읽기실패"
        if not reason:
            p_day = (patient[patient["날짜"] == day]
                     if patient is not None and not patient.empty and "날짜" in patient.columns
                     else pd.DataFrame())
            status, _msg, info = cross_check_daily_patient(daily, p_day)
            if status == "block":
                # 다른 지점/잘못 입력된 날짜 시트로 판단 → 합계가 오염되므로 제외
                reason = f"대조불일치(차트번호 일치율 {info['rate'] * 100:.0f}%)"
            elif status == "warn":
                note = "차트번호 없음(환자단위 대조 불가·합계만 비교)"
        if reason:
            skipped[day] = reason
            for k in gs_cols:
                gs_cols[k].append(reason if k == "일마비고" else None)
            continue
        t = _daily_day_channel_totals(daily, refund)
        day_data[day] = (daily, refund)
        gs_cols["일마카드"].append(t["카드"])
        gs_cols["일마현금+이체"].append(t["현금이체"])
        gs_cols["일마플랫폼"].append(t["플랫폼"])
        gs_cols["일마-차트카드차이"].append(t["카드"] - int(row["차트카드"]))
        gs_cols["일마-차트현금차이"].append(t["현금이체"] - int(row["차트현금+이체"]))
        gs_cols["일마비고"].append(note)

    for k, v in gs_cols.items():
        table[k] = v
    return table, skipped, day_data


def _rank_key(amt, channel_gap):
    """채널 차이값 근처 금액을 우선 + 동률시 큰 금액 우선.
    예: gap=-27,400일 때 27,600(차이200) > 714,000(차이686,600).
    """
    a = abs(int(amt))
    g = abs(int(channel_gap)) if channel_gap else 0
    return (abs(a - g), -a)


def _chart_method_pivots(patient, daily, daily_refund=None):
    """차트번호별 결제수단(카드/현금/이체/플랫폼) 금액을 차트·일마 각각 pivot으로 구축.

    빈 차트번호('')는 제외. 분류가 파일마다 다른 경우(예: 차트=현금·일마=카드)도
    누락 없이 비교하기 위해, 결제수단 교집합이 아닌 전 차트번호 합집합 기준으로 사용한다.

    daily_refund(일마 환불/취소 행)를 주면 같은 차트번호에서 차감해 net으로 비교한다.
    차트마감은 결제취소를 음수 행으로 합산하므로, 일마 쪽도 환불을 빼야 '당일 결제 후
    당일 환불'(양쪽 net 0) 환자가 허위 차이로 잡히지 않는다 (build_verification과 동일 원칙).

    일마(일일마감)에 차트번호 컬럼이 없는 export 형식(성명만 존재)도 있으므로,
    일마 행에 차트번호가 없으면 성명 → 차트(EMR) 이름맵으로 차트번호를 보강 링크한다.
    (동명이인 등 이름이 여러 차트에 매핑되면 모호하므로 링크하지 않고 건너뜀 → 허위 불일치 방지)

    유형B(차트번호 오타) 쌍은 마지막에 병합한다: 차트마감(EMR)은 차트번호가 시스템
    자동기재라 오입력될 수 없으므로, 한쪽에만 존재하는 번호쌍이 '총액 동일 +
    (이름유사 or 번호오타)'면 일일마감 쪽 수기오류로 보고 일마 pivot을 차트마감
    번호로 귀속시킨다 → 금액이 맞는 단순 번호오타는 채널/환자 차이 목록에 잡히지
    않고 [데이터검증] 유형B(번호 정정 안내)로만 보고된다.

    find_channel_suspects / build_ai_text / build_3way_table 공용.
    반환: (p_pivot, d_pivot) — 각 {차트번호: {"카드","현금","이체","플랫폼"}}
    """
    empty = {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0}
    pp, dp = {}, {}
    p_names, d_names = {}, {}
    name2chart = {}  # 이름 → {차트번호} (유일할 때만 일마 링크 키로 사용)
    if patient is not None and not patient.empty and "분류" in patient.columns:
        for _, r in patient.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            cat = str(r.get("분류", ""))
            d = pp.setdefault(ch, dict(empty))
            if cat in d:
                d[cat] += _safe_int(r.get("금액", 0))
            nm = clean_name(r.get("이름", ""))
            if nm:
                p_names.setdefault(ch, nm)
                name2chart.setdefault(nm, set()).add(ch)
    # 이름이 단 하나의 차트번호에만 대응할 때만 일마 보강 링크에 사용
    name2chart_unique = {nm: next(iter(s)) for nm, s in name2chart.items() if len(s) == 1}

    for frame, sign in [(daily, 1), (daily_refund, -1)]:
        if frame is None or frame.empty:
            continue
        for _, r in frame.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                # 일마에 차트번호가 없으면 성명으로 차트(EMR) 링크 시도
                ch = name2chart_unique.get(clean_name(r.get("성명", "")), "")
            if not ch:
                continue
            d = dp.setdefault(ch, dict(empty))
            d["카드"] += sign * _safe_int(r.get("카드", 0))
            d["현금"] += sign * _safe_int(r.get("현금", 0))
            d["이체"] += sign * _safe_int(r.get("이체", 0))
            d["플랫폼"] += sign * _safe_int(r.get("플랫폼합", 0))
            nm = clean_name(r.get("성명", ""))
            if nm:
                d_names.setdefault(ch, nm)

    _merge_typeB_pairs_into_pivots(pp, dp, p_names, d_names)
    return pp, dp


def _merge_typeB_pairs_into_pivots(pp, dp, p_names, d_names):
    """유형B(일일마감 차트번호 수기오타) 쌍을 일마 pivot에서 차트마감 번호로 병합.

    build_verification의 유형B 페어링과 동일 조건(총액 동일 + 이름유사 or 번호오타).
    병합 후 분배까지 같으면 차이 0으로 사라지고, 분배가 다르면 결제수단 불일치로
    정상 포착된다. 반환: {일일마감기재번호: 차트마감번호}"""
    p_only = [ch for ch in pp if ch not in dp]
    d_only = [ch for ch in dp if ch not in pp]
    remap = {}
    used = set()
    for pch in sorted(p_only):
        p_total = sum(pp[pch].values())
        if p_total == 0:
            continue
        best, best_score = None, -1
        for dch in sorted(d_only):
            if dch in used or sum(dp[dch].values()) != p_total:
                continue
            ns = _verif_name_sim(p_names.get(pch, ""), d_names.get(dch, ""))
            ts = _verif_chart_typo_loose(pch, dch)
            if not (ns or ts):
                continue
            score = (2 if ns else 0) + (1 if _verif_chart_typo_adjacent(pch, dch) else 0)
            if score > best_score:
                best_score, best = score, dch
        if best is not None:
            used.add(best)
            remap[best] = pch
    for dch, pch in remap.items():
        dp[pch] = dp.pop(dch)
    return remap


def _pivot_presence_tag(ch, pp, dp):
    """차이 건이 어느 파일에 존재하는지 명시 태그.

    차트(EMR)가 세무 기준원장이므로 '어느 쪽에만 있는 수납인지'가 검토 방향을
    결정한다 — 일일마감에만 존재 = 차트 누락(세무위험) 의심,
    차트마감에만 존재 = 일일마감 누락 의심."""
    in_p, in_d = ch in pp, ch in dp
    if in_p and not in_d:
        return "[차트마감에만 존재]"
    if in_d and not in_p:
        return "[일일마감에만 존재]"
    return "[양쪽 존재·금액 상이]"


def _hansol_card_by_chart(hansol, patient):
    """승인번호를 다리로 한솔(PG) 카드금액을 차트번호에 귀속한다.

    일마(일일마감)·한솔에 차트번호가 없어도 한솔 승인번호 ↔ 차트(EMR) 결제메모 승인번호로
    직접 연결한다. 승인번호가 정확히 한 차트에만 대응할 때만 귀속(공동결제·미링크는 제외)해
    허위 차이를 방지한다.
    반환: {차트번호: (한솔카드합, 건수)} — 링크 불가 차트는 미포함(=한솔금액 unknown).
    """
    if (hansol is None or hansol.empty or patient is None or patient.empty
            or "분류" not in patient.columns or "승인번호목록" not in patient.columns
            or "tx_status" not in hansol.columns):
        return {}

    def _ak(a):
        aa = clean_no(a)
        return aa[-8:] if len(aa) >= 8 else aa

    appr_charts: dict = {}  # 승인번호(8자리) → {차트번호}
    for _, pr in patient[patient["분류"] == "카드"].iterrows():
        ch = clean_no(pr.get("차트번호", ""))
        if not ch:
            continue
        al = pr.get("승인번호목록", [])
        if not isinstance(al, list):
            continue
        for a in al:
            if len(clean_no(a)) >= 4:
                appr_charts.setdefault(_ak(a), set()).add(ch)

    h_ok = hansol[hansol["tx_status"] == "정상"]
    h_card = h_ok[~h_ok["is_현금"]] if "is_현금" in h_ok.columns else h_ok
    acc: dict = {}  # 차트번호 → [금액합, 건수]
    for _, hr in h_card.iterrows():
        a = clean_no(hr.get("승인번호", ""))
        if len(a) < 4:
            continue
        charts = appr_charts.get(_ak(a))
        if not charts or len(charts) != 1:  # 미링크 또는 공동결제(2차트 이상) → 제외
            continue
        ch = next(iter(charts))
        e = acc.setdefault(ch, [0, 0])
        e[0] += int(hr["금액"])
        e[1] += 1
    return {ch: (v[0], v[1]) for ch, v in acc.items()}


def find_channel_suspects(channel, hansol, daily, patient, totals=None, top_n=12,
                          daily_refund=None):
    """채널 차이를 설명할 후보 거래 추출 (multiset diff + 승인번호 cross-match 기반).

    우선순위:
      ★★ 동일환자 확정(gap일치) — 승인번호로 환자 확정 + 한솔-일마 차이 = channel gap 완전 일치
      ★★ 동일환자 확정         — 승인번호로 환자 확정 + 한솔-일마 금액 불일치
      ★ 차이값 정확매칭 페어   — counter diff 기반 금액 페어가 gap과 수학적으로 일치
      한솔에만 존재★/일마에만 존재★ — gap 근접 금액
      차트↔일마 카드차이★      — 전 결제수단 pivot 비교로 누락·결제수단 오기재 포착(gap근접 시 ★)
    """
    from collections import Counter
    suspects = []

    # 차트번호 → 환자이름 조회맵 (daily 성명 우선, patient 이름 보완)
    name_map = {}
    if not daily.empty and "차트번호" in daily.columns and "성명" in daily.columns:
        for _, row in daily[["차트번호", "성명"]].drop_duplicates("차트번호").iterrows():
            ch_no = str(row["차트번호"]).strip()
            nm = str(row["성명"]).strip()
            if ch_no and nm and nm != "nan":
                name_map[ch_no] = nm
    if not patient.empty and "차트번호" in patient.columns and "이름" in patient.columns:
        for _, row in patient[["차트번호", "이름"]].drop_duplicates("차트번호").iterrows():
            ch_no = str(row["차트번호"]).strip()
            nm = str(row["이름"]).strip()
            if ch_no and nm and nm != "nan" and ch_no not in name_map:
                name_map[ch_no] = nm

    # 일마 금액 → 환자이름 후보 (한솔에만 존재 건의 환자 추정용)
    d_amt_to_names: dict = {}
    if not daily.empty and "카드" in daily.columns and "성명" in daily.columns:
        for _, row in daily.iterrows():
            amt_key = int(row["카드"]) if row["카드"] > 0 else None
            nm = str(row.get("성명", "")).strip()
            if amt_key and nm and nm != "nan":
                d_amt_to_names.setdefault(amt_key, [])
                if nm not in d_amt_to_names[amt_key]:
                    d_amt_to_names[amt_key].append(nm)

    if channel == "카드":
        has_hansol = hansol is not None and not hansol.empty and "tx_status" in hansol.columns

        # 채널 차이값 기준 (한솔-일마 우선, 없으면 일마-차트)
        gap = 0
        if totals:
            h_c = totals.get("h_card")
            if h_c is not None:
                gap = h_c - totals.get("d_card", 0)
                if gap == 0:
                    gap = h_c - totals.get("p_card", 0)
            if gap == 0:
                gap = totals.get("d_card", 0) - totals.get("p_card", 0)

        if has_hansol:
            h_ok = hansol[hansol["tx_status"] == "정상"] if "tx_status" in hansol.columns else hansol
            h_card = h_ok[~h_ok["is_현금"]] if "is_현금" in h_ok.columns else h_ok
            h_amts = [int(x) for x in h_card["금액"].tolist()] if not h_card.empty else []
            d_amts = [int(x) for x in daily.loc[daily["카드"] > 0, "카드"].tolist()] if not daily.empty else []

            ch_h, ch_d = Counter(h_amts), Counter(d_amts)
            only_h = ch_h - ch_d
            only_d = ch_d - ch_h

            # 한솔에만 (일마에 누락 의심) — 차이값 근접도 우선 정렬
            h_remaining = h_card.copy()
            for amt, cnt in sorted(only_h.items(), key=lambda x: _rank_key(x[0], gap))[:top_n]:
                rows = h_remaining[h_remaining["금액"] == amt].head(cnt)
                for _, r in rows.iterrows():
                    cn = str(r.get("카드번호", ""))
                    cn_tail = cn[-5:] if cn and cn != "nan" else ""
                    near = abs(abs(amt) - abs(gap)) <= max(1000, abs(gap) * 0.05) if gap else False
                    tag = "한솔에만 존재★" if near else "한솔에만 존재"
                    name_candidates = d_amt_to_names.get(int(amt), [])
                    patient_hint = "·".join(name_candidates[:2]) + ("?" if name_candidates else "")
                    suspects.append({
                        "출처": tag,
                        "환자(추정)": patient_hint,
                        "금액": int(amt),
                        "단서": f"{r.get('시간표시','')} 말미{cn_tail} 승인{r.get('승인번호','')} {str(r.get('카드사',''))[:6]}",
                        "조치": "일마/차트에 같은 금액 누락 또는 부분취소 가능성",
                    })

            # 일마에만 (PG미경유 or 일마오기재)
            for amt, cnt in sorted(only_d.items(), key=lambda x: _rank_key(x[0], gap))[:top_n]:
                rows = daily[(daily["카드"] == amt)].head(cnt)
                for _, r in rows.iterrows():
                    near = abs(abs(amt) - abs(gap)) <= max(1000, abs(gap) * 0.05) if gap else False
                    tag = "일마에만 존재★" if near else "일마에만 존재"
                    nm = str(r.get("성명", "")).strip()
                    suspects.append({
                        "출처": tag,
                        "환자": nm,
                        "금액": int(amt),
                        "단서": f"차트{r['차트번호']}",
                        "조치": "PG 승인내역 없음 → 결제수단 오기재 또는 미수납",
                    })

            # 차이값 정확히 일치하는 후보 페어 (한솔의 X - 일마의 Y = gap)
            if gap != 0 and only_h and only_d:
                pair_inserted = set()
                for h_amt in list(only_h.keys())[:20]:
                    for d_amt in list(only_d.keys())[:20]:
                        if (h_amt - d_amt) == gap:
                            pair_key = (h_amt, d_amt)
                            if pair_key in pair_inserted:
                                continue
                            pair_inserted.add(pair_key)
                            d_names = "·".join(d_amt_to_names.get(int(d_amt), [])[:3])
                            h_rows = h_card[h_card["금액"] == h_amt].head(1)
                            h_extra = ""
                            if not h_rows.empty:
                                hr = h_rows.iloc[0]
                                cn = str(hr.get("카드번호", ""))
                                cn_tail = cn[-5:] if cn and cn != "nan" else ""
                                h_extra = f" 한솔건:{hr.get('시간표시','')} 말미{cn_tail} 승인{hr.get('승인번호','')}"
                            suspects.insert(0, {
                                "출처": "★ 차이값 정확매칭 페어",
                                "환자(추정)": d_names,
                                "금액": gap,
                                "단서": f"한솔 {h_amt:,}원 - 일마 {d_amt:,}원 = {gap:+,}원(gap 정확일치){h_extra}",
                                "조치": f"일마 {d_amt:,}원({d_names}) 결제수단 오기재 또는 한솔 {h_amt:,}원 부분취소 의심 — 즉시 확인",
                            })

            # ★★ 승인번호 cross-match: 승인번호별 [한솔 카드합] vs [차트 카드합] 비교 (한솔 있을 때만)
            # 분할기재(한 결제가 차트 여러 줄)·공동결제(한 카드결제가 여러 환자)로 흩어져도
            # 승인번호 단위 합계로 비교하므로 허위 불일치가 생기지 않는다. (일마 차트번호 유무 무관)
            if not patient.empty and "승인번호목록" in patient.columns and "분류" in patient.columns:
                p_card_rows = patient[patient["분류"] == "카드"]

                def _ak(a):
                    aa = clean_no(a)
                    return aa[-8:] if len(aa) >= 8 else aa

                # 차트: 승인번호(8자리 suffix) → 카드합·이름·차트 집합
                c_appr: dict = {}
                ambiguous: set = set()
                for _, pr in p_card_rows.iterrows():
                    ch = clean_no(pr.get("차트번호", ""))
                    nm = clean_name(pr.get("이름", "")) or ch
                    amt = _safe_int(pr.get("금액", 0))
                    appr_list = pr.get("승인번호목록", [])
                    keys = ({_ak(a) for a in appr_list if len(clean_no(a)) >= 4}
                            if isinstance(appr_list, list) else set())
                    if len(keys) > 1:
                        # 한 행에 승인번호가 여러 개면 금액 귀속이 모호 → 비교 제외(허위방지)
                        ambiguous |= keys
                        continue
                    for key in keys:
                        e = c_appr.setdefault(key, {"amt": 0, "names": set(), "charts": set()})
                        e["amt"] += amt
                        if nm:
                            e["names"].add(nm)
                        if ch:
                            e["charts"].add(ch)

                # 한솔: 승인번호(8자리 suffix) → 카드합 + 대표행(단서용)
                h_appr: dict = {}
                for _, hr in h_card.iterrows():
                    a = clean_no(hr.get("승인번호", ""))
                    if len(a) < 4:
                        continue
                    e = h_appr.setdefault(_ak(a), {"amt": 0, "row": hr})
                    e["amt"] += int(hr["금액"])

                star2: list = []
                for key in set(h_appr) & set(c_appr):
                    if key in ambiguous:
                        continue
                    h_total, c_total = h_appr[key]["amt"], c_appr[key]["amt"]
                    if h_total == c_total:
                        continue
                    diff = h_total - c_total
                    gap_match = gap != 0 and diff == gap
                    hr = h_appr[key]["row"]
                    cn = str(hr.get("카드번호", ""))
                    cn_tail = cn[-5:] if cn and cn != "nan" else ""
                    t = str(hr.get("시간표시", ""))
                    co = str(hr.get("카드사", ""))[:6]
                    names = "·".join(sorted(c_appr[key]["names"])[:3])
                    charts = ",".join(sorted(c_appr[key]["charts"]))
                    tag = "★★ 동일환자 확정(gap일치)" if gap_match else "★★ 동일환자 확정"
                    star2.append({
                        "출처": tag,
                        "환자": names or charts,
                        "금액": diff,
                        "단서": (
                            f"승인{key} {t} 말미{cn_tail} {co} | "
                            f"한솔 {h_total:,} vs 차트 {c_total:,}" + (f" (차트{charts})" if charts else "")
                        ),
                        "조치": (
                            f"{'★gap완전일치 → ' if gap_match else ''}"
                            f"환자 {names}(차트{charts}) 한솔·차트 카드금액 불일치 — 즉시 수정"
                        ),
                    })
                if star2:
                    star2.sort(key=lambda x: (0 if "gap일치" in x["출처"] else 1, -abs(x["금액"])))
                    suspects = star2 + suspects

        # 차트 vs 일마 카드금액 차이 — 전 결제수단 pivot 기반(분류 무관, 한솔 유무 무관)
        # 교집합이 아닌 합집합을 사용해 "차트=현금·일마=카드" 같은 결제수단 오기재까지 포착
        if not patient.empty and not daily.empty and "분류" in patient.columns:
            pp, dp = _chart_method_pivots(patient, daily, daily_refund)
            empty = {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0}
            mismatches = []
            for ch in (set(pp) | set(dp)):
                p = pp.get(ch, empty)
                d = dp.get(ch, empty)
                if d["카드"] == p["카드"]:
                    continue
                mismatches.append((ch, p, d, d["카드"] - p["카드"]))
            # 채널 gap 근접도 우선 정렬
            mismatches.sort(key=lambda x: _rank_key(x[3], gap))
            for ch, p, d, diff in mismatches[:top_n]:
                near = abs(abs(diff) - abs(gap)) <= max(1000, abs(gap) * 0.05) if gap else False
                # 카드 차이분이 어느 파일·어느 결제수단으로 옮겨졌는지(오기재) 자동 탐지
                moved = ""
                for m in ("현금", "이체", "플랫폼"):
                    if diff != 0 and (p[m] - d[m]) == diff:
                        side = "차트" if p[m] > d[m] else "일마"
                        moved = f" → {side}가 {m}({max(p[m], d[m]):,})로 기재(결제수단 오기재 의심)"
                        break
                loc = _pivot_presence_tag(ch, pp, dp)
                suspects.append({
                    "출처": "차트↔일마 카드차이★" if near else "차트↔일마 카드차이",
                    "환자": name_map.get(str(ch).strip(), ""),
                    "금액": diff,
                    "단서": (
                        f"{loc} 차트{ch} 차트(카{p['카드']:,}/현{p['현금']:,}/이{p['이체']:,}) "
                        f"vs 일마(카{d['카드']:,}/현{d['현금']:,}/이{d['이체']:,}){moved}"
                    ),
                    "조치": ("일일마감에만 있는 수납 — 차트(세무 기준) 누락인지, 일마 오기재인지 확인"
                             if loc == "[일일마감에만 존재]"
                             else "차트마감에만 있는 수납 — 일일마감 누락인지, 차트 오기재인지 확인"
                             if loc == "[차트마감에만 존재]"
                             else "결제수단(카드↔현금↔이체) 오기재 또는 카드결제 누락/중복 확인"),
                })

    elif channel == "현금+이체":
        if not patient.empty and not daily.empty:
            pp, dp = _chart_method_pivots(patient, daily, daily_refund)
            empty = {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0}
            mismatches = []
            for ch in (set(pp) | set(dp)):
                p = pp.get(ch, empty)
                d = dp.get(ch, empty)
                pv, dv = p["현금"] + p["이체"], d["현금"] + d["이체"]
                if pv != dv:
                    mismatches.append((ch, p, d, dv - pv))
            mismatches.sort(key=lambda x: -abs(x[3]))
            for ch, p, d, diff in mismatches[:top_n]:
                loc = _pivot_presence_tag(ch, pp, dp)
                suspects.append({
                    "출처": "차트↔일마 현금/이체차이",
                    "환자": name_map.get(str(ch).strip(), ""),
                    "금액": diff,
                    "단서": (
                        f"{loc} 차트{ch} 차트(현{p['현금']:,}/이{p['이체']:,}/카{p['카드']:,}) "
                        f"vs 일마(현{d['현금']:,}/이{d['이체']:,}/카{d['카드']:,})"
                    ),
                    "조치": "현금↔이체↔카드 오기재 확인",
                })

    elif channel == "플랫폼":
        if not patient.empty and not daily.empty:
            pp, dp = _chart_method_pivots(patient, daily, daily_refund)
            mismatches = []
            for ch in (set(pp) | set(dp)):
                pv = pp.get(ch, {}).get("플랫폼", 0)
                dv = dp.get(ch, {}).get("플랫폼", 0)
                if pv != dv:
                    mismatches.append((ch, pv, dv, dv - pv))
            mismatches.sort(key=lambda x: -abs(x[3]))
            for ch, pv, dv, diff in mismatches[:top_n]:
                loc = _pivot_presence_tag(ch, pp, dp)
                suspects.append({
                    "출처": "차트↔일마 플랫폼차이",
                    "환자": name_map.get(str(ch).strip(), ""),
                    "금액": diff,
                    "단서": f"{loc} 차트{ch} / 차트플랫폼={pv:,} 일마={dv:,}",
                    "조치": ("일일마감에만 있는 수납 — 차트(세무 기준) 누락인지, 일마 오기재인지 확인"
                             if loc == "[일일마감에만 존재]"
                             else "일일마감 누락인지, 차트 오기재인지 확인"
                             if loc == "[차트마감에만 존재]"
                             else "플랫폼 종류/금액 오기재 확인"),
                })

    return suspects


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI 분석 텍스트 (3-way 통합 데이터 ~6~8KB / ~2~3K토큰)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _safe_int(v, default=0):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return default
        return int(v)
    except Exception:
        return default


def build_ai_text(hansol, daily, daily_refund, patient, channel_df,
                  p1_full, h_um, d_um, suspects_by_channel,
                  totals=None, verif=None, max_chars=8000):
    """3개 파일(한솔/일마/차트)을 차트번호·승인번호 join으로 유기적 연결한 통합 raw 구조.
    AI가 사전요약을 rephrase하는게 아니라 cross-file 추적분석을 수행할 수 있게 한다.

    섹션 구성:
      [META]              — 파일별 건수·총액 (방향감각)
      [채널대사]           — 채널×파일 합계·차이 (분석 진입점)
      [★★승인번호확정]    — 한솔↔차트 승인번호 매핑 + 금액불일치 (코드추출 확정단서)
      [차트번호3way통합]   — 한 환자의 차트/일마/한솔 채널별 금액을 한 줄로 (오기재 패턴추적)
      [한솔PG-only]        — PG미매칭 + 동일금액 일마환자 hint (누락추정)
      [일마front-only]     — front미매칭 카드 (PG에 없음 = 결제수단 오기재 의심)
      [차트환불/일마환불]   — 합계에 영향 주는 음수 행

    구분자 파이프(|). 숫자 천단위 콤마. 차이가 0이면 META+채널대사만 반환.
    """
    has_hansol = hansol is not None and not hansol.empty

    def _f(v):
        if v is None:
            return "-"
        try:
            if pd.isna(v):
                return "-"
        except Exception:
            pass
        return f"{int(v):,}"

    L = []

    # ── [META] ──────────────────────────────────────
    L.append("[META]")
    if has_hansol:
        h_ok = hansol[hansol["tx_status"] == "정상"]
        h_can = hansol[hansol["tx_status"] == "취소"]
        h_um_n = len(h_um) if h_um is not None and not h_um.empty else 0
        h_um_sum = int(h_um["금액"].sum()) if h_um_n else 0
        L.append(
            f"한솔: 정상={len(h_ok)}건/{int(h_ok['금액'].sum()):,}원 · "
            f"취소={len(h_can)}건/{int(h_can['금액'].sum()):,}원 · "
            f"PG미매칭={h_um_n}건/{h_um_sum:,}원"
        )
    d_um_n = len(d_um) if d_um is not None and not d_um.empty else 0
    d_um_sum = int(d_um["카드"].sum()) if d_um_n else 0
    d_ref_n = len(daily_refund) if daily_refund is not None and not daily_refund.empty else 0
    d_ref_sum = int(daily_refund["총액"].sum()) if d_ref_n and "총액" in daily_refund.columns else 0
    L.append(f"일마: 총{len(daily)}건 · 카드미매칭={d_um_n}건/{d_um_sum:,}원 · 환불행={d_ref_n}건/{d_ref_sum:,}원")
    if "분류" in patient.columns:
        p_cnt = patient["분류"].value_counts().to_dict()
        p_can_n = int(patient["is_취소"].sum()) if "is_취소" in patient.columns else 0
        L.append(
            f"차트: 카드={p_cnt.get('카드',0)} 현금={p_cnt.get('현금',0)} "
            f"이체={p_cnt.get('이체',0)} 플랫폼={p_cnt.get('플랫폼',0)} (취소행={p_can_n})"
        )

    # ── [채널대사] ──────────────────────────────────
    L.append("\n[채널대사]")
    L.append("채널|한솔|일마|차트|한-차|한-일|일-차")
    for _, r in channel_df.iterrows():
        L.append(
            f"{r['채널']}|{_f(r['한솔'])}|{_f(r['일마'])}|{_f(r['차트'])}|"
            f"{_f(r['한솔-차트'])}|{_f(r['한솔-일마'])}|{_f(r['일마-차트'])}"
        )

    # 차이 zero 단축 — 단, 채널 합계가 맞아도 환자별 금액이 +/-로 상쇄돼 숨은 불일치가
    # 있을 수 있으므로(상쇄쌍 누락 방지), 채널차이뿐 아니라 환자별(차트번호별) 일마↔차트
    # 불일치 유무까지 확인한 뒤 종료를 결정한다.
    diff_cols = ["한솔-차트", "한솔-일마", "일마-차트"] if has_hansol else ["일마-차트"]
    has_nonzero = False
    for _, r in channel_df.iterrows():
        for c in diff_cols:
            v = r[c]
            try:
                if v is not None and not pd.isna(v) and int(v) != 0:
                    has_nonzero = True
                    break
            except Exception:
                pass
        if has_nonzero:
            break

    # 환자별(차트번호별) 일마↔차트 금액 불일치 존재 여부 (채널 합계엔 상쇄돼 안 보일 수 있음).
    # 아래 [3-way 통합] 섹션과 동일한 pivot을 한 번만 만들어 재사용한다.
    p_pivot, d_pivot = _chart_method_pivots(patient, daily, daily_refund)
    _empty_m = {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0}
    has_patient_diff = False
    for ch in (set(p_pivot) | set(d_pivot)):
        p = p_pivot.get(ch, _empty_m)
        d = d_pivot.get(ch, _empty_m)
        if (d["카드"] != p["카드"] or d["현금"] != p["현금"]
                or d["이체"] != p["이체"] or d["플랫폼"] != p["플랫폼"]):
            has_patient_diff = True
            break

    # ── 데이터검증(유형B/C1/C2/C3) 존재 여부 — 코드가 확정한 오입력 의심 오차 ──
    # 유형B(번호 오타)·C3(결제수단 분배)는 총액이 같아 채널/환자 합계엔 안 잡히는
    # 경우가 있으므로, 합계가 맞아도 검증 결과가 있으면 절대 조기 종료하지 않는다.
    vB = verif.get("유형B_차트번호오타") if verif else None
    vC1 = verif.get("유형C1_한쪽만존재") if verif else None
    vC2 = verif.get("유형C2_금액불일치") if verif else None
    vC3 = verif.get("유형C3_결제수단불일치") if verif else None
    n_vB = len(vB) if vB is not None else 0
    n_vC1 = len(vC1) if vC1 is not None else 0
    n_vC2 = len(vC2) if vC2 is not None else 0
    n_vC3 = len(vC3) if vC3 is not None else 0
    has_verif = (n_vB + n_vC1 + n_vC2 + n_vC3) > 0

    if not has_nonzero and not has_patient_diff and not has_verif:
        L.append("\n[결과] 모든채널·환자 합계 일치 — 추가분석 불필요")
        return "\n".join(L)[:max_chars]
    if not has_nonzero and has_patient_diff:
        L.append("\n[주의] 채널 합계는 일치하나 환자별 금액 불일치가 +/-로 상쇄되어 숨어있음 "
                 "— 아래 [차트번호별 3-way 통합]에서 환자별 개별 검토 필요")
    if not has_nonzero and not has_patient_diff and has_verif:
        L.append("\n[주의] 채널·환자 합계는 일치하나 차트마감↔일일마감 환자단위 대조에서 "
                 "오입력 의심 건이 확정됨 — 아래 [데이터검증] 우선 검토")

    # ── [데이터검증 — 차트마감↔일일마감 확정대조] ──
    # 코드가 결정론적으로 추출한 '오입력 의심' 확정 단서. AI는 이를 최우선 신뢰·보고.
    if has_verif:
        L.append("\n[데이터검증 — 차트마감↔일일마감 환자단위 확정대조 (코드추출·오차0·최우선단서)]")
        L.append(f"요약: 유형B 차트번호오타 {n_vB}건 · 유형C1 한쪽만존재 {n_vC1}건 · "
                 f"유형C2 금액불일치 {n_vC2}건 · 유형C3 결제수단불일치 {n_vC3}건")
        if n_vB:
            L.append("·유형B[차트번호오타] 이름·금액 동일·번호만 상이 → 차트마감(EMR)은 차트번호 "
                     "오입력 불가(시스템 기재) → 항상 구글 일일마감 쪽 수기오류 — 일일마감에서 해당 "
                     "번호를 찾아 정정만 하면 됨(단순 오류건·차이금액 집계 미포함)")
            L.append("환자|차트마감#|일일마감#|금액|원인")
            for _, r in vB.head(15).iterrows():
                nm = str(r.get("성명", ""))[:14].replace("|", " ")
                cause = str(r.get("추정원인", "")).replace("일일마감 차트번호 수기오류", "").strip("()")
                L.append(f"{nm}|{r.get('차트마감_차트번호','')}|{r.get('일일마감_차트번호','')}|"
                         f"{_f(r.get('차트금액'))}|{cause or '번호상이'}")
        if n_vC1:
            L.append("·유형C1[한쪽만존재] 한 파일에만 있는 환자 → 구분 그대로 '[차트마감에만 존재]/"
                     "[일일마감에만 존재]'를 명시해 보고. 일일마감에만 존재=차트(세무 기준) 누락 의심, "
                     "차트마감에만 존재=일일마감 누락 의심. "
                     "'동일금액상대'가 있으면 양쪽이 같은 건일 가능성 높음(이름/번호 오기재) → 한 건으로 묶어 보고")
            L.append("구분|차트#|환자|금액|결제수단|동일금액상대")
            for _, r in vC1.head(15).iterrows():
                nm = str(r.get("성명", ""))[:12].replace("|", " ")
                gb = "[" + str(r.get("구분", "")).replace("|", " ") + "]"
                pay = str(r.get("결제수단", ""))[:30].replace("|", " ")
                hint = str(r.get("동일금액상대", "")).replace("|", " ")[:40] or "-"
                L.append(f"{gb}|{r.get('차트번호','')}|{nm}|{_f(r.get('금액'))}|{pay}|{hint}")
        if n_vC2:
            L.append("·유형C2[금액불일치] 동일 차트번호·수납액 상이 → 금액/결제수단 오기재 의심")
            L.append("차트#|환자|차트금액|일마금액|차이|차트결제|일마결제")
            for _, r in vC2.head(15).iterrows():
                nm = str(r.get("성명", ""))[:12].replace("|", " ")
                pc = str(r.get("차트결제수단", ""))[:30].replace("|", " ")
                dc = str(r.get("일마결제수단", ""))[:30].replace("|", " ")
                L.append(f"{r.get('차트번호','')}|{nm}|{_f(r.get('차트금액'))}|{_f(r.get('일마금액'))}|"
                         f"{_f(r.get('차이'))}|{pc}|{dc}")
        if n_vC3:
            L.append("·유형C3[결제수단불일치] 총액 동일·채널 분배 상이 → 채널 합계 차이의 직접 원인 후보 "
                     "(환자 총액 비교론 안 보임 — 채널대사 차이와 대조 필수)")
            L.append("차트#|환자|총액|차트결제|일마결제|차이요약|원인")
            for _, r in vC3.head(15).iterrows():
                nm = str(r.get("성명", ""))[:12].replace("|", " ")
                pc = str(r.get("차트결제수단", ""))[:30].replace("|", " ")
                dc = str(r.get("일마결제수단", ""))[:30].replace("|", " ")
                ds = str(r.get("차이요약", ""))[:50].replace("|", " ")
                cz = str(r.get("추정원인", ""))[:40].replace("|", " ")
                L.append(f"{r.get('차트번호','')}|{nm}|{_f(r.get('총액'))}|{pc}|{dc}|{ds}|{cz}")

    # ── 차트번호 → 이름 맵 ────────────────────────
    name_map = {}
    if not daily.empty and "성명" in daily.columns:
        for _, row in daily[["차트번호", "성명"]].drop_duplicates("차트번호").iterrows():
            ch = clean_no(row["차트번호"])
            nm = str(row["성명"]).strip()
            if ch and nm and nm != "nan":
                name_map[ch] = nm
    if not patient.empty and "이름" in patient.columns:
        for _, row in patient[["차트번호", "이름"]].drop_duplicates("차트번호").iterrows():
            ch = clean_no(row["차트번호"])
            nm = str(row["이름"]).strip()
            if ch and nm and nm != "nan" and ch not in name_map:
                name_map[ch] = nm

    # ── [★★ 승인번호확정매칭] ────────────────────
    star2_all = []
    seen_key = set()
    for ch_sus in suspects_by_channel.values():
        for s in ch_sus:
            if "★★" not in str(s.get("출처", "")):
                continue
            key = (str(s.get("환자", "")), int(s.get("금액", 0)))
            if key in seen_key:
                continue
            seen_key.add(key)
            star2_all.append(s)
    if star2_all:
        # gap일치 → 일반 ★★ 순으로
        star2_all.sort(key=lambda x: (0 if "gap일치" in str(x["출처"]) else 1, -abs(int(x["금액"]))))
        L.append("\n[★★ 승인번호확정매칭 — 한솔↔차트 동일환자·금액불일치 (1순위)]")
        L.append("환자|차이금액|단서")
        for s in star2_all[:10]:
            nm = str(s.get("환자", ""))[:14].replace("|", " ")
            clue = str(s.get("단서", ""))[:90].replace("|", " ")
            L.append(f"{nm}|{int(s['금액']):+,}|{clue}")

    # ── [차트번호 3-way 통합] ─────────────────────
    # 차트(분류별) / 일마(채널별) / 한솔(매칭카드합) per 차트번호
    # p_pivot·d_pivot은 위 단축 판정에서 이미 만든 것을 재사용 (find_channel_suspects와 동일 로직 → UI·AI·엑셀 수치 일치)

    # 한솔 카드금액은 승인번호 다리로 차트에 귀속(일마 차트번호 유무 무관). 미링크 차트는 unknown.
    h_card_by_ch = _hansol_card_by_chart(hansol, patient)
    if not h_card_by_ch and p1_full is not None and not p1_full.empty:
        for _, r in p1_full.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            h_card_by_ch[ch] = (_safe_int(r.get("한솔카드", 0)), _safe_int(r.get("한솔건수", 0)))

    p_appr_by_ch = {}
    if not patient.empty and "승인번호목록" in patient.columns:
        for ch_g, grp in patient.groupby("차트번호"):
            ch = clean_no(ch_g)
            if not ch:
                continue
            appr = set()
            for lst in grp["승인번호목록"]:
                if isinstance(lst, list):
                    for a in lst:
                        s = str(a)
                        if len(s) >= 5:
                            appr.add(s[-8:])
            if appr:
                p_appr_by_ch[ch] = sorted(appr)[:2]

    all_charts = set(p_pivot.keys()) | set(d_pivot.keys()) | set(h_card_by_ch.keys())
    cross_rows = []
    for ch in all_charts:
        p = p_pivot.get(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
        d = d_pivot.get(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
        h_amt, h_cnt = h_card_by_ch.get(ch, (None, 0))  # None = 한솔 미링크(unknown), 참고열로만 표시
        # 차이는 일마↔차트 비교만 사용(신뢰도 높음). 한솔↔차트 비교는 ★★ 승인번호 섹션이 담당.
        diffs = []
        if d["카드"] != p["카드"]:
            diffs.append(f"일마카드{d['카드']:,}≠차트카드{p['카드']:,}")
        if d["현금"] != p["현금"]:
            diffs.append(f"현금{d['현금']:,}≠차트{p['현금']:,}")
        if d["이체"] != p["이체"]:
            diffs.append(f"이체{d['이체']:,}≠차트{p['이체']:,}")
        if d["플랫폼"] != p["플랫폼"]:
            diffs.append(f"플랫폼{d['플랫폼']:,}≠차트{p['플랫폼']:,}")
        if not diffs:
            continue
        gap_size = (
            abs(d["카드"] - p["카드"]) + abs(d["현금"] - p["현금"])
            + abs(d["이체"] - p["이체"]) + abs(d["플랫폼"] - p["플랫폼"])
        )
        cross_rows.append((gap_size, ch, p, d, h_amt, h_cnt, diffs))

    cross_rows.sort(key=lambda x: -x[0])
    if cross_rows:
        L.append("\n[차트번호별 3-way 통합 — 차이있는 환자, gap큰순 TOP25]")
        if has_hansol:
            L.append("차트#|이름|차트(카/현/이/플)|일마(카/현/이/플)|한솔카드(건)|차트승인말미|차이요약")
        else:
            L.append("차트#|이름|차트(카/현/이/플)|일마(카/현/이/플)|차트승인말미|차이요약")
        for _, ch, p, d, h_amt, h_cnt, diffs in cross_rows[:25]:
            nm = name_map.get(ch, "")[:10].replace("|", " ")
            p_str = f"{p['카드']:,}/{p['현금']:,}/{p['이체']:,}/{p['플랫폼']:,}"
            d_str = f"{d['카드']:,}/{d['현금']:,}/{d['이체']:,}/{d['플랫폼']:,}"
            appr = ",".join(p_appr_by_ch.get(ch, [])) or "-"
            loc = _pivot_presence_tag(ch, p_pivot, d_pivot)
            diff_str = (loc + " " + " · ".join(diffs))[:90]
            if has_hansol:
                h_str = f"{h_amt:,}({h_cnt})" if h_amt is not None else "-"
                L.append(f"{ch}|{nm}|{p_str}|{d_str}|{h_str}|{appr}|{diff_str}")
            else:
                L.append(f"{ch}|{nm}|{p_str}|{d_str}|{appr}|{diff_str}")

    # ── 채널 gap 산출 (PG-only/front-only 정렬용) ──
    gap_card = 0
    if totals:
        h_c = totals.get("h_card")
        if h_c is not None:
            gap_card = h_c - totals.get("d_card", 0)
            if gap_card == 0:
                gap_card = h_c - totals.get("p_card", 0)
        if gap_card == 0:
            gap_card = totals.get("d_card", 0) - totals.get("p_card", 0)

    def _rank(amt):
        a = abs(int(amt))
        g = abs(int(gap_card))
        return (abs(a - g), -a) if g else (-a, 0)

    # ── [한솔 PG-only] ───────────────────────────
    if has_hansol and h_um is not None and not h_um.empty:
        d_amt_to_names = {}
        if not daily.empty:
            for _, row in daily.iterrows():
                amt = _safe_int(row.get("카드", 0))
                nm = str(row.get("성명", "")).strip()
                if amt > 0 and nm and nm != "nan":
                    d_amt_to_names.setdefault(amt, [])
                    if nm not in d_amt_to_names[amt]:
                        d_amt_to_names[amt].append(nm)

        L.append("\n[한솔 PG-only 카드미매칭 — gap근접순 TOP15]")
        L.append("시각|금액|카드사|말미|승인번호|동일금액일마환자")
        h_sorted = sorted(h_um.to_dict("records"), key=lambda r: _rank(r.get("금액", 0)))
        for r in h_sorted[:15]:
            t = str(r.get("시간표시", ""))[:8]
            amt = _safe_int(r.get("금액", 0))
            cs = str(r.get("카드사", ""))[:5]
            cn = str(r.get("카드번호", ""))
            tail = cn[-4:] if cn and cn != "nan" else ""
            ap = str(r.get("승인번호", ""))[-8:]
            cands = d_amt_to_names.get(amt, [])
            cand_str = ("·".join(cands[:2]) + "?") if cands else "-"
            L.append(f"{t}|{amt:,}|{cs}|*{tail}|{ap}|{cand_str}")

    # ── [일마 front-only] ────────────────────────
    if d_um is not None and not d_um.empty:
        L.append("\n[일마 front-only 카드미매칭 — gap근접순 TOP15]")
        L.append("차트#|이름|금액|내원순서")
        d_sorted = sorted(d_um.to_dict("records"), key=lambda r: _rank(r.get("카드", 0)))
        for r in d_sorted[:15]:
            ch = clean_no(r.get("차트번호", ""))
            nm = str(r.get("성명", ""))[:10]
            amt = _safe_int(r.get("카드", 0))
            ord_v = _safe_int(r.get("내원순서", 0))
            L.append(f"{ch}|{nm}|{amt:,}|{ord_v}")

    # ── [차트환불/취소] ──────────────────────────
    if not patient.empty and "is_취소" in patient.columns:
        p_can = patient[patient["is_취소"]]
        if not p_can.empty:
            L.append("\n[차트환불/취소 — 합계에 영향]")
            L.append("차트#|이름|분류|금액")
            for _, r in p_can.head(10).iterrows():
                ch = clean_no(r.get("차트번호", ""))
                nm = str(r.get("이름", ""))[:10]
                cat = str(r.get("분류", ""))
                amt = _safe_int(r.get("금액", 0))
                L.append(f"{ch}|{nm}|{cat}|{amt:+,}")

    # ── [일마환불/취소 행] ───────────────────────
    if daily_refund is not None and not daily_refund.empty:
        L.append("\n[일마환불/취소 행]")
        L.append("차트#|성명|카드|현금|이체|플랫폼")
        for _, r in daily_refund.head(10).iterrows():
            ch = clean_no(r.get("차트번호", ""))
            nm = str(r.get("성명", ""))[:10]
            L.append(
                f"{ch}|{nm}|{_safe_int(r.get('카드',0)):,}|"
                f"{_safe_int(r.get('현금',0)):,}|{_safe_int(r.get('이체',0)):,}|"
                f"{_safe_int(r.get('플랫폼합',0)):,}"
            )

    text = "\n".join(L)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n…(축약)"
    return text


AI_SYSTEM = (
    "병원 정산 분석관. 입력은 한솔(PG카드승인내역)·일마(프론트 일일마감)·차트(EMR 환자별집계) "
    "3개 파일을 차트번호·승인번호로 join한 raw cross-file 구조다. "
    "너의 역할은 '차트에서 무엇을 빼라/얼마로 고쳐라'를 단정하는 것이 아니라, "
    "'어느 환자에서 어느 채널에 얼마만큼 차이가 나는지'를 데이터로 정확히 짚고 "
    "'그 환자의 원본 데이터(차트/일마/한솔)를 먼저 검토하라'고 안내하는 것이다. "
    "다음 규칙을 절대 위반하지 말 것:\n"
    "[R0] 입력에 [데이터검증] 섹션이 있으면 그것은 코드가 차트마감↔일일마감을 환자단위로 "
    "결정론적 대조해 '확정'한 오입력 의심 단서다(오차0). ★★ 다음가는 최우선 신뢰대상이며 "
    "절대 누락 없이 전건 보고한다. 특히 유형B(차트번호오타)·유형C3(결제수단불일치)는 총액이 "
    "같아 합계 비교엔 안 나타나므로, 합계가 맞아도 반드시 별도로 짚는다.\n"
    "[R1] 환자명·차트번호·금액·승인번호는 반드시 입력에 실제 존재하는 값만 인용 (창작·반올림·근사 금지).\n"
    "[R2] 모든 차이는 환자(차트번호) 단위로 끝까지 추적한다. 채널 합계차이를 큰 환자 한 명으로 "
    "'설명 완료'했다고 나머지 환자별 불일치를 버리지 말 것. 서로 +/-로 상쇄되어 채널 합계엔 "
    "안 보이는 환자쌍·소액 불일치도 각각 독립된 검토대상으로 반드시 나열한다.\n"
    "[R3] 우선순위 절대규칙: ★★(승인번호 동일환자 확정) > ★(금액·gap 매칭) > 일반. 절대 뒤집을 수 없음.\n"
    "[R4] 동일등급 내 정렬: 금액 큰순 → gap일치도 높은 순.\n"
    "[R5] 부호 약속: '한-차=+10,000'은 한솔이 차트보다 10,000원 많음 = 차트누락 or 한솔과다. "
    "'일-차=-5,000'은 일마가 차트보다 5,000원 적음 = 일마누락 or 차트과다.\n"
    "[R6] 한솔=일마=A · 차트=B(A≠B) → 차트가 틀렸을 가능성이 높다 (PG승인+프론트수납이 모두 A). "
    "단 '차트를 A로 고쳐라'라고 단정하지 말고 '차트의 해당 환자 결제내역을 먼저 검토하라'고 권고.\n"
    "[R6b] 한솔이 없으면(2파일 분석) 다수결 불가 → 어느 쪽이 맞는지 단정 금지. "
    "'일마·차트 중 한 곳 오기재 가능성'으로 표기하고 양쪽 원본 검토를 권고.\n"
    "[R7] 동일금액이 다른 결제수단 칼럼에 분산되면 결제수단 오기재 가능성 "
    "(예: 차트카드=X · 일마현금=X) — 해당 환자 결제수단 검토 권고.\n"
    "[R8] 한솔PG-only의 '동일금액일마환자' hint가 있으면 그 환자의 일마 결제수단 오기재 가능성 — 검토 권고.\n"
    "[R9] '확인 바랍니다' 식 막연한 일반론 금지. 반드시 '어느 환자(차트#)·어느 채널·차이금액·추정원인'을 "
    "구체적으로 짚되, 마지막 액션은 특정 금액으로 고치라는 명령이 아니라 "
    "'그 환자의 원본 데이터를 검토하라'는 권고 형식으로 출력.\n"
    "[R10] 보고의 단위는 '오류 건'이다 — 모든 항목을 환자(차트#)·채널·차이금액·부호로 특정한다. "
    "각 채널의 합계 차이는 반드시 건 단위로 분해해 '채널차이 = Σ(건별 차이)' 검산 결과를 제시하고, "
    "건들로 분해되지 않는 잔여 금액이 있으면 '미특정 잔여 ±X원'으로 숨김 없이 명시한다 "
    "(잔여가 있다 = 아직 못 찾은 오류 건이 있다는 뜻이므로 PG-only/front-only/환불 행을 재점검).\n"
    "[R11] 금액 오타 패턴 인식: 서로 다른 파일의 두 미설명 금액이 자릿수 추가/누락"
    "(50,000↔500,000), 한 자리 오타(40,000↔49,000), 인접 자리 뒤바뀜(120,000↔210,000) "
    "관계면 '같은 건의 금액 오타 의심'으로 한 쌍으로 묶어 보고한다 — 별개의 누락 2건으로 "
    "처리하면 오류 건 수가 부풀려진다. 단, 쌍으로 묶은 근거(두 금액)를 반드시 함께 인용.\n"
    "[R12] 차트 기준 원칙: 차트(EMR)가 세무 기준원장이다. 세무조사 시 차트 수치를 기준으로 "
    "단말기(PG) 결제금액·플랫폼 정산과의 차이에 증빙을 요구하므로, 모든 차이는 '차트 기준 "
    "±(차트 대비 일마/한솔이 많다·적다)'로 서술한다. 차트마감의 차트번호는 시스템 자동기재라 "
    "오입력될 수 없다 — 차트번호 불일치(유형B)는 항상 구글 일일마감 쪽 수기오류이므로 "
    "'구글 일일마감에서 해당 번호를 찾아 정정'으로만 안내하고, 이름·금액이 일치하면 단순 "
    "오류건으로 차이금액 집계·검토대상 목록에 포함하지 않는다(안내만).\n"
    "[R13] 위치 명시: 모든 차이·누락 건은 어느 파일에 차이가 있는지 '[차트마감에만 존재]' "
    "'[일일마감에만 존재]' '[양쪽 존재·금액 상이]' 태그로 구체적으로 명시한다. "
    "일일마감에만 존재 = 차트(세무 기준) 누락 의심(세무위험 우선 검토), "
    "차트마감에만 존재 = 일일마감 누락 의심."
)

AI_USER = """병원 정산 데이터 (3개 파일을 차트번호·승인번호로 통합한 raw 구조):

{data}

[분석 절차 — 반드시 순서대로 수행]

STEP0. [데이터검증] 확정 오입력 우선 처리 (섹션이 있으면 절대 먼저)
  · 코드가 차트마감↔일일마감을 환자단위로 대조해 확정한 단서 = 추론 불필요한 사실.
  · 유형B(차트번호오타): 이름·금액 동일·번호만 상이. 차트마감(EMR)은 차트번호 오입력이
    불가능(시스템 자동기재)하므로 오류는 항상 구글 일일마감 쪽(R12) → "구글 일일마감에서
    해당 번호를 찾아 차트마감 번호로 정정" 안내만 한다. 금액·이름이 맞으므로 단순 오류건 —
    차이금액 집계·검토대상 목록에 포함하지 않는다(합계 영향 없음 명시).
  · 유형C1(한쪽만존재): 한 파일에만 있는 환자 → 반드시 '[차트마감에만 존재]/[일일마감에만
    존재]' 위치를 명시(R13)하고, 일일마감에만 존재면 "차트(세무 기준)에 해당 수납 누락
    여부/환불 미반영 검토", 차트마감에만 존재면 "일일마감 누락 여부 검토" 권고.
    '동일금액상대'가 있으면 두 건은 같은 건의 이름/번호
    오기재일 가능성이 높으므로 별개 누락 2건이 아니라 한 쌍(오류 건 1건)으로 묶어 보고.
  · 유형C2(금액불일치): 동일번호·금액상이 → R5~R7 적용해 결제수단/금액 오기재 방향 제시.
    두 금액이 R11 오타 패턴이면 '금액 오타 의심'을 명시.
  · 유형C3(결제수단불일치): 총액 일치·채널 분배 상이 = 채널 합계 차이의 직접 원인 후보.
    [채널대사]의 채널별 차이값과 대조해 어느 환자의 분배 오류가 어느 채널 차이를 만드는지
    연결해 보고 (예: 카드 +50,000·현금 -50,000 분배 오류 → 카드채널 +50,000 차이 설명).
  · 이 섹션의 모든 건은 아래 출력 '검토대상'에 누락 없이 포함한다.

STEP1. [채널대사] 차이값 확정
  · 채널별 한-차/한-일/일-차 차이를 정확히 메모 (부호 포함).
  · 채널 합계가 모두 0이어도, 데이터에 환자별(차트번호별) 금액 불일치가 하나라도 있으면 분석을 계속한다
    (서로 +/-로 상쇄돼 합계엔 안 보이는 환자별 오류가 숨어있을 수 있음).
  · 환자별 불일치가 전혀 없을 때만 "✅ 모든 채널·환자 합계 일치 — 검토 불필요" 한 줄로 종료.

STEP2. [★★ 승인번호확정매칭] 1순위 처리 (절대 먼저)
  · 코드가 한솔승인번호 ↔ 차트승인번호목록 매칭으로 확정한 환자 = 환자 특정 完了.
  · 단서의 '한솔 X vs 일마 Y' 해석:
      - 한솔=일마(차트만 다름) → R6 → 차트의 그 환자 결제내역 우선 검토 권고.
      - 한솔≠일마 → PG영수증이 진실에 가까움 → 일마의 그 환자 내역 우선 검토 권고.

STEP3. [차트번호별 3-way 통합]의 차이행을 환자 단위로 빠짐없이 검토
  · 각 차이행의 추정 원인을 아래 패턴으로 분류해 '검토 방향'을 제시 (단정·금액확정 금지):
    Pa(결제수단 오기재): 차트(X/0/0/0)·일마(0/X/0/0) → "카드↔현금 오기재 가능성 — 결제수단 검토"
    Pb(카드↔이체 오기재): 차트(X/0/0/0)·일마(0/0/X/0) → "카드↔이체 오기재 가능성 — 검토"
    Pc(차트 중복기재):   차트(2X)·일마(X)·한솔(X)  → "차트 중복기재 가능성 — 차트 결제행 검토"
    Pd(차트 누락):       차트(0)·일마(X)          → "차트 누락 가능성 — 차트 결제건 검토"
    Pe(차트 금액오류):   차트(X)·일마=한솔=Y(X≠Y) → R6 → "차트 금액오류 가능성 — 차트 금액 검토"
    Pf(금액 오타):       두 값이 R11 오타 패턴(자릿수 추가/누락·한 자리 오타·자리 뒤바뀜)
                         → "같은 건의 금액 오타 의심 — 두 금액({{X}}↔{{Y}}) 인용해 해당 건 검토"
  · 어느 패턴에도 안 맞으면 '복합 — 수동검토'로 표기 (창작금지).
  · ★중요★ 채널 합계차이를 큰 환자 한 명으로 설명했어도 나머지 차이행을 생략하지 말 것.
    +/-로 상쇄되는 환자쌍(예: A −27,500 · B +27,500, 합계 0)도 각각 검토대상으로 반드시 나열한다.

STEP4. [한솔 PG-only] · [일마 front-only] cross-match
  · 한솔PG-only의 '동일금액일마환자' hint 존재 → R8 → 그 환자 일마 결제수단(현금/이체 오기재) 검토 권고.
  · 한솔PG-only 금액 = 일마front-only 금액 페어 → 환자 동일성 검토 권고.
  · 한솔PG-only 금액 ↔ 일마front-only 금액이 R11 오타 패턴 → Pf '같은 건의 금액 오타 의심' 쌍으로 보고.
  · hint 없는 한솔PG-only → 일마·차트 결제건 누락 가능성 검토.
  · hint 없는 일마front-only → PG승인 없음 → 미수납 or 결제수단 오기재 가능성 검토.

STEP5. [환불/취소] 행 부호 검토
  · 차트환불/일마환불 행이 채널대사에 정상 반영됐는지 확인 (음수 누락 시 차이 발생 가능).

STEP6. 정합성 검산 (R10 — 출력에 결과를 반드시 포함)
  · 환자별 차이를 채널별로 합산해 [채널대사] 차이값과 대조: '채널차이 = Σ건별 차이'가 성립하는지 검산.
  · 성립하면 "검산 일치(잔여 0원)"와 합계차이를 주로 만든 '주요원인' 건을 명시.
  · 성립하지 않으면 "미특정 잔여 ±X원"을 명시하고 PG-only/front-only/환불 행에서 후보를 재탐색.
  · 단, 합계엔 안 잡히는 상쇄쌍·소액 불일치·유형C3도 STEP3 목록에 모두 남길 것 (합계 일치는 생략 사유 아님).
    유형B(이름·금액 일치·번호만 오타)는 예외 — R12에 따라 데이터검증 확정건에서 안내만 하고 검토대상엔 넣지 않는다.

[출력 형식 — 900토큰 이내(반드시 끝까지 완결), 마크다운]

### 데이터검증 확정건 (입력에 [데이터검증]이 있을 때만)
- **유형B 차트번호오타**: {{환자}}(차트#{{차트마감#}} ↔ 일마기재 {{일일마감#}}) {{금액}}원 → 구글 일일마감 차트번호만 정정 (단순 오류건 · 차이금액 미포함 · 합계 영향 없음)
- **유형C1 한쪽만존재**: [{{차트마감에만 존재/일일마감에만 존재}}] {{환자}}(차트#) {{금액}}원 → {{일일마감에만: 차트(세무 기준) 누락/환불미반영 검토 · 차트마감에만: 일일마감 누락 검토}} · 동일금액상대 있으면 "{{상대}}와 동일건 의심 — 한 쌍으로 검토"
- **유형C2 금액불일치**: {{환자}}(차트#) 차트{{차트금액}}↔일마{{일마금액}}(차이 {{±}}) → {{결제수단/금액 오기재(R11 오타패턴이면 명시)}} 검토
- **유형C3 결제수단불일치**: {{환자}}(차트#) 총액 {{금액}} 일치 · {{X↔Y 분배 상이}} → {{어느 채널 합계차이를 설명하는지}} + 결제수단 검토
(해당 유형 0건이면 그 줄 생략. 검증 섹션 자체가 없으면 이 블록 전체 생략)

### 채널별 차이 진단
차이 또는 환자별 불일치가 있는 채널마다 ↓
- **{{채널}}**: 한-차=±?원 · 한-일=±?원 · 일-차=±?원
  - **검토대상** (★★ → ★ → 일반 순, 상쇄쌍·소액 포함 빠짐없이):
    1. `★★/★/-` {{환자명}}(차트#{{ch}}) · {{채널}} {{차이금액}}원 · [{{차트마감에만 존재/일일마감에만 존재/양쪽 존재·금액 상이}}] · 추정원인 {{Pa~Pf/복합}} → **검토 권고**: {{어느 파일의 그 환자 어느 내역을 먼저 봐야 하는지}}
    2. …
  - **검산(R10)**: 채널차이 {{±X}}원 = {{건별 차이 합산식}} → {{일치(잔여 0원) / 미특정 잔여 ±Y원}} · 주요원인 = {{환자/금액}}

### 결론 (1~2문장)
오류 건 목록을 한 줄로 요약 — 어느 환자(차트#)의 어느 채널 금액을 어느 파일에서 먼저 검토해야
하는지, 환자명·차트#·차이금액 인용. 검산 결과(잔여 0원 여부)와, 상쇄되어 합계엔 안 보이던
환자쌍·오타의심쌍(R11)도 함께 언급. 특정 금액으로 고치라 단정 말고 '검토 후 정정' 관점으로 안내."""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI 엑셀 (4시트: P1차이 / P2-한솔 / P2-일마 / 합계)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_3way_table(hansol, daily, patient, p1_full, daily_refund=None):
    """차트번호 단위로 차트(분류별)·일마(채널별)·한솔(매칭카드합)을 한 행으로 통합.
    엑셀/AI 두 곳에서 공통으로 사용."""
    has_hansol = hansol is not None and not hansol.empty

    name_map = {}
    if not daily.empty and "성명" in daily.columns:
        for _, row in daily[["차트번호", "성명"]].drop_duplicates("차트번호").iterrows():
            ch = clean_no(row["차트번호"])
            nm = str(row["성명"]).strip()
            if ch and nm and nm != "nan":
                name_map[ch] = nm
    if not patient.empty and "이름" in patient.columns:
        for _, row in patient[["차트번호", "이름"]].drop_duplicates("차트번호").iterrows():
            ch = clean_no(row["차트번호"])
            nm = str(row["이름"]).strip()
            if ch and nm and nm != "nan" and ch not in name_map:
                name_map[ch] = nm

    p_pivot, d_pivot = _chart_method_pivots(patient, daily, daily_refund)

    # 한솔 카드금액은 승인번호 다리로 차트에 귀속(일마 차트번호 유무 무관). 미링크는 unknown.
    h_card_by_ch = _hansol_card_by_chart(hansol, patient)
    if not h_card_by_ch and p1_full is not None and not p1_full.empty:
        for _, r in p1_full.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            h_card_by_ch[ch] = (_safe_int(r.get("한솔카드", 0)), _safe_int(r.get("한솔건수", 0)))

    rows = []
    all_charts = set(p_pivot.keys()) | set(d_pivot.keys()) | set(h_card_by_ch.keys())
    for ch in sorted(all_charts):
        p = p_pivot.get(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
        d = d_pivot.get(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
        h_amt, h_cnt = h_card_by_ch.get(ch, (None, 0))
        row = {
            "차트번호": ch,
            "이름": name_map.get(ch, ""),
            "차트카드": p["카드"], "차트현금": p["현금"], "차트이체": p["이체"], "차트플랫폼": p["플랫폼"],
            "일마카드": d["카드"], "일마현금": d["현금"], "일마이체": d["이체"], "일마플랫폼": d["플랫폼"],
        }
        if has_hansol:
            row["한솔카드"] = h_amt if h_amt is not None else ""
            row["한솔건수"] = h_cnt
            row["한-차카드차"] = (h_amt - p["카드"]) if h_amt is not None else 0
        row["일-차카드차"] = d["카드"] - p["카드"]
        row["일-차현금차"] = d["현금"] - p["현금"]
        row["일-차이체차"] = d["이체"] - p["이체"]
        row["일-차플랫폼차"] = d["플랫폼"] - p["플랫폼"]
        rows.append(row)
    return pd.DataFrame(rows)


def build_ai_excel(p1_diff, h_um, d_um, totals, channel_df=None, suspects_by_channel=None,
                   hansol=None, daily=None, patient=None, p1_full=None, verif=None,
                   daily_refund=None):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        # 시트V: 데이터검증(오입력 의심 확정건) — 가장 앞에 배치
        if verif:
            for key, sheet in [("유형B_차트번호오타", "V_차트번호오타"),
                               ("유형C1_한쪽만존재", "V_한쪽만존재"),
                               ("유형C2_금액불일치", "V_금액불일치"),
                               ("유형C3_결제수단불일치", "V_결제수단불일치")]:
                vdf = verif.get(key)
                if vdf is not None and not vdf.empty:
                    vdf.drop(columns=[c for c in ["지점", "날짜"] if c in vdf.columns]).to_excel(
                        w, sheet_name=sheet, index=False)
        # 시트0: 채널 합계 대사 (★메인)
        if channel_df is not None and not channel_df.empty:
            ch_out = channel_df.copy()
            for c in ch_out.columns:
                if c == "채널":
                    continue
                ch_out[c] = ch_out[c].apply(lambda v: "" if v is None or (isinstance(v, float) and pd.isna(v)) else int(v))
            ch_out.to_excel(w, sheet_name="0_채널대사", index=False)

        # 시트0a: 차트번호 3-way 통합 (차트/일마/한솔 한 행)
        if daily is not None and patient is not None:
            tw = build_3way_table(hansol if hansol is not None else pd.DataFrame(),
                                  daily, patient, p1_full, daily_refund)
            if not tw.empty:
                # 차이 있는 행 우선으로 정렬 (절대 차이 합 큰순)
                diff_cols_ex = [c for c in tw.columns if c.endswith("차")]
                if diff_cols_ex:
                    tw["_abs"] = tw[diff_cols_ex].abs().sum(axis=1)
                    tw = tw.sort_values("_abs", ascending=False).drop(columns=["_abs"])
                tw.to_excel(w, sheet_name="0_3way통합", index=False)

        # 시트0b: 의심 후보 (채널별 통합)
        if suspects_by_channel:
            rows = []
            for ch, sus in suspects_by_channel.items():
                for s in sus:
                    rows.append({"채널": ch, **s})
            if rows:
                pd.DataFrame(rows).to_excel(w, sheet_name="0_의심후보", index=False)
            else:
                pd.DataFrame({"상태": ["의심 후보 없음 — 합계 일치"]}).to_excel(w, sheet_name="0_의심후보", index=False)

        # 시트1: P1 (보조)
        if not p1_diff.empty:
            cols = [c for c in ["차트번호", "이름", "차트카드", "차트건수", "한솔카드", "한솔건수",
                                "차이", "일마카드", "차트카드사", "한솔카드사",
                                "한솔승인번호", "차트승인번호"] if c in p1_diff.columns]
            p1_diff[cols].to_excel(w, sheet_name="1_P1_한솔vs차트", index=False)
        else:
            pd.DataFrame({"상태": ["P1 차이 없음"]}).to_excel(w, sheet_name="1_P1_한솔vs차트", index=False)

        # 시트2: P2-한솔미매칭
        if not h_um.empty:
            cols = [c for c in ["시간표시", "금액", "카드번호", "승인번호", "카드사"] if c in h_um.columns]
            h_um[cols].to_excel(w, sheet_name="2_P2_한솔미매칭", index=False)
        else:
            pd.DataFrame({"상태": ["한솔 미매칭 없음"]}).to_excel(w, sheet_name="2_P2_한솔미매칭", index=False)

        # 시트3: P2-일마미매칭
        if not d_um.empty:
            cols = [c for c in ["차트번호", "성명", "카드", "내원순서"] if c in d_um.columns]
            d_um[cols].to_excel(w, sheet_name="3_P2_일마미매칭", index=False)
        else:
            pd.DataFrame({"상태": ["일마 미매칭 없음"]}).to_excel(w, sheet_name="3_P2_일마미매칭", index=False)

        # 시트4: 합계
        has_h = totals.get("_has_hansol", True)
        if has_h:
            rows = [
                ["카드", totals["h_card"], totals["d_card"], totals["p_card"],
                 totals["p_card"]-totals["h_card"], totals["h_card"]-totals["d_card"]],
                ["현금+이체", totals["h_cash"], totals["d_cashxfer"], totals["p_cashxfer"],
                 totals["p_cashxfer"]-totals["h_cash"], totals["h_cash"]-totals["d_cashxfer"]],
                ["플랫폼", 0, totals["d_plat"], totals["p_plat"],
                 totals["p_plat"]-totals["d_plat"], 0],
            ]
            pd.DataFrame(rows, columns=["구분", "한솔", "일마", "차트",
                                         "차트-한솔(P1)", "한솔-일마(P2)"]).to_excel(
                w, sheet_name="4_합계", index=False)
        else:
            rows = [
                ["카드", totals["d_card"], totals["p_card"], totals["p_card"]-totals["d_card"]],
                ["현금+이체", totals["d_cashxfer"], totals["p_cashxfer"], totals["p_cashxfer"]-totals["d_cashxfer"]],
                ["플랫폼", totals["d_plat"], totals["p_plat"], totals["p_plat"]-totals["d_plat"]],
            ]
            pd.DataFrame(rows, columns=["구분", "일마", "차트", "차트-일마"]).to_excel(
                w, sheet_name="4_합계", index=False)
    buf.seek(0)
    return buf.getvalue()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 검증 (차트마감 ↔ 일일마감 환자단위 대조) — 결정론적, AI 불필요
#   유형B  차트번호 오타  : 이름·금액 동일 + 차트번호만 상이 (수기 오타)
#   유형C1 한쪽만 존재    : 차트마감/일일마감 중 한쪽에만 있는 환자
#   유형C2 금액 불일치    : 동일 차트번호인데 수납액이 다름
# 셋 다 코드로 100% 확정 계산 → 어떤 AI(제미나이·클로드)와도 무관하게 동일 결과.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 일일마감 플랫폼 세부 결제수단 컬럼 (parse_daily pay_map과 동일, 카드/현금/이체 제외)
_DAILY_PLAT_COLS = [
    "여신티켓", "강남언니", "나만의닥터", "제로페이", "기타지역화폐",
    "알리페이", "위챗페이", "카카오페이", "간편결제", "바비톡", "닥터나우",
]


def _verif_name_norm(s):
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(s)).lower()


def _verif_name_sim(a, b):
    """이름 유사 판정: 정규화 후 동일하거나 한쪽이 다른 쪽을 포함."""
    a, b = _verif_name_norm(a), _verif_name_norm(b)
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= 2 and len(b) >= 2 and (a in b or b in a):
        return True
    return False


def _verif_osa(a, b):
    """Optimal String Alignment 거리(인접 전치 1로 계산)."""
    a, b = str(a), str(b)
    la, lb = len(a), len(b)
    if la == 0:
        return lb
    if lb == 0:
        return la
    d = [[0] * (lb + 1) for _ in range(la + 1)]
    for i in range(la + 1):
        d[i][0] = i
    for j in range(lb + 1):
        d[0][j] = j
    for i in range(1, la + 1):
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                d[i][j] = min(d[i][j], d[i - 2][j - 2] + 1)
    return d[la][lb]


def _verif_chart_typo_adjacent(a, b):
    """'인접 자릿수 오타' 판정: 편집거리 1(치환/삽입/삭제/전치) 또는 앞자리 일치 잘림."""
    a, b = str(a), str(b)
    if a == b:
        return False
    short, lng = (a, b) if len(a) <= len(b) else (b, a)
    if short and lng.startswith(short) and len(lng) > len(short):
        return True
    return _verif_osa(a, b) <= 1


def _verif_chart_typo_loose(a, b):
    """차트번호 오타 후보 매칭용(느슨): 인접오타 또는 편집거리 2 이내."""
    if _verif_chart_typo_adjacent(a, b):
        return True
    return _verif_osa(a, b) <= 2


def _verif_fmt_methods(meth):
    return "; ".join(f"{k}:{v:,}" for k, v in meth.items())


def _verif_method_buckets(meth):
    """meth(숫자 dict) → 표준 채널 버킷 {카드, 현금, 이체, 플랫폼, 기타}.

    차트마감 분류(카드/현금/이체/플랫폼/기타)와 일일마감 컬럼(카드/현금/이체 +
    개별 플랫폼명)을 같은 축으로 정규화해, '총액은 같은데 결제수단 분배가 다른'
    오기재(유형C3 — 채널 합계 차이의 직접 원인)를 환자 단위로 확정 비교한다.
    """
    b = {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0, "기타": 0}
    for k, v in (meth or {}).items():
        if k in ("카드", "현금", "이체"):
            b[k] += v
        elif k == "플랫폼" or k in _DAILY_PLAT_COLS:
            b["플랫폼"] += v
        else:
            b["기타"] += v
    return b


def _verif_aggregate_patient(patient):
    """차트마감을 차트번호 단위로 집계: {차트번호: {name, amt, methods, meth}}.

    meth(숫자 dict)는 결제수단 분배 비교(유형C3 — 총액 일치·분배 상이)용."""
    out = {}
    if patient is None or patient.empty or "차트번호" not in patient.columns:
        return out
    df = patient.copy()
    df["차트번호"] = df["차트번호"].astype(str).str.strip()
    df = df[df["차트번호"] != ""]
    order = ["카드", "현금", "이체", "플랫폼", "기타"]
    for ch, g in df.groupby("차트번호"):
        name = ""
        for n in g["이름"].astype(str):
            t = n.strip()
            if t and t.lower() != "nan":
                name = t
                break
        amt = int(g["금액"].sum())
        meth = {}
        for cls in order:
            s = int(g.loc[g["분류"] == cls, "금액"].sum()) if "분류" in g.columns else 0
            if s != 0:
                meth[cls] = s
        if "분류" in g.columns:
            for cls, gg in g.groupby("분류"):
                if str(cls) not in order:
                    s = int(gg["금액"].sum())
                    if s != 0:
                        meth[str(cls)] = s
        if not meth:
            present = [str(c) for c in g["분류"].unique()] if "분류" in g.columns else []
            meth[(present[0] if present else "카드")] = 0
        out[ch] = {"name": name, "amt": amt, "methods": _verif_fmt_methods(meth),
                   "meth": dict(meth)}
    return out


def _verif_aggregate_daily(daily, daily_refund=None):
    """일일마감을 차트번호 단위로 집계: {차트번호: {name, amt, methods}}.

    daily_refund(환불/취소 행)를 주면 같은 차트번호에서 차감해 net으로 비교한다.
    차트마감은 환불을 음수 행으로 합산하므로, 일마 쪽도 환불을 빼야 '당일 결제 후
    당일 환불'(net 0) 환자가 가짜 금액불일치(유형C2)로 잡히지 않는다.
    """
    out = {}
    has_daily = daily is not None and not daily.empty and "차트번호" in daily.columns
    has_refund = (daily_refund is not None and not daily_refund.empty
                  and "차트번호" in daily_refund.columns)
    if not has_daily and not has_refund:
        return out
    raw = {}
    if has_daily:
        df = daily.copy()
        df["차트번호"] = df["차트번호"].astype(str).str.strip()
        df = df[df["차트번호"] != ""]
        name_col = "성명" if "성명" in df.columns else ("이름" if "이름" in df.columns else None)
        for ch, g in df.groupby("차트번호"):
            name = ""
            if name_col:
                for n in g[name_col].astype(str):
                    t = n.strip()
                    if t and t.lower() != "nan":
                        name = t
                        break
            amt = int(g["총액"].sum()) if "총액" in g.columns else 0
            meth = {}
            for col in ["카드", "현금", "이체"] + _DAILY_PLAT_COLS:
                if col in g.columns:
                    s = int(g[col].sum())
                    if s != 0:
                        meth[col] = s
            raw[ch] = {"name": name, "amt": amt, "meth": meth}

    # 환불 행 차감 (환불 전용 환자는 음수 항목으로 새로 생성 → 차트 음수 행과 대조)
    if has_refund:
        rdf = daily_refund.copy()
        rdf["차트번호"] = rdf["차트번호"].astype(str).str.strip()
        rdf = rdf[rdf["차트번호"] != ""]
        r_name_col = "성명" if "성명" in rdf.columns else ("이름" if "이름" in rdf.columns else None)
        for ch, g in rdf.groupby("차트번호"):
            ent = raw.setdefault(ch, {"name": "", "amt": 0, "meth": {}})
            if not ent["name"] and r_name_col:
                for n in g[r_name_col].astype(str):
                    t = n.strip()
                    if t and t.lower() != "nan":
                        ent["name"] = t
                        break
            ent["amt"] -= int(g["총액"].sum()) if "총액" in g.columns else 0
            for col in ["카드", "현금", "이체"] + _DAILY_PLAT_COLS:
                if col in g.columns:
                    s = int(g[col].sum())
                    if s != 0:
                        ent["meth"][col] = ent["meth"].get(col, 0) - s

    for ch, ent in raw.items():
        meth = {k: v for k, v in ent["meth"].items() if v != 0}
        if not meth:
            meth["카드"] = 0
        out[ch] = {"name": ent["name"], "amt": ent["amt"], "methods": _verif_fmt_methods(meth),
                   "meth": dict(meth)}
    return out


def build_verification(patient, daily, daily_refund=None, branch="", date=""):
    """차트마감↔일일마감 환자단위 대조로 오류 '건'(어떤 환자·어떤 거래)을 확정 추출.

    유형B  차트번호오타   : 이름·금액 동일·번호만 상이 → 그 건의 번호가 오류
    유형C1 한쪽만존재     : 한 파일에만 있는 환자 → 그 건의 누락/미반영이 오류
    유형C2 금액불일치     : 동일 번호·총액 상이 → 그 건의 금액이 오류
    유형C3 결제수단불일치 : 동일 번호·총액 일치·채널 분배 상이 → 그 건의 결제수단이
                            오류 (채널 합계 차이를 만드는 직접 원인 — 총액 비교만으론
                            안 잡히던 건을 확정 포착)
    반환: {'요약', '유형B_차트번호오타', '유형C1_한쪽만존재', '유형C2_금액불일치',
           '유형C3_결제수단불일치'} DataFrame dict."""
    P = _verif_aggregate_patient(patient)
    D = _verif_aggregate_daily(daily, daily_refund)
    p_only = set(P) - set(D)
    d_only = set(D) - set(P)
    both = set(P) & set(D)

    # ── 유형B: 한쪽만 존재하는 번호끼리 '금액 동일 + (이름유사 or 번호오타)'로 페어링 ──
    typeB = []
    used_d = set()
    for pch in sorted(p_only):
        p = P[pch]
        # 차트마감에만 있고 금액 0원 → 특이사항만 있는 무수납 건이므로 페어링 불필요
        if p["amt"] == 0:
            continue
        best, best_score = None, -1
        for dch in sorted(d_only):
            if dch in used_d:
                continue
            d = D[dch]
            if p["amt"] != d["amt"]:
                continue
            ns = _verif_name_sim(p["name"], d["name"])
            ts = _verif_chart_typo_loose(pch, dch)
            if not (ns or ts):
                continue
            score = (2 if ns else 0) + (1 if _verif_chart_typo_adjacent(pch, dch) else 0)
            if score > best_score:
                best_score, best = score, dch
        if best is not None:
            used_d.add(best)
            d = D[best]
            adj = _verif_chart_typo_adjacent(pch, best)
            cause = ("일일마감 차트번호 수기오류(인접 자릿수 오타)" if adj
                     else "일일마감 차트번호 수기오류(차트번호 상이(이름·금액 동일))")
            typeB.append({
                "지점": branch, "날짜": date, "성명": p["name"] or d["name"],
                "차트마감_차트번호": pch, "일일마감_차트번호": best,
                "차트금액": p["amt"], "일마금액": d["amt"],
                "금액일치": "일치" if p["amt"] == d["amt"] else "불일치",
                "이름일치": "일치" if _verif_name_sim(p["name"], d["name"]) else "불일치",
                "추정원인": cause,
            })
    paired_p = {r["차트마감_차트번호"] for r in typeB}

    # ── 유형C1: 페어링되지 않은 한쪽만 존재 ──
    typeC1 = []
    for pch in sorted(p_only - paired_p):
        p = P[pch]
        # 금액 0원 → 특이사항만 남긴 무수납 건이므로 오류로 잡지 않음
        if p["amt"] == 0:
            continue
        typeC1.append({"지점": branch, "날짜": date, "구분": "차트마감에만 존재",
                       "차트번호": pch, "성명": p["name"], "금액": p["amt"],
                       "결제수단": p["methods"], "동일금액상대": ""})
    for dch in sorted(d_only - used_d):
        d = D[dch]
        # 결제+환불 net 0 (또는 특이사항 0원) 건은 차트에 흔적이 없어도 정상
        if d["amt"] == 0:
            continue
        typeC1.append({"지점": branch, "날짜": date, "구분": "일일마감에만 존재",
                       "차트번호": dch, "성명": d["name"], "금액": d["amt"],
                       "결제수단": d["methods"], "동일금액상대": ""})

    # C1 보강: 양쪽에 '동일 금액'으로 남은 한쪽만존재 건끼리 힌트 연결.
    # 유형B 페어링(이름유사/번호오타)엔 못 미치지만, 이름·번호를 모두 다르게 적은
    # 오기재면 같은 건일 가능성이 높다 → 두 건을 묶어 '오류 건'을 빠르게 특정.
    c1_p = [r for r in typeC1 if r["구분"] == "차트마감에만 존재"]
    c1_d = [r for r in typeC1 if r["구분"] == "일일마감에만 존재"]
    from collections import Counter as _Counter
    n_p_amt = _Counter(r["금액"] for r in c1_p)
    n_d_amt = _Counter(r["금액"] for r in c1_d)
    for rp in c1_p:
        # 양쪽 모두 그 금액이 '단 한 건'일 때만 연결 (다건이면 모호 → 힌트 생략)
        if n_p_amt[rp["금액"]] != 1 or n_d_amt[rp["금액"]] != 1:
            continue
        rd = next(rd for rd in c1_d if rd["금액"] == rp["금액"])
        rp["동일금액상대"] = f"일마 {rd['성명']}(차트{rd['차트번호']}) — 동일건 의심"
        rd["동일금액상대"] = f"차트 {rp['성명']}(차트{rp['차트번호']}) — 동일건 의심"

    # ── 유형C2: 동일 차트번호인데 총액 불일치 ──
    # ── 유형C3: 총액은 일치하나 결제수단 분배가 상이 (채널 합계 차이의 직접 원인) ──
    typeC2, typeC3 = [], []
    for ch in sorted(both):
        p, d = P[ch], D[ch]
        if p["amt"] != d["amt"]:
            typeC2.append({"지점": branch, "날짜": date, "차트번호": ch,
                           "성명": p["name"] or d["name"],
                           "차트금액": p["amt"], "일마금액": d["amt"], "차이": d["amt"] - p["amt"],
                           "차트결제수단": p["methods"], "일마결제수단": d["methods"]})
            continue
        pb = _verif_method_buckets(p.get("meth"))
        db = _verif_method_buckets(d.get("meth"))
        diffs = {k: db[k] - pb[k] for k in pb if db[k] != pb[k]}
        if not diffs:
            continue
        keys = sorted(diffs)
        if len(keys) == 2 and diffs[keys[0]] == -diffs[keys[1]]:
            # 두 채널이 정확히 반대로 어긋남 = 전형적 결제수단 오기재(한쪽 파일이 채널을 잘못 기재)
            cause = f"{keys[0]}↔{keys[1]} 결제수단 오기재 의심 (총액 일치·분배 상이)"
        else:
            cause = "결제수단 분배 불일치(복합) — 해당 환자 결제행 검토"
        typeC3.append({"지점": branch, "날짜": date, "차트번호": ch,
                       "성명": p["name"] or d["name"], "총액": p["amt"],
                       "차트결제수단": p["methods"], "일마결제수단": d["methods"],
                       "차이요약": "; ".join(f"{k} 일마-차트 {v:+,}" for k, v in sorted(diffs.items())),
                       "추정원인": cause})

    colsB = ["지점", "날짜", "성명", "차트마감_차트번호", "일일마감_차트번호",
             "차트금액", "일마금액", "금액일치", "이름일치", "추정원인"]
    colsC1 = ["지점", "날짜", "구분", "차트번호", "성명", "금액", "결제수단", "동일금액상대"]
    colsC2 = ["지점", "날짜", "차트번호", "성명", "차트금액", "일마금액", "차이",
              "차트결제수단", "일마결제수단"]
    colsC3 = ["지점", "날짜", "차트번호", "성명", "총액",
              "차트결제수단", "일마결제수단", "차이요약", "추정원인"]
    dfB = pd.DataFrame(typeB, columns=colsB)
    dfC1 = pd.DataFrame(typeC1, columns=colsC1)
    dfC2 = pd.DataFrame(typeC2, columns=colsC2)
    dfC3 = pd.DataFrame(typeC3, columns=colsC3)
    summary = pd.DataFrame([{
        "지점": branch or "(현재 지점)",
        "유형B_차트번호오타": len(dfB),
        "유형C1_한쪽만존재": len(dfC1),
        "유형C2_금액불일치": len(dfC2),
        "유형C3_결제수단불일치": len(dfC3),
    }])
    return {
        "요약": summary,
        "유형B_차트번호오타": dfB,
        "유형C1_한쪽만존재": dfC1,
        "유형C2_금액불일치": dfC2,
        "유형C3_결제수단불일치": dfC3,
    }


def build_verification_excel(verif):
    """검증 결과를 업로드 샘플과 동일한 형식의 시트별 엑셀로 출력."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for sheet in ["요약", "유형B_차트번호오타", "유형C1_한쪽만존재",
                      "유형C2_금액불일치", "유형C3_결제수단불일치"]:
            if sheet in verif:
                verif[sheet].to_excel(w, sheet_name=sheet, index=False)
    buf.seek(0)
    return buf.getvalue()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gemini API (캐시 + rate limit)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# Gemini 무료 한도 (2026년 기준, 모델별 일일 호출수 RPD 기준):
#   gemini-2.5-flash-lite  : 15 RPM / 250K TPM / 1000 RPD  ← 무료 RPD 최다, 권장 기본값
#   gemini-2.5-flash       : 10 RPM / 250K TPM /  500 RPD
#   gemini-2.0-flash       : 15 RPM /   1M TPM /  200 RPD  ← TPM은 크지만 RPD 적음
#   gemini-2.0-flash-lite  : 30 RPM /   1M TPM /  200 RPD
GEMINI_MODELS = {
    "gemini-2.5-flash-lite": {"rpm": 15, "rpd": 1000, "label": "Flash Lite 2.5 (권장·RPD1000)"},
    "gemini-2.5-flash":      {"rpm": 10, "rpd": 500,  "label": "Flash 2.5 (RPD500)"},
    "gemini-2.0-flash":      {"rpm": 15, "rpd": 200,  "label": "Flash 2.0 (RPD200)"},
    "gemini-2.0-flash-lite": {"rpm": 30, "rpd": 200,  "label": "Flash Lite 2.0 (RPM30)"},
}
GEMINI_FALLBACK_ORDER = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
]


def _gemini_wait(rpm_limit=15):
    """모델별 RPM의 80%로 안전 동작."""
    import time as _t
    if "_g_times" not in st.session_state:
        st.session_state["_g_times"] = []
    safe_rpm = max(2, int(rpm_limit * 0.8))
    now = _t.time()
    st.session_state["_g_times"] = [t for t in st.session_state["_g_times"] if now - t < 60]
    if len(st.session_state["_g_times"]) >= safe_rpm:
        wait = 60 - (now - st.session_state["_g_times"][0]) + 1
        if wait > 0:
            with st.spinner(f"분당 한도 보호 — {int(wait)}초 대기"):
                _t.sleep(wait)
    st.session_state["_g_times"].append(_t.time())


def _cache_path():
    import os, tempfile
    d = os.path.join(tempfile.gettempdir(), "clinic_pay_ai_cache")
    os.makedirs(d, exist_ok=True)
    return d


def _cache_get(k):
    import os, json
    p = os.path.join(_cache_path(), f"{k}.json")
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f).get("result")
    except Exception:
        return None


def _cache_set(k, v):
    import os, json, time as _t
    p = os.path.join(_cache_path(), f"{k}.json")
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"result": v, "ts": _t.time()}, f, ensure_ascii=False)
    except Exception:
        pass


def run_gemini(api_key, data_text, question="", model="gemini-2.5-flash-lite", allow_fallback=True):
    """무료 한도 친화: 캐시 → 호출 → 한도초과 시 다른 무료 모델로 자동 폴백."""
    import hashlib
    from google import genai
    from google.genai import types

    prompt = AI_USER.format(data=data_text)
    if question:
        prompt += f"\n\n추가 질문: {question}"

    # 캐시 키는 데이터+질문 기준 (모델 무관 → 모델 폴백시에도 캐시 재활용)
    ckey = hashlib.md5((data_text + "|" + question).encode()).hexdigest()
    if st.session_state.get("_ai_ck") == ckey and st.session_state.get("_ai_cv"):
        return st.session_state["_ai_cv"], st.session_state.get("_ai_used_model", model)
    p = _cache_get(ckey)
    if p:
        st.session_state["_ai_ck"] = ckey
        st.session_state["_ai_cv"] = p
        return p, model + " (cache)"

    client = genai.Client(api_key=api_key)

    # 폴백 순서: 사용자 선택 모델 우선, 그 후 권장 순서
    if allow_fallback:
        try_order = [model] + [m for m in GEMINI_FALLBACK_ORDER if m != model]
    else:
        try_order = [model]

    last_err = None
    for try_model in try_order:
        rpm = GEMINI_MODELS.get(try_model, {}).get("rpm", 10)
        _gemini_wait(rpm_limit=rpm)
        # 모델당 1회만 시도 (재시도 대기 누적 방지) → 즉시 다음 모델로
        try:
            r = client.models.generate_content(
                model=try_model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=AI_SYSTEM,
                    max_output_tokens=900,
                    temperature=0.15,
                ),
            )
            out = r.text
            st.session_state["_ai_ck"] = ckey
            st.session_state["_ai_cv"] = out
            st.session_state["_ai_used_model"] = try_model
            _cache_set(ckey, out)
            return out, try_model
        except Exception as e:
            last_err = e
            err = str(e)
            if "429" in err or "rate" in err.lower() or "quota" in err.lower() or "resource" in err.lower():
                # 한도 초과 → 다음 모델 즉시 시도
                if "_g_times" in st.session_state:
                    st.session_state["_g_times"] = []
                if not allow_fallback:
                    break
                continue
            # 다른 오류는 즉시 raise (인증 등)
            raise

    # 모든 모델 한도 초과
    raise last_err if last_err else RuntimeError("All Gemini models quota exceeded")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def filter_to_single_date(patient, hansol, daily, picked_date=None):
    """하루 대사 모드에서 다일(월간) 파일을 분석 날짜 하루로 좁힌다.

    반환: (patient, hansol, note, error). error가 비어 있지 않으면 분석을 중단해야 한다.
    날짜 결정 우선순위:
      ① 구글시트 연동에서 고른 날짜
      ② 차트마감 수납일이 하루뿐이면 그 날
      ③ (파일 업로드 일마) 일일마감 차트번호와 겹침이 가장 큰 수납일
    ※ 차트마감은 반드시 '수납일' 기준(진료일은 외상·선결제로 어긋날 수 있음).
    """
    note = ""
    p_days = sorted(set(patient["날짜"]) - {""}) if "날짜" in patient.columns else []
    target = f"{picked_date:%Y-%m-%d}" if picked_date is not None else ""

    if len(p_days) > 1:
        if not target:
            d_charts = {c for c in daily["차트번호"].astype(str)} if "차트번호" in daily.columns else set()
            d_charts -= {"", "nan"}
            if not d_charts:
                return patient, hansol, "", (
                    f"차트마감 파일이 여러 날({len(p_days)}일: {p_days[0]}~{p_days[-1]})을 담고 있는데 "
                    "일일마감에 차트번호가 없어 분석 날짜를 정할 수 없습니다. "
                    "해당 날짜 하루치 차트마감을 올리거나, '기간 분석' 모드를 사용하세요."
                )
            best, best_n = "", 0
            for d in p_days:
                n = len(d_charts & set(patient.loc[patient["날짜"] == d, "차트번호"].astype(str)))
                if n > best_n:
                    best, best_n = d, n
            if not best:
                return patient, hansol, "", (
                    "차트마감의 어느 수납일도 일일마감 환자와 겹치지 않습니다. "
                    "다른 지점/기간 파일이 아닌지 확인하세요."
                )
            target = best
            note = (f"차트마감이 {len(p_days)}일치 파일이어서, 일일마감과 환자(차트번호) "
                    f"겹침이 가장 큰 **{target}** 수납일만 분석했습니다.")
        if target not in p_days:
            return patient, hansol, "", (
                f"차트마감에 {target} 수납일 데이터가 없습니다 "
                f"(파일 범위: {p_days[0]} ~ {p_days[-1]})."
            )
        patient = patient[patient["날짜"] == target].reset_index(drop=True)
        if not note:
            note = f"차트마감 {len(p_days)}일치 중 **{target}** 수납일만 분석했습니다."
    elif len(p_days) == 1 and not target:
        target = p_days[0]

    # 한솔도 다일 파일이면 같은 날짜의 거래만 사용
    if hansol is not None and not hansol.empty and "날짜" in hansol.columns and target:
        h_days = sorted(set(hansol["날짜"]) - {""})
        if len(h_days) > 1:
            if target not in h_days:
                return patient, hansol, "", (
                    f"한솔페이 파일에 {target} 거래가 없습니다 "
                    f"(파일 범위: {h_days[0]} ~ {h_days[-1]})."
                )
            hansol = hansol[hansol["날짜"] == target].reset_index(drop=True)
            note = (note + "  \n" if note else "") + \
                f"한솔페이 {len(h_days)}일치 중 **{target}** 거래만 사용했습니다."
    return patient, hansol, note, ""


def _fmt_period_amt(v):
    try:
        return f"{int(v):,}"
    except Exception:
        return "-"


def _render_period_results():
    ss = st.session_state
    if st.button("🔄 새 파일로 다시"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    table = ss["per_table"]
    hansol, patient = ss["per_hansol"], ss["per_patient"]
    gs_branch = ss.get("per_gs_branch")
    gs_skipped = ss.get("per_gs_skipped") or {}
    gs_daily = ss.get("per_gs_daily") or {}
    has_gs = "일마-차트카드차이" in table.columns

    def _num(col):
        """일마 컬럼은 시트없는 날이 None이므로 숫자 Series로 안전 변환."""
        return pd.to_numeric(table[col], errors="coerce").fillna(0).astype(int)

    bad_card = table[table["카드차이"] != 0]
    bad_cash = table[table["현금차이"] != 0]
    bad_days = set(bad_card["날짜"]) | set(bad_cash["날짜"])
    if has_gs:
        bad_days |= set(table.loc[_num("일마-차트카드차이") != 0, "날짜"])
        bad_days |= set(table.loc[_num("일마-차트현금차이") != 0, "날짜"])
    bad_days = sorted(bad_days)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("기간", f"{table['날짜'].min()} ~ {table['날짜'].max()}", f"{len(table)}일")
    k2.metric("카드 차이 발생일", f"{len(bad_card)}일", delta_color="inverse")
    k3.metric("현금 차이 발생일", f"{len(bad_cash)}일", delta_color="inverse")
    k4.metric("카드 차이 합계", f"{int(table['카드차이'].abs().sum()):,}원", delta_color="inverse")

    if gs_branch:
        n_cmp = len(table) - len(gs_skipped)
        st.caption(f"📒 구글시트 일일마감 비교: **{gs_branch}** · {n_cmp}/{len(table)}일 비교됨")
        if gs_skipped:
            _lines = "\n".join(f"- **{d}** — {r}" for d, r in sorted(gs_skipped.items()))
            st.warning(
                "📋 아래 날짜는 구글시트에서 일일마감 정보가 확인되지 않아 "
                "**해당 날짜를 빼고** 일일마감 검증을 진행했습니다. "
                "해당 날짜의 시트 입력 여부(날짜 탭·데이터)를 확인해 주세요. "
                "나머지 날짜는 정상적으로 분석을 시행했습니다.\n\n" + _lines
            )

    st.markdown("### 📅 일자별 분석표 (한솔 거래일 ↔ 차트 수납일"
                + (" ↔ 구글시트 일일마감)" if has_gs else ")"))
    st.caption("차이 0원 = 완전 일치. **현금차이**는 한솔 현금영수증 발행분과의 비교이므로 "
               "영수증 미발행 현금이 있으면 차트가 더 클 수 있습니다(참고 지표)."
               + (" **일마-차트 차이**는 같은 프론트 기록끼리의 비교라 0원이 정상입니다." if has_gs else ""))

    RED = "background-color: #ffcccc; color: #8b0000; font-weight: 700"
    ORANGE = "background-color: #ffe6cc; color: #8b4500; font-weight: 700"

    def _style_row(row):
        styles = [""] * len(row)
        cols = row.index.tolist()
        for i, c in enumerate(cols):
            try:
                v = int(row[c])
            except (TypeError, ValueError):
                continue
            if c in ("카드차이", "일마-차트카드차이") and v != 0:
                styles[i] = RED
            elif c in ("현금차이", "일마-차트현금차이") and v != 0:
                styles[i] = ORANGE
        return styles

    amt_cols = [c for c in table.columns if c not in ("날짜", "한솔건수", "차트건수", "일마비고")]
    num_cols = [c for c in table.columns if c not in ("날짜", "일마비고")]
    total_row = {"날짜": "합계",
                 **{c: int(pd.to_numeric(table[c], errors="coerce").fillna(0).sum())
                    for c in num_cols}}
    if "일마비고" in table.columns:
        total_row["일마비고"] = ""
    disp = pd.concat([table, pd.DataFrame([total_row])], ignore_index=True)
    styler = disp.style.format({c: _fmt_period_amt for c in amt_cols}).apply(_style_row, axis=1)
    st.dataframe(styler, width="stretch", hide_index=True, height=min(40 + 36 * len(disp), 1000))

    st.download_button(
        "⬇️ 일자별 분석표 CSV",
        disp.to_csv(index=False).encode("utf-8-sig"),
        file_name="기간분석표.csv", mime="text/csv",
    )

    if not bad_days:
        st.success("✅ 전 기간 카드·현금 합계 일치 — 추가 확인이 필요한 날이 없습니다.")
        return

    st.markdown("### 🎯 차이 원인 거래 (차이 발생일만)")
    sel_day = st.selectbox("확인할 날짜", bad_days, key="per_day")
    un_h, un_p = find_period_day_detail(hansol, patient, sel_day)
    row = table[table["날짜"] == sel_day].iloc[0]
    _gs_cap = ""
    if has_gs and sel_day in gs_daily:
        try:
            _gs_cap = f" · 일마-차트카드차이 **{int(row['일마-차트카드차이']):,}원**"
        except (TypeError, ValueError):
            _gs_cap = ""
    st.caption(f"{sel_day} — 카드차이 **{int(row['카드차이']):,}원** · "
               f"현금차이 **{int(row['현금차이']):,}원**{_gs_cap} "
               "(아래는 승인번호·금액으로 설명되지 않는 거래만 추린 것)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**한솔 측 미설명 거래** ({len(un_h)}건)")
        if un_h.empty:
            st.info("없음")
        else:
            st.dataframe(un_h, width="stretch", hide_index=True)
    with c2:
        st.markdown(f"**차트 측 미설명 수납** ({len(un_p)}건)")
        if un_p.empty:
            st.info("없음")
        else:
            st.dataframe(un_p, width="stretch", hide_index=True)
    st.caption("💡 한솔에만 있으면 차트 기록 누락(또는 카드사 직접 상계), 차트에만 있으면 "
               "한솔 미경유 결제(타 단말·플랫폼) 또는 오입력일 가능성이 큽니다. "
               "결제메모에 경위가 적힌 경우가 많으니 함께 확인하세요.")

    # ── 오류 건 자동 특정: 미설명 잔여 건에서 '같은 건의 입력 오류' 쌍 추출 ──
    pairs = pair_period_typo_suspects(un_h, un_p)
    if not pairs.empty:
        st.markdown("#### 🎯 오류 건 자동 특정 — 같은 건의 입력 오류로 의심되는 쌍")
        st.dataframe(pairs, width="stretch", hide_index=True)
        st.caption("💡 위 쌍은 미설명 잔여 건 중 금액 오타(자릿수 추가/누락·한 자리 오타·"
                   "자리 뒤바뀜) 또는 환불방향 불일치 패턴이 맞아떨어진 것입니다. "
                   "한솔은 PG 원본이므로 **차트 쪽 해당 환자의 그 건**이 오류일 가능성이 큽니다 "
                   "— 위 환자(차트번호)의 해당 결제건부터 확인하세요.")

    # ── 구글시트 일일마감이 있는 날: 차트↔일마 환자단위 확정 대조 ──
    # 그날 '어느 환자의 어느 건'이 오류인지 코드로 확정한다.
    if sel_day in gs_daily:
        daily_day, refund_day = gs_daily[sel_day]
        p_day = patient[patient["날짜"] == sel_day] if "날짜" in patient.columns else patient
        verif_day = build_verification(p_day, daily_day, refund_day,
                                       branch=gs_branch or "", date=sel_day)
        nB = len(verif_day["유형B_차트번호오타"])
        nC1 = len(verif_day["유형C1_한쪽만존재"])
        nC2 = len(verif_day["유형C2_금액불일치"])
        nC3 = len(verif_day["유형C3_결제수단불일치"])
        st.markdown(f"### 🔎 {sel_day} 차트마감 ↔ 일일마감(구글시트) 환자단위 검증")
        if nB + nC1 + nC2 + nC3 == 0:
            st.success("✅ 이날 차트마감과 일일마감은 환자 단위로 완전 일치합니다 "
                       "— 차이 원인은 한솔(PG) 측과의 비교에서 찾으세요.")
        else:
            if nB:
                st.markdown("#### 🅑 차트번호 오타 (이름·금액 동일, 번호만 상이)")
                st.dataframe(verif_day["유형B_차트번호오타"].drop(columns=["지점", "날짜"]),
                             width="stretch", hide_index=True)
            if nC1:
                st.markdown("#### 🅒1 한쪽만 존재 (한 파일에만 있는 환자)")
                st.dataframe(verif_day["유형C1_한쪽만존재"].drop(columns=["지점", "날짜"]),
                             width="stretch", hide_index=True)
            if nC2:
                st.markdown("#### 🅒2 금액 불일치 (동일 차트번호, 수납액 상이)")
                st.dataframe(verif_day["유형C2_금액불일치"].drop(columns=["지점", "날짜"]),
                             width="stretch", hide_index=True)
            if nC3:
                st.markdown("#### 🅒3 결제수단 불일치 (총액 일치, 채널 분배 상이)")
                st.dataframe(verif_day["유형C3_결제수단불일치"].drop(columns=["지점", "날짜"]),
                             width="stretch", hide_index=True)
            st.caption("💡 위 표의 환자(차트번호)가 **그날 오류가 발생한 건**입니다 — "
                       "해당 환자의 그 결제건을 차트마감·일일마감에서 대조하면 "
                       "가장 빨리 정정할 수 있습니다.")
    elif gs_branch and sel_day in gs_skipped:
        st.info(f"ℹ️ {sel_day}은(는) 구글시트 일일마감 정보가 확인되지 않아"
                f"({gs_skipped[sel_day]}) 일일마감 검증에서 제외했습니다. "
                "시트 입력 여부를 확인해 주세요.")


_PER_NO_GSHEET = "(구글시트 비교 안 함)"


def _period_mode_ui():
    st.markdown("### 📅 기간 분석 — 한솔페이 ↔ 차트마감 ↔ 구글시트 일일마감 (여러 날 파일)")
    st.caption("월(기간) 단위로 내려받은 한솔 거래내역과 차트마감(수납) 파일 2개를 "
               "일자별로 자동 비교해 **오류가 있는 날만** 짚어줍니다. "
               "차트마감은 **수납일** 기준으로 집계합니다(진료일 아님). "
               "지점을 선택하면 구글시트 일일마감도 날짜별로 불러와 함께 검증합니다.")

    # 지점 선택: 구글시트 일일마감을 일자별로 불러와 같이 비교(기본 포함).
    # 시트에 없는 날짜는 자동으로 빼고 검증하며, 결과 화면에서 따로 안내한다.
    daily_sheets = get_clinic_daily_sheets()
    _branches = [b for b, u in daily_sheets.items() if sheet_entry_configured(u)]
    gs_branch = None
    if _branches:
        sel = st.selectbox(
            "지점 선택 — 구글시트 일일마감 비교 (권장)",
            _branches + [_PER_NO_GSHEET],
            key="per_branch",
            help="선택한 지점의 일일마감 스프레드시트를 기간 내 날짜별로 불러와 "
                 "차트마감과 함께 3자 검증합니다. 정확한 비교를 위해 분석할 파일과 "
                 "같은 지점을 선택하세요.",
        )
        gs_branch = sel if sel != _PER_NO_GSHEET else None
    else:
        st.warning(
            "등록된 지점 구글시트가 없어 일일마감 비교 없이 진행합니다. app.py 상단의 "
            "CLINIC_DAILY_SHEETS(또는 Streamlit Secrets)에 지점·URL을 입력하세요."
        )

    c1, c2 = st.columns(2)
    with c1:
        f_h = st.file_uploader("한솔페이 거래내역 (필수)", key="per_h_file", help="CSV·XLSX·XLS·XLSB")
        h_pw = st.text_input("비밀번호(선택)", type="password", key="per_h_pw")
    with c2:
        f_p = st.file_uploader("차트마감 — 수납 (필수)", key="per_p_file", help="CSV·XLSX·XLS·XLSB")
        p_pw = st.text_input("비밀번호(선택)", type="password", key="per_p_pw",
                             help="베가스에서 설정한 비밀번호")

    if f_h and f_p:
        if st.button("🚀 기간 분석 시작", type="primary", width="stretch"):
            with st.spinner("기간 분석 중..."):
                try:
                    hansol = parse_hansol(load_file(f_h, password=h_pw))
                    patient = parse_patient(load_file(f_p, password=p_pw))
                except Exception as e:
                    st.error(f"데이터 로딩 실패: {e}")
                    st.stop()
                if hansol.empty:
                    st.error("한솔페이 파일에서 유효한 거래를 읽지 못했습니다.")
                    st.stop()
                if patient.empty:
                    st.error("차트마감 파일을 읽지 못했습니다.")
                    st.stop()
                if (hansol["날짜"] == "").all():
                    st.error("한솔페이 파일에서 거래일을 찾지 못했습니다. '거래일' 컬럼이 있는 "
                             "거래내역 파일인지 확인하세요.")
                    st.stop()
                if (patient["날짜"] == "").all():
                    st.error("차트마감 파일에서 수납일을 찾지 못했습니다. '수납' 파일로 "
                             "다운로드했는지 확인하세요.")
                    st.stop()
                table = compute_period_recon(hansol, patient)

                gs_skipped, gs_daily = {}, {}
                if gs_branch:
                    prog = st.progress(0.0, text="구글시트 일일마감 불러오는 중...")

                    def _gs_prog(i, n, day):
                        prog.progress((i + 1) / max(n, 1),
                                      text=f"구글시트 일일마감 비교 중... {day} ({i + 1}/{n}일)")

                    _gs_cache = st.session_state.setdefault("_gs_cache", {})
                    table, gs_skipped, gs_daily = augment_period_with_gsheet(
                        table, patient, daily_sheets[gs_branch],
                        cache=_gs_cache, progress=_gs_prog,
                    )
                    prog.empty()

                ss = st.session_state
                ss["period_done"] = True
                ss["per_hansol"], ss["per_patient"] = hansol, patient
                ss["per_table"] = table
                ss["per_gs_branch"] = gs_branch
                ss["per_gs_skipped"] = gs_skipped
                ss["per_gs_daily"] = gs_daily
            st.rerun()
    else:
        st.button("🚀 기간 분석 시작", type="primary", width="stretch", disabled=True)
        st.info("한솔페이·차트마감 두 파일을 모두 올리면 시작할 수 있습니다.")


def main():
    st.set_page_config(page_title="정산 3-Way 차이 추적기", layout="wide")
    st.title("📊 BW 컨설팅 AI 정산 분석 시스템")
    st.caption("★ 결제채널별 파일 합계 차이를 먼저 산출 → 차이를 설명할 후보 거래 추적 | 일마+차트 2개 또는 한솔+일마+차트 3개 분석 가능")

    with st.expander("📌 사용 안내", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
### 💳 한솔페이 (선택)
**한솔페이 파일 받기:**
☐ https://cateca.kovan.com/nKIMOS/default.aspx 접속 → 로그인
☐ 거래관리 → 거래내역 조회
☐ 거래구분선택 = '전체거래' ✓
☐ 우측 상단 엑셀파일 다운로드
            """)
        with col2:
            st.markdown("""
### 📝 일일마감
- **시트명 형식**: `26.06.01` (숫자 6자리, `.` 구분)
- 각 날짜별 시트명을 정확히 입력해주세요
- 표준 양식에 맞춰 작성된 자료를 권장합니다
            """)
        with col3:
            st.markdown("""
### 🔐 차트마감 (베가스)
- **비밀번호**: 안내받은 비밀번호 필수 입력
- 베가스 차트 저장 시 설정하는 비밀번호 사용
- 비밀번호 미보유 시:
  **BW컨설팅 | 이두만 상무 / 정용민 센터장** 문의
            """)

    if st.session_state.get("period_done"):
        _render_period_results()
        return

    if "done" not in st.session_state:
        mode = st.radio(
            "분석 모드",
            ["하루 정밀 분석 (일마+차트, 한솔 선택)", "기간 분석 (한솔↔차트↔일마 · 여러 날)"],
            horizontal=True, key="analysis_mode",
            help="기간 분석: 월 단위 한솔·차트마감 파일 2개로 일자별 차이를 한 번에 점검 "
                 "(지점 선택 시 구글시트 일일마감도 함께 비교)",
        )
        if mode.startswith("기간"):
            _period_mode_ui()
            return

        daily_sheets = get_clinic_daily_sheets()
        c1, c2, c3 = st.columns(3)
        # 업로더에 type=[...] 확장자 필터를 두지 않는다. 모바일 브라우저(삼성 인터넷 등)는
        # 이 필터를 HTML accept 속성으로 변환하는데, 일부 환경에서 .xls/.xlsx를 제대로
        # 매칭하지 못해 "파일을 골라도 칸이 비어 있는" 증상이 발생한다.
        # load_file()이 CSV/엑셀/HTML/암호화 형식을 모두 자동 판별하므로 필터는 불필요하다.
        with c1:
            f_h = st.file_uploader("한솔페이 (선택)", key="h", help="CSV·XLSX·XLS·XLSB")
            h_pw = st.text_input("비밀번호(선택)", type="password", key="h_pw")
        with c2:
            # 구글시트 연동을 기본값으로: 일일마감은 언제든 시트에서 불러올 수 있으므로
            # 매번 검증에 포함하는 것을 표준 흐름으로 한다(파일 업로드는 보조 수단).
            daily_mode = st.radio(
                "일일마감 입력 방식",
                ["구글시트 연동", "파일 업로드"],
                horizontal=True,
                key="daily_mode",
            )
            f_d, d_pw = None, ""
            gs_branch, gs_date = None, None
            if daily_mode == "파일 업로드":
                f_d = st.file_uploader("일일마감", key="d", help="CSV·XLSX·XLS·XLSB")
                d_pw = st.text_input("비밀번호(선택)", type="password", key="d_pw")
            else:
                _branches = [b for b, u in daily_sheets.items() if sheet_entry_configured(u)]
                if not _branches:
                    st.warning(
                        "등록된 지점 구글시트가 없습니다. app.py 상단의 "
                        "CLINIC_DAILY_SHEETS(또는 Streamlit Secrets)에 지점·URL을 입력하세요."
                    )
                else:
                    gs_branch = st.selectbox("지점 선택", _branches, key="gs_branch")
                    gs_date = st.date_input("마감 날짜", key="gs_date")
                    st.caption(
                        f"선택 날짜: **{gs_date:%Y-%m-%d}** · 지점: {gs_branch}  \n"
                        "탭 이름은 자동 매칭합니다 (26.06.02 / 06.02 / 6.2 …)."
                    )
        with c3:
            st.markdown("### 🔴 **차트마감 (필수)**")
            st.warning(
                "⚠️ **반드시 '수납' 파일만 선택하세요**\n\n"
                "매출+수납이 아닌 **'수납으로 설정'** 하여 다운로드한 파일을 업로드해주세요."
            )
            f_p = st.file_uploader("파일 선택", key="p", help="CSV·XLSX·XLS·XLSB")
            p_pw = st.text_input("비밀번호(선택)", type="password", key="p_pw", help="베가스에서 설정한 비밀번호")

        daily_ready = (
            f_d is not None if daily_mode == "파일 업로드"
            else (gs_branch is not None and gs_date is not None)
        )
        if daily_ready and f_p:
            if st.button("🚀 분석 시작", type="primary", width="stretch"):
                with st.spinner("분석 중..."):
                    try:
                        if daily_mode == "구글시트 연동":
                            _gs_cache = st.session_state.setdefault("_gs_cache", {})
                            try:
                                daily_raw, _tab = load_gsheet_daily(
                                    daily_sheets[gs_branch], gs_date, cache=_gs_cache
                                )
                            except LookupError:
                                st.error(
                                    f"📋 {gs_branch} 구글시트에서 **{gs_date:%Y-%m-%d}** "
                                    "일일마감 정보가 확인되지 않아 분석할 수 없습니다. "
                                    "해당 날짜 탭이 입력되었는지 시트를 확인해 주세요. "
                                    "(이미 마감 자료가 있다면 탭 이름의 날짜 형식을 확인하세요.)"
                                )
                                st.stop()
                            daily_source = f"구글시트 · {gs_branch} · '{_tab}' 탭"
                        else:
                            daily_raw = load_file(f_d, password=d_pw)
                            daily_source = f"업로드 파일 · {f_d.name}"
                        daily, daily_refund = parse_daily(daily_raw)
                        patient = parse_patient(load_file(f_p, password=p_pw))
                        hansol = parse_hansol(load_file(f_h, password=h_pw)) if f_h else pd.DataFrame()
                    except Exception as e:
                        st.error(f"데이터 로딩 실패: {e}")
                        st.stop()
                    if daily.empty:
                        st.error("일일마감 파싱 실패")
                        st.stop()

                    # 결제수단을 한 건도 못 읽으면(지점 시트가 표준과 너무 다르거나, 하단
                    # 채널 합계(요약블록)에 항목 라벨이 없어 금액열 자동 매핑에 실패) 조용히
                    # 0원으로 진행하지 말고 명확히 경고한다.
                    _pay_cols = [c for c in ["카드", "현금", "이체", "플랫폼합"] if c in daily.columns]
                    if _pay_cols and sum(int(daily[c].sum()) for c in _pay_cols) == 0:
                        st.warning(
                            "⚠️ 일일마감에서 결제수단 금액을 한 건도 읽지 못했습니다(합계 0원). "
                            "해당 지점 마감 시트의 결제수단(카드/현금/이체 등) 열에 머리글이 없거나, "
                            "하단 채널 합계(요약블록)에 항목 라벨이 없어 자동 인식에 실패했을 수 "
                            "있습니다. 결제수단 열 머리글 또는 하단 요약블록의 채널 라벨을 확인해 "
                            "주세요(다른 지점 시트처럼 '카드/현금/이체…' 라벨이 한 열에 세로로 "
                            "있으면 자동 인식됩니다)."
                        )

                    # ── 다일(월간) 차트마감/한솔 파일 → 분석 날짜 하루로 자동 필터 ──
                    # 차트마감은 '수납일' 기준. 월 파일을 그대로 합산하면 합계·매칭이
                    # 전부 어긋나므로 반드시 하루로 좁힌 뒤 기존 파이프라인을 태운다.
                    patient, hansol, _date_note, _date_err = filter_to_single_date(
                        patient, hansol, daily,
                        gs_date if daily_mode == "구글시트 연동" else None,
                    )
                    if _date_err:
                        st.error("🚫 " + _date_err)
                        st.stop()

                    # ── 교차검증: 차트마감↔일일마감이 같은 지점·날짜인지 확인 ──
                    # 양쪽 차트번호 일치율이 기준 미만이면(='block') 합계·통계를 일절
                    # 표시하지 않고 안내만 노출(타 지점/다른 날짜 엿보기 방지).
                    # 차트번호가 없어 대조 불가하면(='warn') 경고만 하고 진행(전환기).
                    _status, _msg, _info = cross_check_daily_patient(daily, patient)
                    if _status == "block":
                        st.error("🚫 " + _msg)
                        st.stop()
                    elif _status == "warn":
                        st.warning("⚠️ " + _msg)

                    # 한솔 파일을 올렸는데 유효 거래를 못 읽으면, 조용히 2-파일 모드로
                    # 넘어가지 말고 사용자에게 한솔이 분석에서 빠졌음을 명확히 알린다.
                    if f_h is not None and hansol.empty:
                        st.warning(
                            "⚠️ 한솔페이 파일을 업로드했지만 유효한 거래내역을 읽지 못했습니다. "
                            "한솔을 제외하고 일마↔차트 2개 파일로만 분석을 진행합니다. "
                            "(파일이 비어 있거나, 조회기간에 거래가 없거나, 예상과 다른 형식일 수 있습니다.)"
                        )

                    has_hansol = not hansol.empty
                    if has_hansol:
                        match_df, matched_h, matched_dc = run_matching(hansol, daily, patient)
                        p1_full, p1_diff = compute_p1(match_df, patient, daily)
                        h_um, d_um = compute_p2(hansol, daily, matched_h, matched_dc)
                    else:
                        match_df, matched_h, matched_dc = pd.DataFrame(), set(), set()
                        p1_full, p1_diff = pd.DataFrame(), pd.DataFrame()
                        h_um, d_um = pd.DataFrame(), pd.DataFrame()

                    totals = compute_totals(hansol, daily, daily_refund, patient)
                    channel_df = compute_channel_recon(totals)
                    suspects_by_channel = {
                        ch: find_channel_suspects(ch, hansol, daily, patient, totals=totals,
                                                  top_n=15, daily_refund=daily_refund)
                        for ch in ["카드", "현금+이체", "플랫폼"]
                    }

                    ss = st.session_state
                    ss["done"] = True
                    ss["daily_source"] = daily_source
                    ss["date_filter_note"] = _date_note
                    ss["has_hansol"] = has_hansol
                    ss["hansol"], ss["daily"], ss["patient"] = hansol, daily, patient
                    ss["daily_refund"] = daily_refund
                    ss["match_df"], ss["matched_h"], ss["matched_dc"] = match_df, matched_h, matched_dc
                    ss["p1_full"], ss["p1_diff"] = p1_full, p1_diff
                    ss["h_um"], ss["d_um"] = h_um, d_um
                    ss["totals"] = totals
                    ss["channel_df"] = channel_df
                    ss["suspects_by_channel"] = suspects_by_channel
                st.rerun()
        else:
            # 필수 파일(일일마감·차트마감)이 빠지면 '분석 시작' 버튼이 통째로 사라져,
            # 한솔만(또는 한솔+일마만) 올린 사용자가 "올려도 아무 일도 안 일어난다"고
            # 느끼는 문제 방지. 버튼은 항상 노출하되 비활성으로 두고, 어떤 파일이
            # 더 필요한지(한솔은 선택) 명시적으로 안내한다.
            st.button("🚀 분석 시작", type="primary", width="stretch", disabled=True)
            _missing = []
            if not daily_ready:
                _missing.append("일일마감")
            if f_p is None:
                _missing.append("차트마감")
            st.info(
                f"분석을 시작하려면 {' · '.join(_missing)} 데이터가 더 필요합니다. "
                "(한솔페이는 선택 항목이며, 한솔만으로는 분석할 수 없습니다.)"
            )

    else:
        ss = st.session_state
        has_hansol = ss.get("has_hansol", True)
        hansol, daily, patient = ss["hansol"], ss["daily"], ss["patient"]
        daily_refund = ss.get("daily_refund", pd.DataFrame())
        match_df, matched_h = ss["match_df"], ss["matched_h"]
        p1_full, p1_diff = ss["p1_full"], ss["p1_diff"]
        h_um, d_um = ss["h_um"], ss["d_um"]
        totals = ss["totals"]
        channel_df = ss["channel_df"]
        suspects_by_channel = ss["suspects_by_channel"]

        if st.button("🔄 새 파일로 다시"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()

        _src = ss.get("daily_source")
        if _src:
            st.caption(f"📄 일일마감 원본: {_src}")
        _date_note = ss.get("date_filter_note")
        if _date_note:
            st.info("📅 " + _date_note)

        # ── 채널 합계 차이 요약 (★메인) ──
        def _nonzero(v):
            if v is None:
                return False
            try:
                if pd.isna(v):
                    return False
            except Exception:
                pass
            return int(v) != 0

        diff_cols = ["한솔-차트", "한솔-일마", "일마-차트"] if has_hansol else ["일마-차트"]

        # 채널 차이 합계·차이 채널 수는 '채널별 파일값의 최대-최소 폭(spread)'으로 센다.
        # 한솔=차트인데 일마만 다르면 한솔-일마·일마-차트가 둘 다 잡혀 같은 원인이 2배로
        # 합산되던 문제(예: 일마 198,000 차이 → 396,000으로 표시)를 막아, 한 채널의 불일치는
        # 한 번만(가장 큰 파일간 격차) 집계한다.
        def _channel_spread(row):
            vals = []
            for c in ("한솔", "일마", "차트"):
                if c not in row.index:
                    continue
                v = row[c]
                if v is None:
                    continue
                try:
                    if pd.isna(v):
                        continue
                    vals.append(int(v))
                except Exception:
                    continue
            return (max(vals) - min(vals)) if len(vals) >= 2 else 0

        channel_gaps = [_channel_spread(r) for _, r in channel_df.iterrows()]
        total_abs_gap = sum(channel_gaps)
        nonzero_count = sum(1 for g in channel_gaps if g != 0)

        if has_hansol:
            n_ok = len(hansol[hansol["tx_status"] == "정상"])
            n_m = len(matched_h)
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("한솔 정상", n_ok)
            k2.metric("자동매칭", n_m, f"{n_m/n_ok*100:.0f}%" if n_ok else "0%")
            k3.metric("🔴 채널 차이 합계", f"{total_abs_gap:,}원", delta_color="inverse")
            k4.metric("⚠️ 차이있는 채널", nonzero_count, delta_color="inverse")
        else:
            k1, k2 = st.columns(2)
            k1.metric("🔴 채널 차이 합계 (일마↔차트)", f"{total_abs_gap:,}원", delta_color="inverse")
            k2.metric("⚠️ 차이있는 채널", nonzero_count, delta_color="inverse")

        title_suffix = "3개 파일" if has_hansol else "일마↔차트 2개 파일"
        st.markdown(f"### ★ 교차분석 합계 ({title_suffix})")
        caption_suffix = "한솔·일마·차트" if has_hansol else "일마·차트"
        st.caption(
            f"각 채널의 {caption_suffix} 합계를 비교 — **차트(비교기준)는 항상 🟦, 일마·한솔은 차트와 같으면 🟦 다르면 🟥**"
        )

        def _fmt_amt(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return "-"
            try:
                return f"{int(v):,}"
            except Exception:
                return "-"

        BLUE = "background-color: #cfe7ff; color: #003366; font-weight: 700"
        RED = "background-color: #ffcccc; color: #8b0000; font-weight: 700"

        def _style_chart_compare(row):
            styles = [""] * len(row)
            cols = row.index.tolist()
            chart_v = row.get("차트")
            try:
                chart_int = int(chart_v) if chart_v is not None and not (isinstance(chart_v, float) and pd.isna(chart_v)) else None
            except Exception:
                chart_int = None
            for i, c in enumerate(cols):
                if c == "차트":
                    # 차트는 비교 기준이므로 값이 있으면 항상 파랑
                    if chart_int is not None:
                        styles[i] = BLUE
                    continue
                if c not in ("일마", "한솔"):
                    continue
                v = row[c]
                if v is None or (isinstance(v, float) and pd.isna(v)) or chart_int is None:
                    continue
                try:
                    same = int(v) == chart_int
                except Exception:
                    continue
                styles[i] = BLUE if same else RED
            return styles

        fmt_cols = {c: _fmt_amt for c in channel_df.columns if c != "채널"}
        styler = (
            channel_df.style
            .format(fmt_cols)
            .apply(_style_chart_compare, axis=1)
        )
        st.dataframe(styler, width="stretch", hide_index=True)

        if nonzero_count == 0:
            st.success("✅ 모든 채널 합계 일치 — 추가 분석 불필요")

        # 탭 3개 - 메인 탭은 의심 후보 추적
        # 검증 결과는 tab3(AI 진단 입력)·tab4(검증 표) 양쪽에서 쓰므로 한 번만 계산
        verif = build_verification(patient, daily, daily_refund)

        tab1, tab2, tab3, tab4 = st.tabs([
            "🎯 차이 원인 추적 (★메인)",
            "🔬 1:1 매칭 상세 (보조)",
            "🤖 AI 진단",
            "🔎 데이터 검증 (오타·누락·불일치)",
        ])

        with tab1:
            if has_hansol:
                st.markdown(
                    "**채널별 차이가 0이 아닌 항목** → 차이를 설명할 후보 거래를 확인하세요.  \n"
                    "🔴 **★★ 동일환자 확정** = 승인번호로 환자 특정 완료 (최우선 수정 대상)  \n"
                    "🟠 **★ 차이값 정확매칭** = 금액 조합이 gap과 수학적으로 일치  \n"
                    "🟡 그 외 = 참고 후보"
                )
            else:
                st.markdown(
                    "**일마↔차트 채널별 차이** — 한솔페이 없이 2개 파일만 비교한 결과입니다.  \n"
                    "🟠 **차트↔일마 카드차이★** = 차트번호별 카드금액 불일치 (gap 근접 시 ★, 결제수단 오기재 단서 포함)  \n"
                    "🟡 그 외 = 현금/이체·플랫폼 결제수단 불일치 환자"
                )
            any_diff = False
            for _, r in channel_df.iterrows():
                ch = r["채널"]
                diffs = []
                for col in diff_cols:
                    if _nonzero(r[col]):
                        diffs.append(f"**{col} = {int(r[col]):+,}원**")
                if not diffs:
                    continue
                any_diff = True
                st.markdown(f"#### 🔴 {ch} 채널 — {' / '.join(diffs)}")
                sus = suspects_by_channel.get(ch, [])
                if not sus:
                    st.info("후보 거래가 추출되지 않았습니다. 원본 파일을 직접 확인하세요.")
                else:
                    # ★★ 항목을 별도 경고 박스로 먼저 표시
                    star2_items = [s for s in sus if "★★" in str(s.get("출처", ""))]
                    if star2_items:
                        for si in star2_items:
                            nm = str(si.get("환자", si.get("환자(추정)", ""))).strip()
                            amt_v = int(si["금액"])
                            st.error(
                                f"🚨 **{si['출처']}** | 환자: **{nm}** | 금액차이: **{amt_v:+,}원**  \n"
                                f"단서: {si.get('단서','')}  \n조치: {si.get('조치','')}"
                            )

                    sus_df = pd.DataFrame(sus)
                    # ★★ → ★ → 일반 우선순위로 정렬 (절대규칙)
                    def _prio(src):
                        s = str(src)
                        if "★★" in s:
                            return 0
                        if "★" in s:
                            return 1
                        return 2
                    sus_df["_prio"] = sus_df["출처"].apply(_prio)
                    sus_df = sus_df.sort_values("_prio", kind="stable").drop(columns=["_prio"]).reset_index(drop=True)
                    sus_df["금액"] = sus_df["금액"].apply(lambda v: f"{int(v):+,}")
                    # 환자명 컬럼 통합 (환자 / 환자(추정) 둘 중 하나)
                    if "환자" not in sus_df.columns and "환자(추정)" in sus_df.columns:
                        sus_df.rename(columns={"환자(추정)": "환자"}, inplace=True)
                    elif "환자" in sus_df.columns and "환자(추정)" in sus_df.columns:
                        sus_df["환자"] = sus_df["환자"].fillna("").replace("", None)
                        sus_df["환자(추정)"] = sus_df["환자(추정)"].fillna("").replace("", None)
                        sus_df["환자"] = sus_df["환자"].combine_first(sus_df["환자(추정)"])
                        sus_df.drop(columns=["환자(추정)"], inplace=True)
                    col_order = [c for c in ["출처", "환자", "금액", "단서", "조치"] if c in sus_df.columns]
                    st.dataframe(sus_df[col_order], width="stretch", hide_index=True)
                st.markdown("---")
            if not any_diff:
                st.success("✅ 추적할 채널 차이 없음")

        with tab2:
            if has_hansol:
                st.caption("1:1 매칭은 본질적 합계 차이가 아니라 매칭 알고리즘의 미매칭 결과 — 참고용")
                sub1, sub2, sub3 = st.tabs(["차트번호별 카드", "한솔 미매칭", "일마 미매칭"])
                with sub1:
                    if p1_diff.empty:
                        st.success("✅ 차트번호별 한솔↔차트 카드금액 모두 일치")
                    else:
                        total_diff = int(p1_diff["차이"].sum())
                        st.warning(f"불일치 {len(p1_diff)}건 / 합계 차이 = {total_diff:+,}원 (1:1 매칭 누적)")
                        cols = [c for c in ["차트번호", "이름", "차트카드", "차트건수", "한솔카드", "한솔건수",
                                            "차이", "일마카드", "차트카드사", "한솔카드사",
                                            "한솔승인번호", "차트승인번호"] if c in p1_diff.columns]
                        st.dataframe(p1_diff[cols], width="stretch", hide_index=True)
                with sub2:
                    if h_um.empty:
                        st.success("✅ 한솔 미매칭 없음")
                    else:
                        st.warning(f"{len(h_um)}건 / 합계 {int(h_um['금액'].sum()):+,}원")
                        cols = [c for c in ["시간표시", "금액", "카드번호", "승인번호", "카드사"] if c in h_um.columns]
                        st.dataframe(h_um[cols].sort_values("금액", key=abs, ascending=False),
                                     width="stretch", hide_index=True)
                with sub3:
                    if d_um.empty:
                        st.success("✅ 일마 미매칭 없음")
                    else:
                        st.warning(f"{len(d_um)}건 / 합계 {int(d_um['카드'].sum()):+,}원")
                        cols = [c for c in ["차트번호", "성명", "카드", "내원순서"] if c in d_um.columns]
                        st.dataframe(d_um[cols].sort_values("카드", key=abs, ascending=False),
                                     width="stretch", hide_index=True)
            else:
                st.caption("한솔페이 없이 분석한 경우 — 일마↔차트 차트번호별 카드 비교")
                p_card_agg = (
                    patient[patient["분류"] == "카드"].groupby("차트번호")
                    .agg(차트카드=("금액", "sum"), 이름=("이름", "first")).reset_index()
                ) if not patient.empty and "분류" in patient.columns else pd.DataFrame()
                d_card_agg = (
                    daily[daily["카드"] > 0].groupby("차트번호")
                    .agg(일마카드=("카드", "sum"), 성명=("성명", "first")).reset_index()
                ) if not daily.empty else pd.DataFrame()
                if not p_card_agg.empty or not d_card_agg.empty:
                    merged = pd.merge(
                        p_card_agg if not p_card_agg.empty else pd.DataFrame(columns=["차트번호", "차트카드", "이름"]),
                        d_card_agg if not d_card_agg.empty else pd.DataFrame(columns=["차트번호", "일마카드", "성명"]),
                        on="차트번호", how="outer"
                    ).fillna({"차트카드": 0, "일마카드": 0})
                    merged["이름"] = merged.get("이름", pd.Series(dtype=str)).fillna(merged.get("성명", pd.Series(dtype=str))).fillna("")
                    merged["차이(일마-차트)"] = merged["일마카드"].astype(int) - merged["차트카드"].astype(int)
                    diff2 = merged[merged["차이(일마-차트)"] != 0].copy()
                    diff2 = diff2.sort_values("차이(일마-차트)", key=abs, ascending=False).reset_index(drop=True)
                    if diff2.empty:
                        st.success("✅ 차트번호별 일마↔차트 카드금액 모두 일치")
                    else:
                        total_diff2 = int(diff2["차이(일마-차트)"].sum())
                        st.warning(f"불일치 {len(diff2)}건 / 합계 차이 = {total_diff2:+,}원")
                        cols2 = [c for c in ["차트번호", "이름", "일마카드", "차트카드", "차이(일마-차트)"] if c in diff2.columns]
                        st.dataframe(diff2[cols2], width="stretch", hide_index=True)
                else:
                    st.info("비교할 데이터가 없습니다.")

        with tab3:
            ai_text = build_ai_text(
                hansol, daily, daily_refund, patient, channel_df,
                p1_full, h_um, d_um, suspects_by_channel, totals=totals, verif=verif,
            )
            st.session_state["_ai_data"] = ai_text

            col_a, col_b = st.columns([2, 1])
            with col_a:
                default_key = st.secrets.get("GOOGLE_API_KEY", "") if "GOOGLE_API_KEY" in st.secrets else ""
                api_key = st.text_input(
                    "Google AI API Key",
                    type="password",
                    value=st.session_state.get("_api_key", default_key),
                    key="_api_key",
                    help="https://aistudio.google.com/apikey 에서 무료 발급 (Secrets 또는 여기 입력)",
                )
            with col_b:
                model_choice = st.selectbox(
                    "모델 (무료 한도 우선)",
                    options=list(GEMINI_MODELS.keys()),
                    index=0,
                    format_func=lambda k: GEMINI_MODELS[k]["label"],
                    key="_model_choice",
                )

            question = st.text_area(
                "추가 질문(선택)",
                placeholder="예: 카드 27,400원 차이가 어디서 났을지 추정",
                height=70,
            )

            est_in = int(len(ai_text) / 2.5) + 250
            est_total = est_in + 900
            col = "🟢" if est_total < 6000 else "🟡" if est_total < 12000 else "🔴"
            rpd = GEMINI_MODELS[model_choice]["rpd"]
            rpm = GEMINI_MODELS[model_choice]["rpm"]
            st.caption(
                f"{col} 통합데이터 {len(ai_text):,}자 / 토큰 ~{est_in}입력+~900출력 = ~{est_total} | "
                f"선택모델 무료한도 RPM{rpm}·RPD{rpd}·TPM250K (한도 시 다른 무료 모델로 자동 폴백)"
            )

            with st.expander("📄 전송 데이터 미리보기"):
                st.code(ai_text, language="text")

            if api_key and st.button("🚀 AI 분석 시작", type="primary"):
                with st.spinner("AI 분석 중..."):
                    try:
                        result, used_model = run_gemini(api_key, ai_text, question, model=model_choice)
                        st.session_state["_ai_result"] = result
                        st.session_state["_ai_used"] = used_model
                    except Exception as e:
                        err = str(e)
                        if "401" in err or "invalid" in err.lower() or "api key" in err.lower():
                            st.error("❌ API 키 오류 — 키가 올바른지 확인하세요.")
                        elif "429" in err or "quota" in err.lower() or "rate" in err.lower() or "resource" in err.lower():
                            st.error(
                                "⚠️ 모든 무료 모델 한도 초과.\n\n"
                                "해결: ① 24시간 후 재시도 (RPD 리셋) "
                                "② 다른 Google 계정으로 새 키 발급 "
                                "③ 결제 활성화 (유료 전환)"
                            )
                        else:
                            st.error(f"오류: {err}")

            if "_ai_result" in st.session_state:
                st.markdown("---")
                used = st.session_state.get("_ai_used", "")
                st.markdown(f"### 📋 AI 분석 결과  _(모델: {used})_")
                st.markdown(st.session_state["_ai_result"])
                st.download_button(
                    "결과 텍스트 저장",
                    data=st.session_state["_ai_result"].encode("utf-8"),
                    file_name=f"AI분석_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
                    mime="text/markdown",
                )

            st.markdown("---")
            with st.expander("📥 (대안) 통합 엑셀 다운로드 — 다른 AI/수작업용"):
                excel = build_ai_excel(
                    p1_diff, h_um, d_um, totals, channel_df, suspects_by_channel,
                    hansol=hansol, daily=daily, patient=patient, p1_full=p1_full, verif=verif,
                    daily_refund=daily_refund,
                )
                st.download_button(
                    "통합 엑셀 다운로드 (검증 + 7시트)",
                    data=excel,
                    file_name=f"정산차이_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                st.caption("시트: V_차트번호오타·V_한쪽만존재·V_금액불일치·V_결제수단불일치(검증) / 0_채널대사(★) / 0_3way통합(★) / 0_의심후보 / 1_차트번호별차이 / 2_한솔미매칭 / 3_일마미매칭 / 4_합계")

        with tab4:
            st.markdown(
                "**차트마감 ↔ 일일마감을 환자 단위로 자동 대조**해 오류가 발생한 '건'을 코드로 확정 추출합니다. "
                "(계산식 기반 — AI 불필요·항상 동일 결과)  \n"
                "🅑 **차트번호 오타** = 이름·금액 같은데 차트번호만 다름 (일일마감 수기 오타)  \n"
                "🅒1 **한쪽만 존재** = 차트마감/일일마감 중 한쪽에만 있는 환자  \n"
                "🅒2 **금액 불일치** = 같은 차트번호인데 수납액이 다름  \n"
                "🅒3 **결제수단 불일치** = 총액은 같은데 카드/현금/이체/플랫폼 분배가 다름 "
                "(채널 합계 차이의 직접 원인 — 총액 비교만으로는 안 보이는 오류)"
            )
            cB = len(verif["유형B_차트번호오타"])
            c1 = len(verif["유형C1_한쪽만존재"])
            c2 = len(verif["유형C2_금액불일치"])
            c3 = len(verif["유형C3_결제수단불일치"])
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("🅑 차트번호 오타", cB)
            mc2.metric("🅒1 한쪽만 존재", c1)
            mc3.metric("🅒2 금액 불일치", c2)
            mc4.metric("🅒3 결제수단 불일치", c3)

            if cB + c1 + c2 + c3 == 0:
                st.success(
                    "✅ 차트번호 오타·누락·금액/결제수단 불일치 없음 — 차트마감과 일일마감이 환자 단위로 완전 일치합니다."
                )
            else:
                if cB:
                    st.markdown("#### 🅑 차트번호 오타 (이름·금액 동일, 번호만 상이)")
                    st.dataframe(
                        verif["유형B_차트번호오타"].drop(columns=["지점", "날짜"]),
                        width="stretch", hide_index=True,
                    )
                if c1:
                    st.markdown("#### 🅒1 한쪽만 존재 (한 파일에만 있는 환자)")
                    st.dataframe(
                        verif["유형C1_한쪽만존재"].drop(columns=["지점", "날짜"]),
                        width="stretch", hide_index=True,
                    )
                    st.caption("💡 '동일금액상대'가 채워진 행은 양쪽이 **같은 건**(이름/번호 오기재)일 "
                               "가능성이 높습니다 — 두 행을 한 건으로 묶어 확인하세요.")
                if c2:
                    st.markdown("#### 🅒2 금액 불일치 (동일 차트번호, 수납액 상이)")
                    st.dataframe(
                        verif["유형C2_금액불일치"].drop(columns=["지점", "날짜"]),
                        width="stretch", hide_index=True,
                    )
                if c3:
                    st.markdown("#### 🅒3 결제수단 불일치 (총액 일치, 채널 분배 상이)")
                    st.dataframe(
                        verif["유형C3_결제수단불일치"].drop(columns=["지점", "날짜"]),
                        width="stretch", hide_index=True,
                    )
                    st.caption("💡 이 환자들은 총액이 맞아 눈에 안 띄지만, **채널 합계 차이를 직접 "
                               "만드는 건**입니다 — 위 '교차분석 합계'의 채널 차이와 대조해 보세요.")

            st.markdown("---")
            st.download_button(
                "📥 검증 결과 엑셀 다운로드 (요약 + 유형B / C1 / C2 / C3)",
                data=build_verification_excel(verif),
                file_name=f"데이터검증_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.caption("시트: 요약 / 유형B_차트번호오타 / 유형C1_한쪽만존재 / 유형C2_금액불일치 / 유형C3_결제수단불일치")


if __name__ == "__main__":
    main()
