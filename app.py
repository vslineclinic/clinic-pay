"""
병원 정산 3-Way 대사 시스템 v2.1
한솔페이 × 일일마감 × 차트마감(환자별집계) 자동 매칭 + 의심건 즉시 탐지

v1 대비 주요 개선:
  1. parse_hansol_time 이중파싱 버그 수정
  2. 승인거절/취소 자동 분류 (한솔페이)
  3. 분할결제 매칭 (한솔 2~3건 합 = 일마 1건, 시간근접 ≤10분)
  4. 현금영수증·이체 매칭 (한솔 현금 ↔ 일마 현금/이체)
  5. 시간-순서 상관 매칭 (동일금액 다건 → 보간)
  6. 일자별 합계매칭 선행 (전체 균형부터 확인)
  7. 환자별집계 결제수단 정밀분류 (카드/현금영수증/통장입금/기타 구분)
  8. 세무위험 자동 탐지 (과소·과다 신고 + 차트번호 불일치)
  9. 본부금(진료비) 차트 금액 통합 – 6,900원 등 본부금 수납 반영
  10. 카드사 정보 매칭 – 결제수단/매입사 카드사명으로 정밀 매칭
  11. 본부금 기반 분할결제 탐지 – 본부금 힌트로 2건 분할 정밀 매칭
"""

import importlib
import io
import re
from datetime import datetime
from itertools import combinations

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="병원 정산 3-Way 대사", layout="wide")


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
    return re.sub(r"\D", "", str(x).split(".")[0])


def clean_name(x):
    if pd.isna(x):
        return ""
    return re.sub(r"[\s\-\*]", "", str(x)).strip()


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


def similar_chart_no(a, b):
    a, b = clean_no(a), clean_no(b)
    if not a or not b:
        return False
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        return sum(c1 != c2 for c1, c2 in zip(a, b)) <= 1
    lo, sh = (a, b) if len(a) > len(b) else (b, a)
    return any(lo[:i] + lo[i + 1:] == sh for i in range(len(lo)))


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

    # 3단계: 기본 비밀번호로 복호화 시도
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

    raise ValueError(f"지원하지 않는 형식이거나 비밀번호가 필요합니다. ({last_error})")


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
        df["승인번호"] = df["승인번호"].apply(clean_no)
        # 승인번호가 없는 건은 실제 결제가 이뤄지지 않은 미승인 건이므로 제외
        df = df[df["승인번호"].astype(str).str.strip() != ""].copy()

    # 시간 파싱
    df["시간_분"] = 0
    df["시간표시"] = ""
    tcol = next((c for c in ["시간", "거래시간", "승인시간"] if c in df.columns), None)
    if tcol:
        tstr = df[tcol].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
        df["시간_분"] = tstr.str[:2].astype(int, errors="ignore") * 60 + tstr.str[2:4].astype(int, errors="ignore")
        df["시간표시"] = tstr.str[:2] + ":" + tstr.str[2:4] + ":" + tstr.str[4:6]

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


def parse_daily(raw):
    """일일마감 파싱: 동적 헤더, 결제수단별 금액"""
    hdr = None
    for i, row in raw.iterrows():
        rs = row.astype(str).str.replace(r"\s", "", regex=True)
        if rs.str.contains("내원|차트번호|성명", na=False).sum() >= 2:
            hdr = i
            break
    if hdr is None:
        st.error("일일마감 파일에서 헤더를 찾을 수 없습니다.")
        return pd.DataFrame()

    df = raw.iloc[hdr + 1:].copy()
    df.columns = [str(c).strip().replace("\n", "") for c in raw.iloc[hdr]]
    df = df.reset_index(drop=True)

    if "성명" in df.columns:
        df = df[df["성명"].notna() & ~df["성명"].astype(str).str.contains("합계|소계", na=False)]
    df = df.reset_index(drop=True)

    df["차트번호"] = df["차트번호"].apply(clean_no)
    df["성명"] = df["성명"].apply(clean_name)

    order_col = next((c for c in df.columns if "내원" in str(c) and "순서" in str(c)), None)
    if order_col is None:
        order_col = next((c for c in df.columns if "내원" in str(c)), None)
    if order_col and order_col in df.columns:
        df["내원순서"] = pd.to_numeric(df[order_col], errors="coerce")
    df["내원순서"] = df.get("내원순서", pd.Series(dtype=float))
    df["내원순서"] = df["내원순서"].fillna(pd.Series(range(1, len(df) + 1))).astype(int)

    pay_map = {
        "카드": ["카드"], "현금": ["현금"], "이체": ["이체"],
        "여신티켓": ["여신티켓", "여신"], "강남언니": ["강남언니"],
        "나만의닥터": ["나만의닥터"], "제로페이": ["제로페이"],
        "기타지역화폐": ["기타-지역화폐", "기타지역화폐"],
    }
    for tgt, cands in pay_map.items():
        mc = next((c for c in cands if c in df.columns), None)
        df[tgt] = df[mc].apply(clean_money) if mc else 0

    df["플랫폼합"] = df["여신티켓"] + df["강남언니"] + df["나만의닥터"] + df["제로페이"] + df["기타지역화폐"]
    df["총액"] = df["카드"] + df["현금"] + df["이체"] + df["플랫폼합"]
    df["d_idx"] = range(len(df))
    return df


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

    df["차트번호"] = df["차트번호"].apply(clean_no)
    df["이름"] = df["이름"].apply(clean_name)

    amt_cols = [c for c in ["비급여(과세총금액)", "비급여(비과세)"] if c in df.columns]
    copay_cols = [c for c in df.columns if "본부금" in str(c) or "본인부담" in str(c)]
    all_amt_cols = amt_cols + copay_cols
    for c in all_amt_cols:
        df[c] = df[c].apply(clean_money)
    df["본부금"] = df[copay_cols].sum(axis=1) if copay_cols else 0
    df["금액"] = df[all_amt_cols].sum(axis=1) if all_amt_cols else 0

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
    df["is_취소"] = cancel_text.str.contains(r"취소|환불", na=False)
    if df["is_취소"].any():
        df.loc[df["is_취소"], "금액"] = -df.loc[df["is_취소"], "금액"].abs()
        df.loc[df["is_취소"], "본부금"] = -df.loc[df["is_취소"], "본부금"].abs()

    card_mask = pay_norm.str.startswith("카드", na=False)
    cash_mask = (
        pay_norm.str.contains("현금", na=False)
        | pay_norm.str.contains("현금영수증", na=False)
        | pay_norm.str.contains("영수증", na=False)
    )
    transfer_mask = (
        pay_norm.isin(["통장", "통장입금"])
        | pay_norm.str.contains("이체", na=False)
        | pay_norm.str.contains("계좌", na=False)
        | pay_norm.str.contains("입금", na=False)
        | pay_norm.str.contains("무통장", na=False)
    )
    platform_mask = pay_norm.str.startswith("기타", na=False)

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
            """결제메모 파싱: 승인번호(6~8자리) 추출 + 플랫폼 키워드 감지
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
            # 승인번호 추출: 6~8자리 숫자 (앞뒤가 숫자가 아닌 경계)
            nums = re.findall(r"(?<!\d)\d{6,8}(?!\d)", s)
            return nums, platform

        parsed = memo.apply(_parse_memo)
        df["승인번호목록"] = parsed.apply(lambda x: x[0])
        df["플랫폼구분"] = parsed.apply(lambda x: x[1])
        # 플랫폼 키워드가 감지된 행 → 분류를 "플랫폼"으로 변경
        plat_mask = df["플랫폼구분"] != ""
        df.loc[plat_mask, "분류"] = "플랫폼"

    df["p_idx"] = range(len(df))
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 매칭 엔진
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def run_matching(hansol, daily, patient):
    """
    8-Pass 매칭:
      P1: 승인번호 직접매칭
      P2: 유일 금액 1:1
      P2b: 카드사+금액 (동일금액 다건 → 카드사 구분)
      P3: 분할결제 2~3건 합 (시간근접)
      P3b: 본부금 기반 분할결제 (차트 본부금 힌트)
      P4: 시간-순서 상관 (동일금액 다건, 카드사 우선)
      P5: 현금영수증 + 이체
      P6: 한솔↔일마 결과 기반 한솔↔차트 크로스레퍼런스 재매칭
    """
    h_ok = hansol[hansol["tx_status"] == "정상"]
    h_card = h_ok[~h_ok["is_현금"]].copy()
    h_cash = h_ok[h_ok["is_현금"]].copy()
    d_card = daily[daily["카드"] > 0].copy()

    matched_h, matched_dc = set(), set()
    results = []

    def add(rule, conf, h_idxs, d_row):
        for hi in h_idxs:
            hr = hansol[hansol["h_idx"] == hi].iloc[0]
            results.append(dict(
                매칭규칙=rule, 확신도=conf,
                한솔_hidx=int(hr["h_idx"]),
                한솔_시간=hr.get("시간표시", ""), 한솔_금액=int(hr["금액"]),
                한솔_카드번호=str(hr.get("카드번호", ""))[:12],
                한솔_카드사=str(hr.get("카드사", "")),
                한솔_승인번호=str(hr.get("승인번호", "")),
                한솔_유형="현금" if hr["is_현금"] else "카드",
                일마_순서=d_row["내원순서"], 일마_성명=d_row["성명"],
                일마_차트=d_row["차트번호"], 일마_카드=int(d_row["카드"]),
            ))
            matched_h.add(hi)
        matched_dc.add(d_row["d_idx"])

    # 승인번호→차트번호 맵 (플랫폼 결제 제외 – 플랫폼은 한솔페이를 경유하지 않음)
    appr_map = {}
    for _, pr in patient.iterrows():
        if pr.get("플랫폼구분", ""):
            continue
        for a in pr["승인번호목록"]:
            appr_map[clean_no(a)] = pr["차트번호"]

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
            if a and a in appr_map:
                ch = appr_map[a]
                dc = d_card[(d_card["차트번호"] == ch) & (~d_card["d_idx"].isin(matched_dc))]
                if not dc.empty:
                    add("P1_승인번호", "🟢HIGH", [hr["h_idx"]], dc.iloc[0])

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
            hc_match = hc[hc["카드사"].str.contains(card_co, na=False, case=False)]
            if len(hc_match) == 1:
                add("P2b_카드사+금액", "🟢HIGH", [hc_match.iloc[0]["h_idx"]], dr)
                break

    # P3
    for _, dr in d_card.iterrows():
        if dr["d_idx"] in matched_dc:
            continue
        target = dr["카드"]
        avail = h_card[~h_card["h_idx"].isin(matched_h)][["h_idx", "금액", "시간_분"]].values.tolist()
        found = False
        for r in [2, 3]:
            if found or len(avail) < r:
                break
            for combo in combinations(range(len(avail)), r):
                items = [avail[k] for k in combo]
                if sum(it[1] for it in items) == target:
                    times = [it[2] for it in items]
                    spread = max(times) - min(times) if times else 999
                    if spread <= 10:
                        idxs = [int(it[0]) for it in items]
                        add(f"P3_분할{r}건", "🟢HIGH" if spread <= 5 else "🟡MED", idxs, dr)
                        found = True
                        break

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

    # P4
    confirmed = [(r["한솔_시간"], r["일마_순서"]) for r in results if r["확신도"] == "🟢HIGH" and r["한솔_시간"]]
    if confirmed:
        confirmed.sort()

        def _t2m(ts):
            p = str(ts).split(":")
            return int(p[0]) * 60 + int(p[1]) if len(p) >= 2 else 0

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
                    hc_co = hc[hc["카드사"].str.contains(card_co, na=False, case=False)]
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
                    한솔_시간=hr.get("시간표시", ""), 한솔_금액=int(amt),
                    한솔_카드번호=str(hr.get("카드번호", "")),
                    한솔_카드사="",
                    한솔_승인번호=str(hr.get("승인번호", "")),
                    한솔_유형="현금영수증",
                    일마_순서=dr["내원순서"], 일마_성명=dr["성명"],
                    일마_차트=dr["차트번호"], 일마_카드=int(dr["카드"]),
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
                for r in [2, 3]:
                    if dr["d_idx"] in matched_dc or len(hc_by_card) < r:
                        break
                    items_list = hc_by_card[["h_idx", "금액", "시간_분"]].values.tolist()
                    for combo in combinations(range(len(items_list)), r):
                        items = [items_list[k] for k in combo]
                        if sum(it[1] for it in items) == target:
                            times = [it[2] for it in items]
                            spread = max(times) - min(times) if times else 999
                            if spread <= 15:
                                idxs = [int(it[0]) for it in items]
                                conf = "🟢HIGH" if spread <= 5 else "🟡MED"
                                add(f"P7_분할레퍼런스{r}건", conf, idxs, dr)
                                break

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
                chart_no = clean_no(appr_map.get(appr, ""))
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

    return pd.DataFrame(results), matched_h, matched_dc


def build_hansol_chart_compare(match_df, patient):
    if match_df.empty:
        return pd.DataFrame(columns=[
            "차트번호", "한솔카드건수", "한솔카드번호", "한솔카드사", "차트카드사", "카드사검증",
        ])

    hc = match_df[match_df["한솔_유형"] == "카드"].copy()
    if hc.empty:
        return pd.DataFrame(columns=[
            "차트번호", "한솔카드건수", "한솔카드번호", "한솔카드사", "차트카드사", "카드사검증",
        ])

    hc["차트번호"] = hc["일마_차트"].apply(clean_no)
    hc["한솔카드번호_norm"] = hc["한솔_카드번호"].apply(lambda x: clean_no(x)[:12])
    grp = hc.groupby("차트번호").agg(
        한솔카드건수=("한솔카드번호_norm", "count"),
        한솔카드번호=("한솔카드번호_norm", lambda x: ", ".join(sorted(set([v for v in x if v])))),
        한솔카드사=("한솔_카드사", lambda x: ", ".join(sorted(set([str(v).strip() for v in x if str(v).strip()])))),
    ).reset_index()

    p_card = patient[patient["분류"] == "카드"].copy()
    p_map = p_card.groupby("차트번호")["카드사"].apply(
        lambda x: ", ".join(sorted(set([str(v).strip() for v in x if str(v).strip()])))
    ).reset_index().rename(columns={"카드사": "차트카드사"})

    out = grp.merge(p_map, on="차트번호", how="left")
    out["차트카드사"] = out["차트카드사"].fillna("")

    def _judge(row):
        hs = [x.strip() for x in str(row["한솔카드사"]).split(",") if x.strip()]
        ps = [x.strip() for x in str(row["차트카드사"]).split(",") if x.strip()]
        if not hs or not ps:
            return "정보부족"
        return "일치" if any(card_company_match(h, p) for h in hs for p in ps) else "불일치"

    out["카드사검증"] = out.apply(_judge, axis=1)
    return out.sort_values(["카드사검증", "차트번호"]).reset_index(drop=True)


def build_missing_receipts(match_df, patient, daily, hansol):
    """한솔-차트 매칭 기반 누락 추정 수납건 분석"""
    p_card = patient[patient["분류"] == "카드"].copy()

    # 차트 카드건수: 행 개수가 아니라 승인번호 개수 기준(없으면 1건)
    if "승인번호목록" in p_card.columns:
        p_card["승인번호건수"] = p_card["승인번호목록"].apply(
            lambda xs: max(1, len({clean_no(x) for x in (xs if isinstance(xs, list) else []) if clean_no(x)}))
        )
    else:
        p_card["승인번호건수"] = 1

    chart_card = p_card.groupby(["차트번호", "이름"]).agg(
        차트카드금액=("금액", "sum"),
        차트카드건수=("승인번호건수", "sum"),
    ).reset_index()

    # 차트 승인번호 기반 한솔 매칭(차트 1건에 승인번호 2개 이상 입력한 케이스 보강)
    chart_appr = {}
    if "승인번호목록" in p_card.columns:
        for _, pr in p_card.iterrows():
            ch = clean_no(pr.get("차트번호", ""))
            if not ch:
                continue
            chart_appr.setdefault(ch, set())
            vals = pr.get("승인번호목록", [])
            if isinstance(vals, list):
                for a in vals:
                    aa = clean_no(a)
                    if aa:
                        chart_appr[ch].add(aa)

    h_ok_card = hansol[(hansol["tx_status"] == "정상") & (~hansol["is_현금"])].copy()
    h_ok_card["승인번호_norm"] = h_ok_card["승인번호"].apply(clean_no)

    appr_rows = []
    if chart_appr:
        for ch, apprs in chart_appr.items():
            if not apprs:
                continue
            hm = h_ok_card[h_ok_card["승인번호_norm"].isin(apprs)]
            appr_rows.append({
                "차트번호": ch,
                "한솔매칭금액_appr": int(hm["금액"].sum()) if not hm.empty else 0,
                "한솔매칭건수_appr": int(hm["h_idx"].nunique()) if not hm.empty else 0,
            })
    h_appr_agg = pd.DataFrame(appr_rows)

    # 기존 매칭 결과 기반 집계(승인번호가 없거나 누락된 케이스 fallback)
    if not match_df.empty:
        h_card_match = match_df[match_df["한솔_유형"] == "카드"].copy()
        h_card_match["_chart"] = h_card_match["일마_차트"].apply(clean_no)
        agg_cnt_col = "한솔_hidx" if "한솔_hidx" in h_card_match.columns else "한솔_금액"
        h_agg = h_card_match.groupby("_chart").agg(
            한솔매칭금액=("한솔_금액", "sum"),
            한솔매칭건수=(agg_cnt_col, "nunique"),
        ).reset_index().rename(columns={"_chart": "차트번호"})
    else:
        h_agg = pd.DataFrame(columns=["차트번호", "한솔매칭금액", "한솔매칭건수"])

    d_card = daily[daily["카드"] > 0][["차트번호", "성명", "카드"]].copy()
    d_agg = d_card.groupby("차트번호").agg(일마카드금액=("카드", "sum")).reset_index()

    result = chart_card.merge(h_agg, on="차트번호", how="left")
    if not h_appr_agg.empty:
        result = result.merge(h_appr_agg, on="차트번호", how="left")
    else:
        result["한솔매칭금액_appr"] = 0
        result["한솔매칭건수_appr"] = 0

    # 승인번호 기반 결과를 우선 사용, 없으면 기존 매칭 결과 fallback
    result["한솔매칭금액(기존)"] = result["한솔매칭금액"].fillna(0)
    result["한솔매칭건수(기존)"] = result["한솔매칭건수"].fillna(0)
    result["한솔매칭금액"] = result["한솔매칭금액_appr"].fillna(0)
    result["한솔매칭건수"] = result["한솔매칭건수_appr"].fillna(0)
    no_appr_match = result["한솔매칭금액"] == 0
    result.loc[no_appr_match, "한솔매칭금액"] = result.loc[no_appr_match, "한솔매칭금액(기존)"]
    result.loc[no_appr_match, "한솔매칭건수"] = result.loc[no_appr_match, "한솔매칭건수(기존)"]

    result = result.merge(d_agg, on="차트번호", how="left")
    for c in ["한솔매칭금액", "한솔매칭건수", "일마카드금액", "차트카드건수", "차트카드금액"]:
        if c in result.columns:
            result[c] = result[c].fillna(0).astype(int)
    result["차이(차트-한솔)"] = result["차트카드금액"] - result["한솔매칭금액"]

    def _status(row):
        if row["한솔매칭금액"] == 0:
            return "❌한솔매칭없음"
        if row["차이(차트-한솔)"] > 0:
            return "⚠️금액부족"
        if row["차이(차트-한솔)"] < 0:
            return "⚠️초과매칭"
        return "✅일치"

    result["매칭상태"] = result.apply(_status, axis=1)
    missing = result[result["매칭상태"] != "✅일치"].copy()
    return result, missing



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 환자별 3-Way 비교
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_patient_compare(daily, patient):
    d_agg = daily.groupby(["차트번호", "성명"]).agg(
        **{"[일마]카드": ("카드", "sum"), "[일마]현금": ("현금", "sum"),
           "[일마]이체": ("이체", "sum"), "[일마]플랫폼": ("플랫폼합", "sum"),
           "[일마]합계": ("총액", "sum")}
    ).reset_index()

    p_piv = patient.pivot_table(
        index=["차트번호", "이름"], columns="분류", values="금액", aggfunc="sum"
    ).fillna(0).reset_index()
    rmap = {"카드": "[차트]카드", "현금": "[차트]현금", "이체": "[차트]이체", "플랫폼": "[차트]플랫폼", "기타": "[차트]기타"}
    p_piv.rename(columns=rmap, inplace=True)
    for c in rmap.values():
        if c not in p_piv.columns:
            p_piv[c] = 0
    p_piv["[차트]합계"] = p_piv[[c for c in p_piv.columns if c.startswith("[차트]")]].sum(axis=1)
    p_piv.rename(columns={"이름": "성명_차트"}, inplace=True)

    # 본부금 참고 컬럼 추가
    if "본부금" in patient.columns:
        p_copay = patient.groupby(["차트번호", "이름"])["본부금"].sum().reset_index()
        p_copay.rename(columns={"이름": "성명_차트", "본부금": "[차트]본부금(참고)"}, inplace=True)
        p_piv = p_piv.merge(p_copay, on=["차트번호", "성명_차트"], how="left")
        p_piv["[차트]본부금(참고)"] = p_piv["[차트]본부금(참고)"].fillna(0)

    mg = d_agg.merge(p_piv, on="차트번호", how="outer", indicator=True)
    mg["_merge"] = mg["_merge"].astype(str)  # Categorical → str (fillna 호환)
    mg["매칭"] = "✅일치"
    mg.loc[mg["_merge"] == "left_only", "매칭"] = "❌차트누락"
    mg.loc[mg["_merge"] == "right_only", "매칭"] = "❌일마누락"

    # fuzzy
    lo = mg[mg["_merge"] == "left_only"].copy()
    ro = mg[mg["_merge"] == "right_only"].copy()
    used = set()
    for i, dr in lo.iterrows():
        for j, pr in ro.iterrows():
            if j in used:
                continue
            if clean_name(dr.get("성명", "")) == clean_name(pr.get("성명_차트", "")) \
                    and similar_chart_no(dr["차트번호"], pr["차트번호"]):
                mg.at[i, "매칭"] = f"⚠️유사({dr['차트번호']}↔{pr['차트번호']})"
                for c in [c for c in pr.index if str(c).startswith("[차트]")]:
                    mg.at[i, c] = pr[c]
                used.add(j)
                break

    # 숫자 컬럼만 fillna(0), 문자열 컬럼은 빈문자열
    num_cols = mg.select_dtypes(include="number").columns
    mg[num_cols] = mg[num_cols].fillna(0)
    str_cols = mg.select_dtypes(include=["object", "string"]).columns
    mg[str_cols] = mg[str_cols].fillna("")
    for pay in ["카드", "현금", "이체", "플랫폼"]:
        ic, pc = f"[일마]{pay}", f"[차트]{pay}"
        if ic in mg.columns and pc in mg.columns:
            mg[f"△{pay}"] = mg[ic] - mg[pc]

    # 현금+이체 통합 (수납 방식 차이로 인한 불필요한 불일치 제거)
    mg["[일마]현금+이체"] = mg.get("[일마]현금", 0) + mg.get("[일마]이체", 0)
    mg["[차트]현금+이체"] = mg.get("[차트]현금", 0) + mg.get("[차트]이체", 0)
    mg["△현금+이체"] = mg["[일마]현금+이체"] - mg["[차트]현금+이체"]

    def detail(row):
        r = []
        for pay, ico in [("카드", "💳"), ("현금+이체", "💵"), ("플랫폼", "📱")]:
            c = f"△{pay}"
            if c in row and row[c] != 0:
                r.append(f"{ico}{pay}({row[c]:+,.0f})")
        return " / ".join(r) if r else "✅일치"

    mg["불일치상세"] = mg.apply(detail, axis=1)
    int_cols = mg.select_dtypes(include="number").columns
    mg[int_cols] = mg[int_cols].astype(int)
    mg = mg.drop(columns=["_merge"], errors="ignore")
    return mg


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 세무위험
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def tax_risk(hansol, daily, patient, matched_h):
    risks = []
    h_ok = hansol[hansol["tx_status"] == "정상"]
    h_um = h_ok[~h_ok["h_idx"].isin(matched_h)]
    for _, r in h_um.iterrows():
        risks.append(dict(
            위험등급="🔴높음", 유형="과소신고 위험",
            내용=f"한솔 {r.get('시간표시', '')} {r['금액']:,}원 → 차트 미반영",
            금액=int(r["금액"]),
        ))

    d_ch = set(daily["차트번호"].unique())
    p_ch = set(patient["차트번호"].unique())
    dn = dict(zip(daily["차트번호"], daily["성명"]))
    pn = dict(zip(patient["차트번호"], patient["이름"]))
    for dc in d_ch - p_ch:
        nm = dn.get(dc, "")
        if not nm:
            continue
        for pc in p_ch - d_ch:
            if clean_name(nm) == clean_name(pn.get(pc, "")) and similar_chart_no(dc, pc):
                risks.append(dict(
                    위험등급="🟡중간", 유형="차트번호 불일치",
                    내용=f"{nm}: 일마 {dc} ↔ 차트 {pc}", 금액=0,
                ))

    for _, r in hansol[hansol["tx_status"] == "취소"].iterrows():
        risks.append(dict(
            위험등급="🟡중간", 유형="취소거래 확인",
            내용=f"한솔 취소 {r.get('시간표시', '')} {r['금액']:,}원", 금액=int(r["금액"]),
        ))

    return pd.DataFrame(risks) if risks else pd.DataFrame(columns=["위험등급", "유형", "내용", "금액"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI용 통합 엑셀 생성
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_ai_merged_excel(hansol, daily, patient, match_df, hc_compare,
                          missing_all, missing_only, pc, tx, tots,
                          h_um, d_um, matched_h):
    """3개 파일 + 분석결과를 AI가 이해하기 쉬운 단일 엑셀로 생성"""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:

        # ── Sheet 1: AI_안내 (시스템 개요 + 데이터 사전) ──
        guide_rows = [
            ["[시스템 개요]", ""],
            ["시스템명", "병원 정산 3-Way 대사 시스템 v2.1"],
            ["목적", "한솔페이(PG사) × 일일마감(프론트) × 차트마감(EMR) 교차 검증으로 의심거래 탐지"],
            ["분석일시", datetime.now().strftime("%Y-%m-%d %H:%M")],
            ["", ""],
            ["[데이터 출처 설명]", ""],
            ["한솔페이(Sheet: 1_한솔페이)", "PG사(결제대행사)에서 실제 승인/취소된 카드·현금영수증 거래 원본. 금액·시간·승인번호·카드사 포함."],
            ["일일마감(Sheet: 2_일일마감)", "병원 프론트에서 작성하는 일일 환자별 수납 기록. 차트번호·성명·결제수단별 금액 포함."],
            ["차트마감(Sheet: 3_차트마감)", "EMR(전자의무기록)의 환자별 결제 집계. 차트번호·이름·결제수단·금액·본부금 포함. 승인번호목록(결제메모에서 6~8자리 추출)과 플랫폼구분(강언→강남언니, 나닥→나만의닥터, 여신→여신티켓) 포함."],
            ["", ""],
            ["[핵심 시트 설명]", ""],
            ["4_매칭결과", "한솔페이↔일일마감 자동 매칭 결과. 매칭규칙·확신도(HIGH/MED/LOW)·한솔_승인번호 포함. 승인번호는 한솔페이-일일마감-차트정보 연결 키값."],
            ["5_한솔미매칭", "한솔페이에 정상 승인되었으나 일일마감과 매칭되지 않은 건. 누락·오류 가능성 높음."],
            ["6_일마미매칭", "일일마감에 카드 수납이 있으나 한솔페이와 매칭되지 않은 건."],
            ["7_한솔vs차트_누락추정", "한솔페이↔차트마감 간 금액 비교. 매칭없음/금액부족/초과매칭 등 상태 표시."],
            ["8_일마vs차트_수단별비교", "일일마감↔차트마감 결제수단별 금액 비교. 불일치 상세 포함."],
            ["9_세무위험", "자동 탐지된 세무 위험 항목. 과소신고·차트번호불일치·취소거래 등. 위험등급(높음/중간) 포함."],
            ["10_합계비교", "3개 데이터 소스의 결제수단별 합계 교차 비교."],
            ["", ""],
            ["[의심거래 판단 기준 (AI 분석 시 참고)]", ""],
            ["과소신고 위험(높음)", "한솔페이에 정상 승인되었으나 차트에 미반영된 건 → 매출 누락 가능성"],
            ["차트번호 불일치(중간)", "일마/차트 간 동일 환자이나 차트번호가 다른 경우 → 이중 차트 또는 입력 오류"],
            ["취소거래 확인(중간)", "한솔페이에 취소 기록 → 정상 환불 여부 확인 필요"],
            ["한솔 미매칭(높음)", "PG사에 기록 있으나 병원 장부에 없음 → 수납 누락 또는 현금 전환 의심"],
            ["금액 불일치(중간)", "같은 환자의 결제수단별 금액이 소스 간 다름 → 수납 오류 또는 조작 가능성"],
            ["분할결제 패턴(참고)", "동일 시간대 2~3건 소액 분할 → 의도적 분할 여부 확인"],
            ["", ""],
            ["[누적 분석 시 AI 활용 가이드]", ""],
            ["활용법", "이 파일을 여러 일자 분석 결과와 함께 AI에 업로드하여 패턴을 분석하세요."],
            ["누적 패턴 탐지", "특정 환자/차트번호가 반복적으로 미매칭되거나, 특정 시간대에 취소가 집중되는 패턴을 확인하세요."],
            ["프롬프트 예시", "차트정보(3_차트마감)를 기준 원장으로 보고, 4_매칭결과·5_한솔미매칭·6_일마미매칭·7_한솔vs차트_누락추정·8_일마vs차트_수단별비교를 교차검토해 의심거래를 환자별로 찾아주세요. 우선순위는 (1) 차트 반영 없음/부족 금액, (2) 승인번호 불일치, (3) 결제수단 불일치입니다. 결과는 '어떤 환자 수납건부터 확인할지'를 차트번호·이름·금액·근거시트와 함께 Top 10으로 제시해주세요."],
        ]
        guide_df = pd.DataFrame(guide_rows, columns=["항목", "설명"])
        guide_df.to_excel(writer, sheet_name="0_AI안내_데이터사전", index=False)

        # ── Sheet 2: 한솔페이 원본 ──
        h_export = hansol.copy()
        h_cols = [c for c in ["h_idx", "금액", "시간표시", "tx_status", "카드사",
                               "승인번호", "카드번호", "is_현금"] if c in h_export.columns]
        h_export = h_export[h_cols].rename(columns={
            "h_idx": "순번", "tx_status": "거래상태", "is_현금": "현금여부"
        })
        h_export.to_excel(writer, sheet_name="1_한솔페이", index=False)

        # ── Sheet 3: 일일마감 원본 ──
        d_export = daily.copy()
        d_cols = [c for c in ["d_idx", "내원순서", "차트번호", "성명", "카드", "현금",
                               "이체", "여신티켓", "강남언니", "나만의닥터", "제로페이",
                               "기타지역화폐", "플랫폼합", "총액"] if c in d_export.columns]
        d_export = d_export[d_cols].rename(columns={"d_idx": "순번"})
        d_export.to_excel(writer, sheet_name="2_일일마감", index=False)

        # ── Sheet 4: 차트마감 원본 ──
        p_export = patient.copy()
        p_cols = [c for c in ["p_idx", "차트번호", "이름", "분류", "플랫폼구분", "금액", "카드사",
                               "본부금", "승인번호목록"] if c in p_export.columns]
        p_export = p_export[p_cols].rename(columns={"p_idx": "순번"})
        if "승인번호목록" in p_export.columns:
            p_export["승인번호목록"] = p_export["승인번호목록"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        p_export.to_excel(writer, sheet_name="3_차트마감", index=False)

        # ── Sheet 5: 매칭결과 ──
        if not match_df.empty:
            m_export = match_df.copy()
            m_export.to_excel(writer, sheet_name="4_매칭결과", index=False)

        # ── Sheet 6: 한솔 미매칭 ──
        if not h_um.empty:
            h_um_export = h_um.copy()
            h_um_cols = [c for c in ["시간표시", "금액", "카드번호", "승인번호",
                                      "is_현금", "카드사"] if c in h_um_export.columns]
            h_um_export = h_um_export[h_um_cols]
            h_um_export.to_excel(writer, sheet_name="5_한솔미매칭", index=False)
        else:
            pd.DataFrame({"상태": ["한솔 미매칭 건 없음"]}).to_excel(
                writer, sheet_name="5_한솔미매칭", index=False)

        # ── Sheet 7: 일마 미매칭 ──
        if not d_um.empty:
            d_um_export = d_um[["내원순서", "성명", "차트번호", "카드"]].copy()
            d_um_export.to_excel(writer, sheet_name="6_일마미매칭", index=False)
        else:
            pd.DataFrame({"상태": ["일마 미매칭 건 없음"]}).to_excel(
                writer, sheet_name="6_일마미매칭", index=False)

        # ── Sheet 8: 한솔↔차트 누락추정 ──
        if not missing_all.empty:
            miss_cols = [c for c in ["매칭상태", "차트번호", "이름", "차트카드금액",
                                      "차트카드건수", "한솔매칭금액", "한솔매칭건수",
                                      "일마카드금액", "차이(차트-한솔)"] if c in missing_all.columns]
            missing_all[miss_cols].to_excel(writer, sheet_name="7_한솔vs차트_누락추정", index=False)

        # ── Sheet 9: 일마↔차트 수단별 비교 ──
        if not pc.empty:
            pc_cols = [c for c in ["매칭", "차트번호", "성명", "불일치상세",
                                    "[일마]카드", "[차트]카드", "[차트]본부금(참고)",
                                    "[일마]현금+이체", "[차트]현금+이체",
                                    "[일마]플랫폼", "[차트]플랫폼"] if c in pc.columns]
            pc[pc_cols].to_excel(writer, sheet_name="8_일마vs차트_수단별비교", index=False)

        # ── Sheet 10: 세무위험 ──
        if not tx.empty:
            tx.to_excel(writer, sheet_name="9_세무위험", index=False)
        else:
            pd.DataFrame({"상태": ["세무위험 탐지 건 없음"]}).to_excel(
                writer, sheet_name="9_세무위험", index=False)

        # ── Sheet 11: 합계비교 ──
        h_total_base = tots["h_card"] + tots["h_cash"]
        d_cash_xfer = tots["d_cash"] + tots["d_xfer"]
        p_cash_xfer = tots["p_cash"] + tots["p_xfer"]
        p_etc_ex = tots.get("p_etc", 0)
        # 일마감 플랫폼 == 차트마감 플랫폼이면 검증된 금액으로 한솔 합계에 반영
        plat_confirmed_ex = tots["d_plat"] == tots["p_plat"] and tots["d_plat"] > 0
        h_plat_ex = tots["d_plat"] if plat_confirmed_ex else 0
        h_total_ex = h_total_base + h_plat_ex
        labels = ["카드", "현금/영수증+이체"]
        if plat_confirmed_ex:
            labels.append("플랫폼(일마=차트 일치→반영)")
        else:
            labels.append("플랫폼")
        h_vals = [tots["h_card"], tots["h_cash"], h_plat_ex]
        d_vals = [tots["d_card"], d_cash_xfer, tots["d_plat"]]
        p_vals = [tots["p_card"], p_cash_xfer, tots["p_plat"]]
        if p_etc_ex > 0:
            labels.append("기타(미분류)")
            h_vals.append(0)
            d_vals.append(0)
            p_vals.append(p_etc_ex)
        labels.append("합계")
        h_vals.append(h_total_ex)
        d_vals.append(tots["d_tot"])
        p_vals.append(tots["p_tot"])
        summary_data = {
            "구분": labels,
            "한솔페이": h_vals,
            "일일마감": d_vals,
            "차트마감": p_vals,
            "한솔vs차트_차이": [h - p for h, p in zip(h_vals, p_vals)],
            "일마vs차트_차이": [d - p for d, p in zip(d_vals, p_vals)],
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="10_합계비교", index=False)

    buf.seek(0)
    return buf.getvalue()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

st.title("📊 병원 정산 3-Way 대사 v2.1")
st.caption("한솔페이 × 일일마감 × 차트마감 | 자동 매칭 → 의심건 즉시 탐지")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2-Phase UI: 분석 전 → 파일업로드 / 분석 후 → 결과만 표시
# (결과 화면에서 위젯 조작해도 절대 업로드 화면으로 안 돌아감)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if "done" not in st.session_state:
    # ════════════════════════════════════════════
    # Phase 1: 파일 업로드 + 분석 실행
    # ════════════════════════════════════════════
    c1, c2, c3 = st.columns(3)
    with c1:
        f_h = st.file_uploader("📥 한솔페이", type=["csv", "xlsx", "xls", "xlsb"], key="h")
        h_pw = st.text_input(
            "🔐 한솔 파일 비밀번호 (선택)",
            type="password",
            key="h_pw",
            help="비워두면 비밀번호 없음 → 기본값(vsline99!!) 순서로 자동 시도합니다.",
        )
    with c2:
        f_d = st.file_uploader("📥 일일마감", type=["csv", "xlsx", "xls", "xlsb"], key="d")
        d_pw = st.text_input(
            "🔐 일일마감 파일 비밀번호 (선택)",
            type="password",
            key="d_pw",
            help="비워두면 비밀번호 없음 → 기본값(vsline99!!) 순서로 자동 시도합니다.",
        )
    with c3:
        f_p = st.file_uploader("📥 차트마감", type=["csv", "xlsx", "xls", "xlsb"], key="p")
        p_pw = st.text_input(
            "🔐 차트 파일 비밀번호 (선택)",
            type="password",
            key="p_pw",
            help="비워두면 비밀번호 없음 → 기본값(vsline99!!) 순서로 자동 시도합니다.",
        )

    if f_h and f_d and f_p:
        if st.button("🚀 정산 분석 시작", type="primary", use_container_width=True):
            with st.spinner("매칭 엔진 실행 중..."):
                hansol = parse_hansol(load_file(f_h, password=h_pw))
                daily = parse_daily(load_file(f_d, password=d_pw))
                patient = parse_patient(load_file(f_p, password=p_pw))
                if daily.empty:
                    st.error("일일마감 파일 파싱 실패")
                    st.stop()

                h_ok = hansol[hansol["tx_status"] == "정상"]
                h_cancel = hansol[hansol["tx_status"] == "취소"]
                tots = {
                    "h_card": int(h_ok[~h_ok["is_현금"]]["금액"].sum()) - int(h_cancel[~h_cancel["is_현금"]]["금액"].sum()),
                    "h_cash": int(h_ok[h_ok["is_현금"]]["금액"].sum()) - int(h_cancel[h_cancel["is_현금"]]["금액"].sum()),
                    "d_card": int(daily["카드"].sum()),
                    "d_cash": int(daily["현금"].sum()),
                    "d_xfer": int(daily["이체"].sum()),
                    "d_plat": int(daily["플랫폼합"].sum()),
                    "d_tot": int(daily["총액"].sum()),
                    "p_card": int(patient[patient["분류"] == "카드"]["금액"].sum()),
                    "p_cash": int(patient[patient["분류"] == "현금"]["금액"].sum()),
                    "p_xfer": int(patient[patient["분류"] == "이체"]["금액"].sum()),
                    "p_plat": int(patient[patient["분류"] == "플랫폼"]["금액"].sum()),
                    "p_etc": int(patient[patient["분류"] == "기타"]["금액"].sum()),
                    "p_tot": int(patient["금액"].sum()),
                }

                match_df, matched_h, matched_dc = run_matching(hansol, daily, patient)
                hc_compare = build_hansol_chart_compare(match_df, patient)
                missing_all, missing_only = build_missing_receipts(match_df, patient, daily, hansol)
                pc = build_patient_compare(daily, patient)
                tx = tax_risk(hansol, daily, patient, matched_h)

                h_um = h_ok[~h_ok["h_idx"].isin(matched_h)]
                d_um = daily[(daily["카드"] > 0) & (~daily["d_idx"].isin(matched_dc))]

                # session_state에 저장
                st.session_state["done"] = True
                st.session_state["hansol"] = hansol
                st.session_state["daily"] = daily
                st.session_state["patient"] = patient
                st.session_state["tots"] = tots
                st.session_state["match_df"] = match_df
                st.session_state["hc_compare"] = hc_compare
                st.session_state["missing_all"] = missing_all
                st.session_state["missing_only"] = missing_only
                st.session_state["pc"] = pc
                st.session_state["tx"] = tx
                st.session_state["h_um"] = h_um
                st.session_state["d_um"] = d_um
                st.session_state["n_ok"] = len(h_ok)
                st.session_state["n_m"] = len(matched_h)

            st.rerun()  # 즉시 Phase 2로 전환
    else:
        st.info("👆 3개 파일을 모두 업로드해주세요.")

else:
    # ════════════════════════════════════════════
    # Phase 2: 결과 표시 (파일 업로더 없음 → 위젯 안전)
    # ════════════════════════════════════════════
    hansol = st.session_state["hansol"]
    daily = st.session_state["daily"]
    patient = st.session_state["patient"]
    tots = st.session_state["tots"]
    match_df = st.session_state["match_df"]
    hc_compare = st.session_state["hc_compare"]
    missing_all = st.session_state["missing_all"]
    missing_only = st.session_state["missing_only"]
    pc = st.session_state["pc"]
    tx = st.session_state["tx"]
    h_um = st.session_state["h_um"]
    d_um = st.session_state["d_um"]
    n_ok = st.session_state["n_ok"]
    n_m = st.session_state["n_m"]
    rate = n_m / n_ok * 100 if n_ok else 0

    # 다시 분석 버튼
    if st.button("🔄 새 파일로 다시 분석하기"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    # ── KPI ──
    st.divider()
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("한솔 유효건", f"{n_ok}")
    k2.metric("자동매칭", f"{n_m}", f"{rate:.0f}%")
    k3.metric("한솔 미매칭", f"{len(h_um)}", delta_color="inverse")
    k4.metric("일마 미매칭", f"{len(d_um)}", delta_color="inverse")
    k5.metric("세무위험", f"{len(tx)}", delta_color="inverse")
    k6.metric("누락추정", f"{len(missing_only)}", delta_color="inverse")

    # ── 탭 ──
    t0, t1, t2, t2b, t3, t4, t5, t6 = st.tabs([
        "📋 일자별 합계매칭", "🚨 의심건", "💳 한솔↔일마", "🧩 한솔↔차트", "📊 일마↔차트", "🔒 세무위험", "📝 메신저 요약", "🤖 AI 통합 다운로드",
    ])

    with t0:
        st.subheader("일자별 합계매칭")
        d_cash_xfer = tots["d_cash"] + tots["d_xfer"]
        p_cash_xfer = tots["p_cash"] + tots["p_xfer"]
        p_etc = tots.get("p_etc", 0)
        # 차트마감 합계: 개별 항목 합산 (p_tot과 일치하는지 검증용)
        p_sum = tots["p_card"] + p_cash_xfer + tots["p_plat"] + p_etc

        # 일마감 플랫폼 == 차트마감 플랫폼이면 검증된 금액으로 한솔 합계에 반영
        plat_confirmed = tots["d_plat"] == tots["p_plat"] and tots["d_plat"] > 0
        h_plat_display = tots["d_plat"] if plat_confirmed else "-"
        h_total_base = tots["h_card"] + tots["h_cash"]
        h_total_with_plat = h_total_base + (tots["d_plat"] if plat_confirmed else 0)

        sm_rows = [
            {"구분": "카드", "한솔페이": tots["h_card"], "일일마감": tots["d_card"], "차트마감": tots["p_card"]},
            {"구분": "현금/영수증+이체", "한솔페이": tots["h_cash"], "일일마감": d_cash_xfer, "차트마감": p_cash_xfer},
            {"구분": "플랫폼", "한솔페이": h_plat_display, "일일마감": tots["d_plat"], "차트마감": tots["p_plat"]},
        ]
        if p_etc > 0:
            sm_rows.append({"구분": "기타(미분류)", "한솔페이": "-", "일일마감": "-", "차트마감": p_etc})
        sm_rows.append({"구분": "합계", "한솔페이": h_total_with_plat, "일일마감": tots["d_tot"], "차트마감": tots["p_tot"]})
        sm = pd.DataFrame(sm_rows)
        if plat_confirmed:
            st.success(f"✅ 플랫폼 결제 {tots['d_plat']:,}원: 일마감=차트마감 일치 → 한솔 합계에 반영 (중복 계산 없음)")
        if p_etc > 0:
            st.info(f"📌 차트마감에 '기타(미분류)' {p_etc:,}원이 있습니다. 카드/현금/이체/플랫폼에 분류되지 않은 금액입니다.")

        def _highlight_vs_chart(row):
            """차트마감 기준 비교: 일치=파란배경, 불일치=붉은배경"""
            styles = [""] * len(row)
            chart_val = row["차트마감"]
            for i, (col, val) in enumerate(row.items()):
                if col in ("구분", "차트마감"):
                    continue
                if str(val) == "-" or str(chart_val) == "-":
                    continue
                try:
                    v1 = int(str(val).replace(",", ""))
                    v2 = int(str(chart_val).replace(",", ""))
                    if v1 == v2:
                        styles[i] = "background-color: #3b82f6; color: white"
                    else:
                        styles[i] = "background-color: #ef4444; color: white"
                except (ValueError, TypeError):
                    pass
            return styles

        styled_sm = sm.style.apply(_highlight_vs_chart, axis=1)
        st.dataframe(styled_sm, use_container_width=True, hide_index=True)

        # 구분별 차이 금액 정리
        st.markdown("#### 구분별 차이 금액")
        diff_rows = []
        diff_rows.append({
            "구분": "카드",
            "한솔 vs 차트": f"{tots['h_card'] - tots['p_card']:+,}",
            "일마 vs 차트": f"{tots['d_card'] - tots['p_card']:+,}",
            "한솔 vs 일마": f"{tots['h_card'] - tots['d_card']:+,}",
        })
        diff_rows.append({
            "구분": "현금/영수증+이체",
            "한솔 vs 차트": f"{tots['h_cash'] - p_cash_xfer:+,}",
            "일마 vs 차트": f"{d_cash_xfer - p_cash_xfer:+,}",
            "한솔 vs 일마": "-",
        })
        if plat_confirmed:
            diff_rows.append({
                "구분": "플랫폼 ✅",
                "한솔 vs 차트": "+0",
                "일마 vs 차트": "+0",
                "한솔 vs 일마": "+0",
            })
        else:
            diff_rows.append({
                "구분": "플랫폼",
                "한솔 vs 차트": "-",
                "일마 vs 차트": f"{tots['d_plat'] - tots['p_plat']:+,}",
                "한솔 vs 일마": "-",
            })
        diff_rows.append({
            "구분": "합계",
            "한솔 vs 차트": f"{h_total_with_plat - tots['p_tot']:+,}",
            "일마 vs 차트": f"{tots['d_tot'] - tots['p_tot']:+,}",
            "한솔 vs 일마": f"{h_total_with_plat - tots['d_tot']:+,}",
        })
        diff_df = pd.DataFrame(diff_rows)

        def _highlight_diff_col(col):
            styles = []
            for val in col:
                if val == "-" or val == "+0":
                    styles.append("")
                    continue
                try:
                    n = int(str(val).replace(",", "").replace("+", ""))
                    styles.append("background-color: #ef4444; color: white" if n != 0 else "")
                except (ValueError, TypeError):
                    styles.append("")
            return styles

        styled_diff = diff_df.style.apply(_highlight_diff_col, subset=["한솔 vs 차트", "일마 vs 차트", "한솔 vs 일마"])
        st.dataframe(styled_diff, use_container_width=True, hide_index=True)

        diffs = []
        if tots["h_card"] != tots["d_card"]:
            diffs.append(f"💳 한솔카드 ≠ 일마카드: 차이 {tots['h_card'] - tots['d_card']:,}")
        if tots["d_plat"] != tots["p_plat"]:
            diffs.append(f"📱 일마플랫폼 ≠ 차트플랫폼: 차이 {tots['d_plat'] - tots['p_plat']:,}")
        if diffs:
            st.warning("\n".join(diffs))
        else:
            st.success("✅ 주요 합계 일치")

        rej = hansol[hansol["tx_status"] == "승인거절"]
        can = hansol[hansol["tx_status"] == "취소"]
        if len(rej) + len(can) > 0:
            cancel_amt = int(can["금액"].sum()) if len(can) > 0 else 0
            msg = f"📌 승인거절 {len(rej)}건 / 취소 {len(can)}건"
            if cancel_amt > 0:
                msg += f" (취소금액 {cancel_amt:,}원 → 순매출에서 차감됨)"
            st.info(msg)

    with t1:
        st.subheader("🚨 즉시 확인 필요")
        prio = []
        if len(h_um):
            prio.append(dict(순위="🔴P1", 항목="한솔 미매칭", 건수=len(h_um), 금액=f"{h_um['금액'].sum():,}"))
        if len(d_um):
            prio.append(dict(순위="🔴P1", 항목="일마 미매칭카드", 건수=len(d_um), 금액=f"{d_um['카드'].sum():,}"))
        if not match_df.empty:
            ml = match_df[match_df["확신도"].isin(["🟡MED", "🔴LOW"])]
            if len(ml):
                prio.append(dict(순위="🟡P2", 항목="추정매칭(수동확인)", 건수=len(ml), 금액=f"{ml['한솔_금액'].sum():,}"))
        if not pc.empty:
            mm = pc[pc["불일치상세"] != "✅일치"]
            if len(mm):
                prio.append(dict(순위="🟠P3", 항목="수단별 불일치", 건수=len(mm), 금액="-"))
        if len(missing_only):
            missing_amt = int(missing_only["차이(차트-한솔)"].sum()) if "차이(차트-한솔)" in missing_only.columns else 0
            prio.append(dict(순위="🟠P3", 항목="한솔↔차트 누락추정", 건수=len(missing_only), 금액=f"{missing_amt:,}"))
        if prio:
            st.dataframe(pd.DataFrame(prio), use_container_width=True, hide_index=True)
        else:
            st.success("🎉 의심건 없음!")

        if len(h_um):
            st.markdown("#### ❌ 한솔 미매칭")
            cols = [c for c in ["시간표시", "금액", "카드번호", "승인번호", "is_현금"] if c in h_um.columns]
            st.dataframe(h_um[cols], use_container_width=True, hide_index=True)
        if len(d_um):
            st.markdown("#### ❌ 일마 미매칭(카드)")
            st.dataframe(d_um[["내원순서", "성명", "차트번호", "카드"]], use_container_width=True, hide_index=True)

    with t2:
        st.subheader("💳 한솔↔일마 매칭")
        st.caption("🟢HIGH 자동확정 | 🟡MED 검토권장 | 🔴LOW 수동확인")
        if not match_df.empty:
            cf = st.multiselect("확신도", ["🟢HIGH", "🟡MED", "🔴LOW"], default=["🟢HIGH", "🟡MED", "🔴LOW"])
            st.dataframe(match_df[match_df["확신도"].isin(cf)].sort_values("일마_순서"),
                         use_container_width=True, hide_index=True)
            st.markdown("##### 규칙별 통계")
            st.dataframe(match_df.groupby("매칭규칙").agg(건수=("매칭규칙", "count"), 금액합=("한솔_금액", "sum")).reset_index(),
                         use_container_width=True, hide_index=True)

    with t2b:
        st.subheader("🧩 한솔↔차트 누락 추정 수납건")
        if not missing_all.empty:
            view = st.radio("표시", ["누락/불일치만", "전체"], horizontal=True, key="t2b_view")
            disp = missing_only if view == "누락/불일치만" else missing_all
            disp_cols = [c for c in ["매칭상태", "차트번호", "이름", "차트카드금액", "차트카드건수",
                                     "한솔매칭금액", "한솔매칭건수", "일마카드금액", "차이(차트-한솔)"] if c in disp.columns]
            st.dataframe(disp[disp_cols].sort_values("매칭상태"), use_container_width=True, hide_index=True)

            # 누락 요약 통계
            st.markdown("#### 누락 분석 요약")
            no_match = missing_all[missing_all["매칭상태"] == "❌한솔매칭없음"]
            under_match = missing_all[missing_all["매칭상태"] == "⚠️금액부족"]
            over_match = missing_all[missing_all["매칭상태"] == "⚠️초과매칭"]
            ok_match = missing_all[missing_all["매칭상태"] == "✅일치"]
            summary_items = []
            summary_items.append(f"✅ 완전일치: {len(ok_match)}건")
            if len(no_match):
                summary_items.append(f"❌ 한솔매칭없음: {len(no_match)}건 (차트금액 합계 {int(no_match['차트카드금액'].sum()):,}원)")
            if len(under_match):
                summary_items.append(f"⚠️ 금액부족: {len(under_match)}건 (부족금액 합계 {int(under_match['차이(차트-한솔)'].sum()):,}원)")
            if len(over_match):
                summary_items.append(f"⚠️ 초과매칭: {len(over_match)}건 (초과금액 합계 {int(abs(over_match['차이(차트-한솔)'].sum())):,}원)")
            for item in summary_items:
                st.markdown(f"- {item}")
        else:
            st.success("✅ 모든 차트 카드수납이 한솔과 정상 매칭되었습니다.")

    with t3:
        st.subheader("📊 일마↔차트 수단별")
        if not pc.empty:
            vw = st.radio("표시", ["불일치만", "전체"], horizontal=True)
            disp = pc if vw == "전체" else pc[pc["불일치상세"] != "✅일치"]
            cols = [c for c in ["매칭", "차트번호", "성명", "불일치상세",
                                "[일마]카드", "[차트]카드", "[차트]본부금(참고)",
                                "[일마]현금+이체", "[차트]현금+이체",
                                "[일마]플랫폼", "[차트]플랫폼"] if c in disp.columns]

            # 차트 컬럼에 파란색 배경 적용
            chart_cols_in_display = [c for c in cols if c.startswith("[차트]")]

            def _highlight_chart_cols(row):
                styles = [""] * len(row)
                for i, (col, val) in enumerate(row.items()):
                    if col in chart_cols_in_display:
                        try:
                            v = float(str(val).replace(",", ""))
                            if v != 0:
                                styles[i] = "background-color: #3b82f6; color: white"
                        except (ValueError, TypeError):
                            pass
                return styles

            styled_pc = disp[cols].style.apply(_highlight_chart_cols, axis=1)
            st.dataframe(styled_pc, use_container_width=True, hide_index=True)

    with t4:
        st.subheader("🔒 세무위험")
        if not tx.empty:
            st.dataframe(tx.sort_values("위험등급"), use_container_width=True, hide_index=True)
            hi = tx[tx["위험등급"] == "🔴높음"]
            if len(hi):
                st.error(f"⚠️ 고위험 {len(hi)}건 (합계 {hi['금액'].sum():,}원)")
        else:
            st.success("✅ 세무위험 없음")

    with t5:
        st.subheader("📝 메신저 정산 요약")
        st.caption("최종매칭 완료 후 차트정보 기준으로 생성 · 복사 버튼으로 바로 붙여넣기")

        # 신환/구환 구분 시도
        type_col = None
        for c in daily.columns:
            c_clean = str(c).replace(" ", "")
            if any(k in c_clean for k in ["신구환", "환자구분", "예약구분", "구분"]):
                # '구분' 컬럼이 결제수단 구분이 아닌지 확인
                sample = daily[c].astype(str).str.cat()
                if any(k in sample for k in ["신환", "구환", "신규", "재진", "초진"]):
                    type_col = c
                    break

        new_appt = new_paid = new_amt = 0
        old_appt = old_paid = old_amt = 0
        if type_col:
            new_mask = daily[type_col].astype(str).str.contains("신환|신규|초진|N", na=False)
            old_mask = ~new_mask
            new_appt = int(new_mask.sum())
            new_paid = int((new_mask & (daily["총액"] > 0)).sum())
            new_amt = int(daily.loc[new_mask, "총액"].sum())
            old_appt = int(old_mask.sum())
            old_paid = int((old_mask & (daily["총액"] > 0)).sum())
            old_amt = int(daily.loc[old_mask, "총액"].sum())

        # 취소+부도
        cancel_count = len(hansol[hansol["tx_status"].isin(["취소", "승인거절"])])

        # 결제수단별 합계 (최종매칭 시 차트정보 기준)
        card_total = int(patient[patient["분류"] == "카드"]["금액"].sum())
        cash_total = int(patient[patient["분류"] == "현금"]["금액"].sum())
        transfer_total = int(patient[patient["분류"] == "이체"]["금액"].sum())
        platform_group = patient[patient["분류"] == "플랫폼"]
        yeoshin = int(platform_group[platform_group["플랫폼구분"] == "여신티켓"]["금액"].sum())
        gangnam = int(platform_group[platform_group["플랫폼구분"] == "강남언니"]["금액"].sum())
        naman = int(platform_group[platform_group["플랫폼구분"] == "나만의닥터"]["금액"].sum())
        zeropay = int(daily["제로페이"].sum())
        local_currency = int(daily["기타지역화폐"].sum())

        # 환불+취소 금액
        refund = int(hansol[hansol["tx_status"] == "취소"]["금액"].sum())

        # Today (차트 기준)
        today_total = int(patient["금액"].sum())

        # 템플릿 생성
        lines = []
        lines.append("--------------------------------------")
        lines.append("VS라인클리닉 인천점")
        lines.append("총 내원 환자 : ")
        if type_col:
            lines.append(f"신환예약 : [    ]명 수납 : {new_paid}명 {new_amt:,}원")
            lines.append(f"구환예약 : [    ]명 수납 : {old_paid}명 {old_amt:,}원")
        else:
            paid_count = int((daily["총액"] > 0).sum())
            lines.append(f"신환예약 : [    ]명 수납 : {paid_count}명 원")
            lines.append(f"구환예약 : [    ]명 수납 : 명 원")
        lines.append(f"총 취소+부도 환자 : {cancel_count}명")
        lines.append(f"Today : {today_total:,}원")
        lines.append("")
        lines.append(f"이체 : {transfer_total:,}원")
        lines.append(f"현금 : {cash_total:,}원")
        lines.append(f"카드 : {card_total:,}원")
        lines.append(f"여신티켓 : {yeoshin:,}원")
        lines.append(f"강남언니 : {gangnam:,}원")
        lines.append(f"나만의닥터 : {naman:,}원")
        lines.append(f"제로페이 : {zeropay:,}원")
        lines.append(f"지역화폐 : {local_currency:,}원" if local_currency > 0 else "지역화폐 : 원")
        lines.append(f"환불+취소 : {refund:,}원")
        lines.append(f"월별 total :          원")

        template_text = "\n".join(lines)
        st.code(template_text, language=None)

        if not type_col:
            st.warning("⚠️ 일일마감 데이터에서 신환/구환 구분 컬럼을 찾지 못했습니다. 신환/구환 수치는 수동으로 입력해주세요.")

    with t6:
        st.subheader("🤖 AI 분석용 통합 파일 다운로드")
        st.caption("한솔페이 + 일일마감 + 차트마감 + 분석결과를 하나의 엑셀 파일로 통합합니다.")

        st.markdown("""
**사용법:** 다운로드한 엑셀 파일을 ChatGPT / Gemini / Claude 등에 업로드하고 아래 프롬프트를 사용하세요.

---

**권장 프롬프트 (복사해서 사용):**

> 첨부된 엑셀 파일은 병원 정산 3-Way 대사(한솔페이 PG사 × 일일마감 × 차트마감) 분석 결과입니다.
> '0_AI안내_데이터사전' 시트를 먼저 읽고 데이터 구조를 파악한 후,
> 아래 항목을 분석해주세요:
>
> 1. 차트정보(3_차트마감)를 기준 원장으로 확정하고, '7_한솔vs차트_누락추정'에서 차트 대비 미반영/금액부족 건을 우선순위로 정렬해주세요.
> 2. '4_매칭결과'와 '6_일마미매칭'을 함께 보고 승인번호/금액/결제수단이 어긋난 건을 환자별로 묶어주세요.
> 3. '5_한솔미매칭'과 '9_세무위험'을 교차검토해 실제 점검이 필요한 의심거래 Top 10을 제시해주세요.
> 4. 출력 형식은 "검토 시작 환자 → 차트번호/이름/수납금액/의심사유/근거시트" 순서로 작성해주세요.
>
> 여러 날짜의 파일이 있다면 동일 차트번호/동일 패턴 반복 여부까지 누적 분석해주세요.

---

**누적 분석 팁:** 여러 날짜의 파일을 한꺼번에 AI에 올리면 반복 패턴(동일 차트번호 반복 미매칭, 특정 시간대 취소 집중 등)을 탐지할 수 있습니다.
        """)

        h_ok = hansol[hansol["tx_status"] == "정상"]
        excel_data = build_ai_merged_excel(
            hansol=hansol, daily=daily, patient=patient,
            match_df=match_df, hc_compare=hc_compare,
            missing_all=missing_all, missing_only=missing_only,
            pc=pc, tx=tx, tots=tots,
            h_um=h_um, d_um=d_um, matched_h=set(match_df["한솔_idx"].tolist()) if not match_df.empty and "한솔_idx" in match_df.columns else set(),
        )
        today_str = datetime.now().strftime("%Y%m%d")
        st.download_button(
            label="📥 AI 통합 엑셀 다운로드",
            data=excel_data,
            file_name=f"정산대사_AI통합_{today_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )

        # 파일 내용 미리보기
        st.markdown("#### 📑 포함 시트 목록")
        preview_data = {
            "시트명": [
                "0_AI안내_데이터사전", "1_한솔페이", "2_일일마감", "3_차트마감",
                "4_매칭결과", "5_한솔미매칭", "6_일마미매칭",
                "7_한솔vs차트_누락추정", "8_일마vs차트_수단별비교",
                "9_세무위험", "10_합계비교",
            ],
            "설명": [
                "AI가 데이터를 이해하기 위한 안내 및 용어 사전",
                f"PG사 거래 원본 ({len(hansol)}건)",
                f"프론트 일일마감 ({len(daily)}건)",
                f"EMR 차트마감 ({len(patient)}건)",
                f"한솔↔일마 자동매칭 ({len(match_df)}건)",
                f"한솔 미매칭 ({len(h_um)}건)",
                f"일마 미매칭 ({len(d_um)}건)",
                f"한솔↔차트 누락추정 ({len(missing_all)}건)",
                f"일마↔차트 수단별 비교 ({len(pc)}건)",
                f"세무위험 ({len(tx)}건)",
                "3개 소스 합계 교차비교",
            ],
        }
        st.dataframe(pd.DataFrame(preview_data), use_container_width=True, hide_index=True)
