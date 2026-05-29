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
from datetime import datetime
from itertools import combinations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="정산 3-Way 차이 추적기", layout="wide")


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
    df.columns = [str(c).strip().replace("\n", "") for c in raw.iloc[hdr]]
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
            r_data.columns = [str(c).strip().replace("\n", "") for c in refund_raw.iloc[r_hdr]]
            r_data = r_data.reset_index(drop=True)
            # 빈 행 제거
            if "성명" in r_data.columns:
                r_data = r_data[r_data["성명"].notna() & (r_data["성명"].astype(str).str.strip() != "")]
            if "차트번호" in r_data.columns:
                r_data = r_data[r_data["차트번호"].notna() & (r_data["차트번호"].astype(str).str.strip() != "")]
            r_data = r_data.reset_index(drop=True)

            if not r_data.empty:
                if "차트번호" in r_data.columns:
                    r_data["차트번호"] = r_data["차트번호"].apply(clean_no)
                if "성명" in r_data.columns:
                    r_data["성명"] = r_data["성명"].apply(clean_name)

                pay_map_r = {
                    "카드": ["카드"], "현금": ["현금"], "이체": ["이체"],
                    "여신티켓": ["여신티켓", "여신"], "강남언니": ["강남언니"],
                    "나만의닥터": ["나만의닥터"], "제로페이": ["제로페이"],
                    "기타지역화폐": ["기타-지역화폐", "기타지역화폐"],
                }
                for tgt, cands in pay_map_r.items():
                    mc = next((c for c in cands if c in r_data.columns), None)
                    r_data[tgt] = r_data[mc].apply(clean_money) if mc else 0

                r_data["플랫폼합"] = r_data["여신티켓"] + r_data["강남언니"] + r_data["나만의닥터"] + r_data["제로페이"] + r_data["기타지역화폐"]
                r_data["총액"] = r_data["카드"] + r_data["현금"] + r_data["이체"] + r_data["플랫폼합"]
                refund_df = r_data

    # --- 메인 데이터 필터링 ---
    if "성명" in df.columns:
        df = df[df["성명"].notna() & ~df["성명"].astype(str).str.contains("합계|소계", na=False)]
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
    )
    transfer_mask = (
        pay_norm.isin(["통장", "통장입금"])
        | pay_norm.str.contains("이체", na=False)
        | pay_norm.str.contains("계좌", na=False)
        | pay_norm.str.contains("입금", na=False)
        | pay_norm.str.contains("무통장", na=False)
        | (df["is_취소"] & cancel_norm.str.contains("이체|계좌|입금", na=False)
           & ~cancel_norm.str.contains("카드|현금", na=False))
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

        cand = d_card[(d_card["차트번호"].isin(charts)) & (~d_card["d_idx"].isin(matched_dc))].copy()
        # 일마에 같은 차트가 여러 줄로 나뉜 경우까지 고려하기 위해 최소 2건 이상이면 탐색
        if len(cand) < 2:
            continue

        target = int(hr["금액"])
        cand_rows = list(cand.to_dict("records"))
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
        card_amt = dr.get("카드", 0)
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
                                    add(f"P9c_소급_분할{r}건", conf, idxs, dr,
                                        note=f"소급재검토: 카드번호 {card_ref[-4:]} 분할")
                                    break

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


def _rank_key(amt, channel_gap):
    """채널 차이값 근처 금액을 우선 + 동률시 큰 금액 우선.
    예: gap=-27,400일 때 27,600(차이200) > 714,000(차이686,600).
    """
    a = abs(int(amt))
    g = abs(int(channel_gap)) if channel_gap else 0
    return (abs(a - g), -a)


def find_channel_suspects(channel, hansol, daily, patient, totals=None, top_n=12):
    """채널 차이를 설명할 후보 거래 추출 (multiset diff + 승인번호 cross-match 기반).

    우선순위:
      ★★ 동일환자 확정(gap일치) — 승인번호로 환자 확정 + 한솔-일마 차이 = channel gap 완전 일치
      ★★ 동일환자 확정         — 승인번호로 환자 확정 + 한솔-일마 금액 불일치
      ★ 차이값 정확매칭 페어   — counter diff 기반 금액 페어가 gap과 수학적으로 일치
      한솔에만 존재★/일마에만 존재★ — gap 근접 금액
      차트↔일마 분류차이       — 참고
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

            # ★★ 승인번호 cross-match (한솔 있을 때만)
            if not patient.empty and "승인번호목록" in patient.columns and "분류" in patient.columns:
                p_card_rows = patient[patient["분류"] == "카드"]
                p_ch_total: dict = {}
                p_ch_name: dict = {}
                for _, pr in p_card_rows.iterrows():
                    ch = clean_no(pr.get("차트번호", ""))
                    nm = str(pr.get("이름", "")).strip()
                    amt = int(pr.get("금액", 0))
                    if ch:
                        p_ch_total[ch] = p_ch_total.get(ch, 0) + amt
                        if nm and ch not in p_ch_name:
                            p_ch_name[ch] = nm

                appr_patient_map: dict = {}
                for _, pr in p_card_rows.iterrows():
                    ch = clean_no(pr.get("차트번호", ""))
                    nm = p_ch_name.get(ch, str(pr.get("이름", "")).strip())
                    amt = int(pr.get("금액", 0))
                    appr_list = pr.get("승인번호목록", [])
                    if not isinstance(appr_list, list):
                        continue
                    for a in appr_list:
                        aa = clean_no(a)
                        if len(aa) < 4:
                            continue
                        keys = [aa]
                        if len(aa) > 8:
                            keys.append(aa[-8:])
                        for key in keys:
                            existing = appr_patient_map.setdefault(key, [])
                            if not any(x[0] == ch for x in existing):
                                existing.append((ch, nm, amt))

                d_ch_card: dict = {}
                if not daily.empty and "카드" in daily.columns:
                    for ch_g, grp in daily[daily["카드"] > 0].groupby("차트번호"):
                        d_ch_card[clean_no(str(ch_g))] = int(grp["카드"].sum())

                star2: list = []
                seen_pair: set = set()
                for _, hr in h_card.iterrows():
                    appr = clean_no(hr.get("승인번호", ""))
                    if not appr or len(appr) < 4:
                        continue
                    h_amt = int(hr["금액"])
                    hits: list = list(appr_patient_map.get(appr, []))
                    if not hits and len(appr) > 8:
                        hits = list(appr_patient_map.get(appr[-8:], []))
                    for (ch, nm, chart_row_amt) in hits:
                        suffix = appr[-8:] if len(appr) >= 8 else appr
                        pair_key = (suffix, ch)
                        if pair_key in seen_pair:
                            continue
                        d_amt = d_ch_card.get(ch, 0)
                        cmp_amt = d_amt if d_amt > 0 else chart_row_amt
                        if h_amt == cmp_amt:
                            seen_pair.add(pair_key)
                            continue
                        seen_pair.add(pair_key)
                        diff = h_amt - cmp_amt
                        gap_match = gap != 0 and diff == gap
                        cn = str(hr.get("카드번호", ""))
                        cn_tail = cn[-5:] if cn and cn != "nan" else ""
                        t = str(hr.get("시간표시", ""))
                        co = str(hr.get("카드사", ""))[:6]
                        amt_detail = f"한솔 {h_amt:,} vs 일마 {d_amt:,}" if d_amt > 0 else f"한솔 {h_amt:,} vs 차트 {chart_row_amt:,}"
                        if d_amt > 0 and d_amt != chart_row_amt:
                            amt_detail += f"(차트 {chart_row_amt:,})"
                        tag = "★★ 동일환자 확정(gap일치)" if gap_match else "★★ 동일환자 확정"
                        star2.append({
                            "출처": tag,
                            "환자": nm or ch,
                            "금액": diff,
                            "단서": f"승인{suffix} {t} 말미{cn_tail} {co} | {amt_detail}",
                            "조치": (
                                f"{'★gap완전일치 → ' if gap_match else ''}"
                                f"환자 {nm}({ch}) 한솔·일마 카드금액 불일치 — 즉시 수정"
                            ),
                        })
                if star2:
                    star2.sort(key=lambda x: (0 if "gap일치" in x["출처"] else 1, -abs(x["금액"])))
                    suspects = star2 + suspects

        # 차트(환자집계) vs 일마 카드 분류 차이 (참고) — 한솔 유무 무관
        if not patient.empty and not daily.empty and "분류" in patient.columns:
            p_card = patient[patient["분류"] == "카드"].groupby("차트번호")["금액"].sum()
            d_by_chart = daily.groupby("차트번호")["카드"].sum()
            common = (set(p_card.index) & set(d_by_chart.index)) - {""}
            mismatches = []
            for ch in common:
                pv, dv = int(p_card.get(ch, 0)), int(d_by_chart.get(ch, 0))
                if pv != dv:
                    mismatches.append((ch, pv, dv))
            # 차트↔일마 차이도 channel gap 근접도 우선
            mismatches.sort(key=lambda x: _rank_key(x[2] - x[1], gap))
            for ch, pv, dv in mismatches[:top_n]:
                suspects.append({
                    "출처": "차트↔일마 분류차이",
                    "환자": name_map.get(str(ch).strip(), ""),
                    "금액": dv - pv,
                    "단서": f"차트{ch} (차트={pv:,} / 일마={dv:,})",
                    "조치": "결제수단(카드↔현금↔이체) 오기재 가능성",
                })

    elif channel == "현금+이체":
        if not patient.empty and not daily.empty:
            p_cx = patient[patient.get("분류", "").isin(["현금", "이체"])].groupby("차트번호")["금액"].sum()
            d_cx = daily.groupby("차트번호").apply(lambda x: int(x["현금"].sum() + x["이체"].sum()))
            common = set(p_cx.index) & set(d_cx.index)
            mismatches = []
            for ch in common:
                pv, dv = int(p_cx.get(ch, 0)), int(d_cx.get(ch, 0))
                if pv != dv:
                    mismatches.append((ch, pv, dv, abs(pv - dv)))
            mismatches.sort(key=lambda x: -x[3])
            for ch, pv, dv, _ in mismatches[:top_n]:
                suspects.append({
                    "출처": "차트↔일마 분류차이",
                    "환자": name_map.get(str(ch).strip(), ""),
                    "금액": dv - pv,
                    "단서": f"차트{ch} / 차트현금+이체={pv:,} 일마={dv:,}",
                    "조치": "현금↔이체↔카드 오기재 확인",
                })

    elif channel == "플랫폼":
        if not patient.empty and not daily.empty:
            p_pl = patient[patient.get("분류", "") == "플랫폼"].groupby("차트번호")["금액"].sum()
            d_pl = daily.groupby("차트번호")["플랫폼합"].sum()
            common = set(p_pl.index) & set(d_pl.index) | set(p_pl.index) | set(d_pl.index)
            mismatches = []
            for ch in common:
                pv, dv = int(p_pl.get(ch, 0)), int(d_pl.get(ch, 0))
                if pv != dv:
                    mismatches.append((ch, pv, dv, abs(pv - dv)))
            mismatches.sort(key=lambda x: -x[3])
            for ch, pv, dv, _ in mismatches[:top_n]:
                suspects.append({
                    "출처": "차트↔일마 플랫폼차이",
                    "환자": name_map.get(str(ch).strip(), ""),
                    "금액": dv - pv,
                    "단서": f"차트{ch} / 차트플랫폼={pv:,} 일마={dv:,}",
                    "조치": "플랫폼 종류/금액 오기재 확인",
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
                  totals=None, max_chars=8000):
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

    # 차이 zero 단축
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
    if not has_nonzero:
        L.append("\n[결과] 모든채널 일치 — 추가분석 불필요")
        return "\n".join(L)[:max_chars]

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
    p_pivot = {}
    if not patient.empty and "분류" in patient.columns:
        for _, r in patient.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            cat = str(r.get("분류", ""))
            amt = _safe_int(r.get("금액", 0))
            d = p_pivot.setdefault(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
            if cat in d:
                d[cat] += amt

    d_pivot = {}
    if not daily.empty:
        for _, r in daily.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            d = d_pivot.setdefault(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
            d["카드"] += _safe_int(r.get("카드", 0))
            d["현금"] += _safe_int(r.get("현금", 0))
            d["이체"] += _safe_int(r.get("이체", 0))
            d["플랫폼"] += _safe_int(r.get("플랫폼합", 0))

    h_card_by_ch = {}
    if p1_full is not None and not p1_full.empty:
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
        h_amt, h_cnt = h_card_by_ch.get(ch, (0, 0))
        diffs = []
        if has_hansol and h_amt != d["카드"]:
            diffs.append(f"한솔카드{h_amt:,}≠일마카드{d['카드']:,}")
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
            (abs(h_amt - d["카드"]) if has_hansol else 0)
            + abs(d["카드"] - p["카드"]) + abs(d["현금"] - p["현금"])
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
            diff_str = " · ".join(diffs)[:90]
            if has_hansol:
                L.append(f"{ch}|{nm}|{p_str}|{d_str}|{h_amt:,}({h_cnt})|{appr}|{diff_str}")
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
    "출력은 추측이 아닌 데이터 기반 추적이어야 하며 다음 규칙을 절대 위반하지 말 것:\n"
    "[R1] 환자명·차트번호·금액·승인번호는 반드시 입력에 실제 존재하는 값만 인용 (창작·반올림·근사 금지).\n"
    "[R2] 모든 수정 제안은 해당 채널 차이값을 산술적으로 정확히 상쇄해야 한다 (마지막에 검증).\n"
    "[R3] 우선순위 절대규칙: ★★(승인번호 동일환자 확정) > ★(금액·gap 매칭) > 일반. 절대 뒤집을 수 없음.\n"
    "[R4] 동일등급 내 정렬: 금액 큰순 → gap일치도 높은 순.\n"
    "[R5] 부호 약속: '한-차=+10,000'은 한솔이 차트보다 10,000원 많음 = 차트누락 or 한솔과다. "
    "'일-차=-5,000'은 일마가 차트보다 5,000원 적음 = 일마누락 or 차트과다.\n"
    "[R6] 한솔=일마=A · 차트=B(A≠B) → 차트 단독 오류로 단정 (PG승인+프론트수납이 모두 A이므로 진실=A).\n"
    "[R7] 동일금액이 다른 결제수단 칼럼에 분산되면 결제수단 오기재 (예: 차트카드=X · 일마현금=X).\n"
    "[R8] 한솔PG-only의 '동일금액일마환자' hint가 있으면 그 환자의 일마 결제수단 오기재로 강하게 의심.\n"
    "[R9] '확인 바랍니다' 같은 일반론 금지. 반드시 '어느 파일·어느 환자·어느 금액을 무엇으로 수정' 형식의 실행 가능한 명령으로 출력."
)

AI_USER = """병원 정산 데이터 (3개 파일을 차트번호·승인번호로 통합한 raw 구조):

{data}

[분석 절차 — 반드시 순서대로 수행]

STEP1. [채널대사] 차이값 확정
  · 채널별 한-차/한-일/일-차 차이를 정확히 메모 (부호 포함).
  · 모든 차이=0이면 "✅ 모든 채널 합계 일치 — 분석 불필요" 한 줄로 즉시 종료.

STEP2. [★★ 승인번호확정매칭] 1순위 처리 (절대 먼저)
  · 코드가 한솔승인번호 ↔ 차트승인번호목록 매칭으로 확정한 환자 = 환자 특정 完了.
  · 단서의 '한솔 X vs 일마 Y' 표기 해석:
      - 한솔=일마 일치(차트만 다름) → R6에 따라 차트를 한솔금액으로 수정.
      - 한솔≠일마 → PG영수증이 진실 → 일마를 한솔금액으로 수정.

STEP3. [차트번호별 3-way 통합]에서 5가지 패턴 코드로 분류
  Pa(결제수단 오기재): 차트(X/0/0/0) · 일마(0/X/0/0) → "차트# {{ch}} 환자{{nm}}: 차트 결제수단을 카드→현금으로 수정"
  Pb(카드↔이체 오기재): 차트(X/0/0/0) · 일마(0/0/X/0) → 차트 결제수단을 카드→이체로 수정
  Pc(차트 중복기재):   차트(2X/...) · 일마(X/...) · 한솔(X) → "차트# {{ch}}의 X원 카드행 1개 삭제"
  Pd(차트 누락):       차트(0/0/0/0) · 일마(X/Y/Z/W) → "차트# {{ch}}에 해당 결제건 추가"
  Pe(차트 금액오류):   차트(X) · 일마=한솔=Y, X≠Y → R6 → 차트를 Y로 수정
  · 위 5패턴 중 하나로 분류되지 않으면 '복합 — 수동확인' 으로 표기 (창작금지).

STEP4. [한솔 PG-only] · [일마 front-only] cross-match
  · 한솔PG-only 행의 '동일금액일마환자' hint 존재 → R8 → 그 환자 일마 결제수단을 카드로 정정.
  · 한솔PG-only 금액 = 일마front-only 금액 페어 발견 → 환자 동일성 검증 후 매칭처리 권고.
  · hint 없는 한솔PG-only → 일마·차트에 결제건 누락 → '추가' 권고.
  · hint 없는 일마front-only → PG승인 없음 → 미수납 or 결제수단 오기재 (현금·이체 가능성).

STEP5. [환불/취소] 행 부호 검증
  · 차트환불/일마환불 행이 채널대사에 정상 반영됐는지 확인 (음수 누락 시 차이 발생 가능).

STEP6. 산술 상쇄 검증 (R2 — 출력 직전 필수)
  · 제안한 수정들의 채널별 net 변화량을 합산해 채널대사 차이값과 정확히 일치하는지 확인.
  · 불일치 → 남은 차이만큼 추가후보 탐색 (한솔PG-only / 일마front-only / 환불행 재확인).

[출력 형식 — 1100토큰 이내, 마크다운]

### 채널별 차이 진단
차이≠0 채널마다 ↓
- **{{채널}}**: 한-차=±?원 · 한-일=±?원 · 일-차=±?원
  - **패턴**: `★★확정` 또는 `Pa/Pb/Pc/Pd/Pe` 또는 `복합-수동확인`
  - **수정대상** (★★ → ★ → 일반 순 강제):
    1. `★★/★/-` {{환자명}}(차트#{{ch}}): "{{파일}}의 {{현재금액}}을 {{목표금액}}으로 수정" 또는 "{{파일}}에 {{금액}} {{결제수단}} 결제건 추가" / "{{파일}}의 {{환자/금액}} 행 삭제"
    2. …
  - **상쇄검증**: 위 수정합산 = ±?원 → 채널차이값과 일치 ✓ (또는 부족분 ?원 → 추가후보 제시)

### 결론 (1~2문장)
어느 파일의 어느 환자(차트#·금액)를 어떻게 수정하면 전 채널 합계가 맞는지 — 환자명·차트#·금액 모두 인용."""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI 엑셀 (4시트: P1차이 / P2-한솔 / P2-일마 / 합계)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_3way_table(hansol, daily, patient, p1_full):
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

    p_pivot = {}
    if not patient.empty and "분류" in patient.columns:
        for _, r in patient.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            cat = str(r.get("분류", ""))
            amt = _safe_int(r.get("금액", 0))
            d = p_pivot.setdefault(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
            if cat in d:
                d[cat] += amt

    d_pivot = {}
    if not daily.empty:
        for _, r in daily.iterrows():
            ch = clean_no(r.get("차트번호", ""))
            if not ch:
                continue
            d = d_pivot.setdefault(ch, {"카드": 0, "현금": 0, "이체": 0, "플랫폼": 0})
            d["카드"] += _safe_int(r.get("카드", 0))
            d["현금"] += _safe_int(r.get("현금", 0))
            d["이체"] += _safe_int(r.get("이체", 0))
            d["플랫폼"] += _safe_int(r.get("플랫폼합", 0))

    h_card_by_ch = {}
    if p1_full is not None and not p1_full.empty:
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
        h_amt, h_cnt = h_card_by_ch.get(ch, (0, 0))
        row = {
            "차트번호": ch,
            "이름": name_map.get(ch, ""),
            "차트카드": p["카드"], "차트현금": p["현금"], "차트이체": p["이체"], "차트플랫폼": p["플랫폼"],
            "일마카드": d["카드"], "일마현금": d["현금"], "일마이체": d["이체"], "일마플랫폼": d["플랫폼"],
        }
        if has_hansol:
            row["한솔카드"] = h_amt
            row["한솔건수"] = h_cnt
            row["한-일카드차"] = h_amt - d["카드"]
        row["일-차카드차"] = d["카드"] - p["카드"]
        row["일-차현금차"] = d["현금"] - p["현금"]
        row["일-차이체차"] = d["이체"] - p["이체"]
        row["일-차플랫폼차"] = d["플랫폼"] - p["플랫폼"]
        rows.append(row)
    return pd.DataFrame(rows)


def build_ai_excel(p1_diff, h_um, d_um, totals, channel_df=None, suspects_by_channel=None,
                   hansol=None, daily=None, patient=None, p1_full=None):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
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
                                  daily, patient, p1_full)
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

st.title("📊 BW 컨설팅 AI 정산 분석 시스템")
st.caption("★ 결제채널별 파일 합계 차이를 먼저 산출 → 차이를 설명할 후보 거래 추적 | 일마+차트 2개 또는 한솔+일마+차트 3개 분석 가능")

if "done" not in st.session_state:
    c1, c2, c3 = st.columns(3)
    with c1:
        f_h = st.file_uploader("한솔페이 (선택)", type=["csv", "xlsx", "xls", "xlsb"], key="h")
        h_pw = st.text_input("비밀번호(선택)", type="password", key="h_pw")
    with c2:
        f_d = st.file_uploader("일일마감", type=["csv", "xlsx", "xls", "xlsb"], key="d")
        d_pw = st.text_input("비밀번호(선택)", type="password", key="d_pw")
    with c3:
        f_p = st.file_uploader("차트마감", type=["csv", "xlsx", "xls", "xlsb"], key="p")
        p_pw = st.text_input("비밀번호(선택)", type="password", key="p_pw")

    if f_d and f_p:
        if st.button("🚀 분석 시작", type="primary", width="stretch"):
            with st.spinner("분석 중..."):
                try:
                    daily, daily_refund = parse_daily(load_file(f_d, password=d_pw))
                    patient = parse_patient(load_file(f_p, password=p_pw))
                    hansol = parse_hansol(load_file(f_h, password=h_pw)) if f_h else pd.DataFrame()
                except Exception as e:
                    st.error(f"파일 로딩 실패: {e}")
                    st.stop()
                if daily.empty:
                    st.error("일일마감 파싱 실패")
                    st.stop()

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
                    ch: find_channel_suspects(ch, hansol, daily, patient, totals=totals, top_n=15)
                    for ch in ["카드", "현금+이체", "플랫폼"]
                }

                ss = st.session_state
                ss["done"] = True
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
        st.info("일일마감·차트마감 파일을 업로드하세요 (한솔페이는 선택).")

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
    nonzero_count = sum(
        1 for _, r in channel_df.iterrows()
        for col in diff_cols
        if _nonzero(r[col])
    )
    total_abs_gap = sum(
        abs(int(r[col])) for _, r in channel_df.iterrows()
        for col in diff_cols
        if _nonzero(r[col])
    )

    if has_hansol:
        n_ok = len(hansol[hansol["tx_status"] == "정상"])
        n_m = len(matched_h)
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("한솔 정상", n_ok)
        k2.metric("자동매칭", n_m, f"{n_m/n_ok*100:.0f}%" if n_ok else "0%")
        k3.metric("🔴 채널 차이 합계", f"{total_abs_gap:,}원", delta_color="inverse")
        k4.metric("⚠️ 차이있는 채널-쌍", nonzero_count, delta_color="inverse")
    else:
        k1, k2 = st.columns(2)
        k1.metric("🔴 채널 차이 합계 (일마↔차트)", f"{total_abs_gap:,}원", delta_color="inverse")
        k2.metric("⚠️ 차이있는 채널-쌍", nonzero_count, delta_color="inverse")

    title_suffix = "3개 파일" if has_hansol else "일마↔차트 2개 파일"
    st.markdown(f"### ★ 교차분석 합계 ({title_suffix})")
    caption_suffix = "한솔·일마·차트" if has_hansol else "일마·차트"
    st.caption(
        f"각 채널의 {caption_suffix} 합계를 비교 — **차트 기준으로 일마·한솔이 같으면 🟦 파랑, 다르면 🟥 빨강**"
    )

    def _fmt_amt(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "-"
        try:
            return f"{int(v):,}"
        except Exception:
            return "-"

    def _style_chart_compare(row):
        styles = [""] * len(row)
        cols = row.index.tolist()
        chart_v = row.get("차트")
        try:
            chart_int = int(chart_v) if chart_v is not None and not (isinstance(chart_v, float) and pd.isna(chart_v)) else None
        except Exception:
            chart_int = None
        for i, c in enumerate(cols):
            if c not in ("일마", "한솔"):
                continue
            v = row[c]
            if v is None or (isinstance(v, float) and pd.isna(v)) or chart_int is None:
                continue
            try:
                same = int(v) == chart_int
            except Exception:
                continue
            if same:
                styles[i] = "background-color: #cfe7ff; color: #003366; font-weight: 700"
            else:
                styles[i] = "background-color: #ffcccc; color: #8b0000; font-weight: 700"
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
    tab1, tab2, tab3 = st.tabs([
        "🎯 차이 원인 추적 (★메인)",
        "🔬 1:1 매칭 상세 (보조)",
        "🤖 AI 진단",
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
                "🟡 **차트↔일마 분류차이** = 차트번호별로 결제수단 불일치 환자"
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
            p1_full, h_um, d_um, suspects_by_channel, totals=totals,
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
                hansol=hansol, daily=daily, patient=patient, p1_full=p1_full,
            )
            st.download_button(
                "통합 엑셀 다운로드 (6시트)",
                data=excel,
                file_name=f"정산차이_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.caption("시트: 0_채널대사(★) / 0_3way통합(★) / 0_의심후보 / 1_차트번호별차이 / 2_한솔미매칭 / 3_일마미매칭")
