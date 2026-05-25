"""
병원 정산 3-Way 차이 추적기 (채널 합계 대사 중심)

목표 (재정의):
  ★ 결제채널별(카드/현금+이체/플랫폼) 3개 파일 합계 차이 산출
  ★ 차이금액을 설명할 후보 환자·거래 추적 (잘못 기입/누락 즉시 수정)

원칙:
  - 채널 합계 차이 → 의심 후보(소수) → AI 한 줄 진단 흐름
  - 1:1 매칭은 후보 식별 도구로만 사용 (메인 산출물 아님)
  - AI 입력 ≤ ~600자 / 출력 ≤ 800토큰 / 무료 한도(2.5-flash-lite 기준) 내
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
    h_ok = hansol[hansol["tx_status"] == "정상"]
    h_cancel = hansol[hansol["tx_status"] == "취소"]
    h_card = int(h_ok[~h_ok["is_현금"]]["금액"].sum()) - int(h_cancel[~h_cancel["is_현금"]]["금액"].sum())
    h_cash = int(h_ok[h_ok["is_현금"]]["금액"].sum()) - int(h_cancel[h_cancel["is_현금"]]["금액"].sum())

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
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 채널 합계 대사 (메인 산출물)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_channel_recon(totals):
    """3개 파일 결제채널별 합계 + 차이값 산출.

    채널 정의:
      - 카드: 한솔(카드 승인) / 일마(카드) / 차트(분류=카드)
      - 현금+이체: 한솔(현금영수증) / 일마(현금+이체) / 차트(현금+이체)
        ※ 한솔은 현금영수증만 처리하므로 일마/차트 합과 동일하지 않을 수 있음
      - 플랫폼: 한솔(비경유) / 일마(플랫폼 합) / 차트(분류=플랫폼)
    """
    rows = [
        {
            "채널": "카드",
            "한솔": totals["h_card"],
            "일마": totals["d_card"],
            "차트": totals["p_card"],
            "한솔-차트": totals["h_card"] - totals["p_card"],
            "한솔-일마": totals["h_card"] - totals["d_card"],
            "일마-차트": totals["d_card"] - totals["p_card"],
        },
        {
            "채널": "현금+이체",
            "한솔": totals["h_cash"],
            "일마": totals["d_cashxfer"],
            "차트": totals["p_cashxfer"],
            "한솔-차트": totals["h_cash"] - totals["p_cashxfer"],
            "한솔-일마": totals["h_cash"] - totals["d_cashxfer"],
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
    """채널 차이를 설명할 후보 거래 추출 (multiset diff 기반).

    원리:
      1. 한솔/일마 카드 금액 multiset에서 짝지어지지 않는 금액 추출
         → 이 후보들의 합 = 실제 채널 합계 차이와 정확히 일치
      2. 정렬은 채널 차이값 근접도 우선 — 작은 차이는 작은 후보를 우선해야 잡힘
         (예: gap=-27,400원이면 27,600원이 714,000원보다 더 유력한 후보)
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

    # 승인번호(전체/끝8자리) → (차트번호, 이름) 조회맵 — 차트의 승인번호목록 기반
    appr_to_patient: dict = {}
    if not patient.empty and "승인번호목록" in patient.columns:
        for _, row in patient.iterrows():
            appr_list = row.get("승인번호목록", []) or []
            nm = str(row.get("이름", "")).strip()
            ch_no = str(row.get("차트번호", "")).strip()
            if not (nm and ch_no):
                continue
            for a in appr_list:
                a_str = str(a).strip()
                if not a_str:
                    continue
                appr_to_patient[a_str] = (ch_no, nm)
                # 한솔 승인번호와 자릿수 차이 대비: 끝 8자리 매칭도 등록
                if len(a_str) >= 8:
                    appr_to_patient[a_str[-8:]] = (ch_no, nm)

    def _lookup_by_appr(appr_no: str):
        """한솔 승인번호로 차트 환자 조회 (전체/끝8자리 비교)."""
        a = str(appr_no).strip()
        if not a:
            return None
        if a in appr_to_patient:
            return appr_to_patient[a]
        if len(a) >= 8 and a[-8:] in appr_to_patient:
            return appr_to_patient[a[-8:]]
        return None

    # 일마 환자별 카드 합계 (★★ 동일환자 확정 검증용)
    d_by_name_card: dict = {}
    if not daily.empty and "성명" in daily.columns and "카드" in daily.columns:
        for nm, grp in daily.groupby("성명"):
            total = int(grp["카드"].sum())
            if total > 0 and str(nm).strip() and str(nm).strip() != "nan":
                d_by_name_card[str(nm).strip()] = total

    if channel == "카드":
        h_ok = hansol[hansol["tx_status"] == "정상"] if "tx_status" in hansol.columns else hansol
        h_card = h_ok[~h_ok["is_현금"]] if "is_현금" in h_ok.columns else h_ok
        h_amts = [int(x) for x in h_card["금액"].tolist()] if not h_card.empty else []
        d_amts = [int(x) for x in daily.loc[daily["카드"] > 0, "카드"].tolist()] if not daily.empty else []

        ch_h, ch_d = Counter(h_amts), Counter(d_amts)
        only_h = ch_h - ch_d
        only_d = ch_d - ch_h

        # 채널 차이값 기준 (한솔-일마 또는 한솔-차트 중 0이 아닌 값)
        gap = 0
        if totals:
            gap = totals.get("h_card", 0) - totals.get("d_card", 0)
            if gap == 0:
                gap = totals.get("h_card", 0) - totals.get("p_card", 0)

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ★★ 동일환자 확정 (최우선): 한솔 거래의 승인번호가 차트의 특정
        # 환자에 속하고, 그 환자가 일마에는 다른 금액으로 기재된 경우.
        # 카운터 차분으로 미매칭에서 빠진 거래도 잡기 위해 only_h에 한정하지 않고
        # 동일 금액의 모든 한솔 거래를 승인번호로 검증한다.
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        confirmed_suspects = []
        seen_confirmed = set()
        h_remaining = h_card.copy()
        # only_h 금액과 더불어, gap에 근접한 모든 한솔 카드 거래도 후보로
        amts_to_probe = set(int(a) for a in only_h.keys())
        tol_gap = max(1000, abs(gap) * 0.05) if gap else 0
        for hr_amt in h_card["금액"].unique():
            if gap != 0 and abs(abs(int(hr_amt)) - abs(gap)) <= max(50000, abs(gap) * 2):
                amts_to_probe.add(int(hr_amt))
        for amt in amts_to_probe:
            rows = h_card[h_card["금액"] == amt]
            for _, r in rows.iterrows():
                appr = str(r.get("승인번호", "")).strip()
                hit = _lookup_by_appr(appr)
                if not hit:
                    continue
                ch_no, nm = hit
                d_total = d_by_name_card.get(nm, 0)
                diff = d_total - int(amt)
                if d_total > 0 and diff != 0:
                    key = (ch_no, int(amt), d_total)
                    if key in seen_confirmed:
                        continue
                    seen_confirmed.add(key)
                    cn = str(r.get("카드번호", ""))
                    cn_tail = cn[-5:] if cn and cn != "nan" else ""
                    near = (gap != 0 and abs(abs(diff) - abs(gap)) <= tol_gap)
                    tag = "★★ 동일환자 확정(gap일치)" if near else "★★ 동일환자 확정"
                    confirmed_suspects.append({
                        "출처": tag,
                        "환자": f"{nm}({ch_no})",
                        "금액": diff,
                        "단서": f"한솔 {int(amt):,}원(승인{appr} 말미{cn_tail}) vs 일마 {nm} 카드합 {d_total:,}원 → 차이 {diff:+,}",
                        "조치": "한솔=부분취소/차트=오기재 → 동일환자의 두 금액 즉시 확인",
                        "_near": 1 if near else 0,
                    })

        # gap일치 → 그 외 순으로 정렬
        confirmed_suspects.sort(key=lambda x: -x.pop("_near"))
        suspects.extend(confirmed_suspects)

        # 한솔에만 (일마에 누락 의심) — 차이값 근접도 우선 정렬
        for amt, cnt in sorted(only_h.items(), key=lambda x: _rank_key(x[0], gap))[:top_n]:
            rows = h_remaining[h_remaining["금액"] == amt].head(cnt)
            for _, r in rows.iterrows():
                cn = str(r.get("카드번호", ""))
                cn_tail = cn[-5:] if cn and cn != "nan" else ""
                appr = str(r.get("승인번호", "")).strip()
                near = abs(abs(amt) - abs(gap)) <= max(1000, abs(gap) * 0.05) if gap else False
                # 승인번호로 차트 환자 확정 시도
                hit = _lookup_by_appr(appr)
                if hit:
                    ch_no, nm = hit
                    patient_field = f"{nm}({ch_no})✓"
                else:
                    name_candidates = d_amt_to_names.get(int(amt), [])
                    patient_field = "·".join(name_candidates[:2]) + ("?" if name_candidates else "")
                tag = "한솔에만 존재★" if near else "한솔에만 존재"
                suspects.append({
                    "출처": tag,
                    "환자": patient_field,
                    "금액": int(amt),
                    "단서": f"{r.get('시간표시','')} 말미{cn_tail} 승인{appr} {str(r.get('카드사',''))[:6]}",
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

        # ★ 차이값 정확매칭 페어 (h_amt - d_amt = gap) — 환자 확정 시도
        if gap != 0 and only_h and only_d:
            for h_amt in list(only_h.keys())[:20]:
                for d_amt in list(only_d.keys())[:20]:
                    if (h_amt - d_amt) == gap:
                        # 한솔 h_amt 거래의 승인번호로 환자 확정 시도
                        h_rows = h_card[h_card["금액"] == h_amt]
                        d_rows = daily[daily["카드"] == d_amt]
                        d_names = [str(x).strip() for x in d_rows.get("성명", []) if str(x).strip() and str(x).strip() != "nan"]
                        # 한솔 승인번호 ↔ 차트 환자 매칭하여 일마 환자와 일치하는지 확인
                        confirmed_name = None
                        confirmed_chart = None
                        confirmed_clue_extra = ""
                        for _, hr in h_rows.iterrows():
                            appr = str(hr.get("승인번호", "")).strip()
                            hit = _lookup_by_appr(appr)
                            if hit:
                                ch_no, nm = hit
                                if nm in d_names:
                                    confirmed_name = nm
                                    confirmed_chart = ch_no
                                    cn = str(hr.get("카드번호", ""))
                                    cn_tail = cn[-5:] if cn and cn != "nan" else ""
                                    confirmed_clue_extra = f" / 승인{appr} 말미{cn_tail}"
                                    break
                        if confirmed_name:
                            # 이미 ★★에 추가된 경우 중복 방지
                            already = any(
                                s["출처"].startswith("★★") and confirmed_chart and confirmed_chart in str(s.get("환자", ""))
                                for s in suspects
                            )
                            if not already:
                                suspects.insert(0, {
                                    "출처": "★★ 동일환자 확정 페어",
                                    "환자": f"{confirmed_name}({confirmed_chart})",
                                    "금액": gap,
                                    "단서": f"한솔 {h_amt:,} 와 일마 {d_amt:,} 차이가 정확히 {gap:+,}{confirmed_clue_extra}",
                                    "조치": "동일환자 — 한솔 부분취소 또는 차트 오기재 확인",
                                })
                        else:
                            d_names_str = "·".join(d_names[:2])
                            suspects.append({
                                "출처": "★ 차이값 정확매칭 페어",
                                "환자": d_names_str + ("?" if d_names_str else ""),
                                "금액": gap,
                                "단서": f"한솔 {h_amt:,} 와 일마 {d_amt:,} 의 차이가 정확히 {gap:+,}",
                                "조치": "두 거래가 같은 환자(부분취소·오기재 의심) — 즉시 확인",
                            })

        # 차트(환자집계) vs 일마 카드 분류 차이 (참고)
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
# AI 분석 텍스트 (채널 중심 ~600자 / ~250토큰)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_ai_text(channel_df, suspects_by_channel, max_chars=1200):
    """채널 합계 대사 + 채널별 의심후보 TOP만 전달 (~500자 목표).
    구분자는 파이프(|) 사용 — 숫자 천단위 콤마와 충돌 방지."""
    def _f(v):
        if v is None:
            return "-"
        try:
            if pd.isna(v):
                return "-"
        except Exception:
            pass
        return str(int(v))  # 토큰 절약: 콤마 제거

    L = []
    L.append("채널|한솔|일마|차트|한솔-차트|한솔-일마|일마-차트")
    for _, r in channel_df.iterrows():
        L.append(f"{r['채널']}|{_f(r['한솔'])}|{_f(r['일마'])}|{_f(r['차트'])}|{_f(r['한솔-차트'])}|{_f(r['한솔-일마'])}|{_f(r['일마-차트'])}")

    nonzero_channels = []
    for _, r in channel_df.iterrows():
        diffs = []
        for col in ["한솔-차트", "한솔-일마", "일마-차트"]:
            v = r[col]
            if v is None:
                continue
            try:
                if pd.isna(v):
                    continue
            except Exception:
                pass
            if int(v) != 0:
                diffs.append(f"{col}={int(v):+d}")
        if diffs:
            nonzero_channels.append((r["채널"], diffs))

    if not nonzero_channels:
        L.append("\n[결과] 모든채널 일치")
    else:
        for ch, diffs in nonzero_channels:
            L.append(f"\n[{ch}] {' / '.join(diffs)}")
            sus = suspects_by_channel.get(ch, [])
            if sus:
                # ★★ 우선 → ★ 다음 → 나머지 순으로 정렬하여 상위 6개 송신
                def _prio(s):
                    src = str(s.get("출처", ""))
                    if src.startswith("★★"):
                        return 0
                    if src.startswith("★") or "★" in src:
                        return 1
                    return 2
                sus_sorted = sorted(sus, key=_prio)
                L.append("출처|환자|금액|단서")
                for s in sus_sorted[:10]:
                    clue = str(s.get("단서", ""))[:70].replace("|", " ")
                    nm = str(s.get("환자", s.get("환자(추정)", ""))).replace("|", " ")[:14]
                    L.append(f"{s['출처']}|{nm}|{int(s['금액']):+d}|{clue}")

    text = "\n".join(L)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n(축소)"
    return text


AI_SYSTEM = (
    "병원 정산 분석관. 한솔(PG)·일마(프론트)·차트(EMR) 채널합계 차이의 원인 거래를 짧게 진단. "
    "출처 태그 우선순위는 절대규칙: ★★ > ★ > 일반. "
    "★★(동일환자 확정)이 1건이라도 있으면 그것이 최종답이며, 반드시 TOP1에 환자명·차트번호와 함께 명시."
)

AI_USER = """3개 파일 채널 합계 대사 + 채널별 의심후보 표:

{data}

[출처 태그 의미 — 우선순위 순]
- ★★ 동일환자 확정 / ★★ 동일환자 확정 페어 / ★★ 동일환자 확정(gap일치):
    한솔 거래의 승인번호가 차트의 특정 환자와 일치하며, 일마에 동일 환자가 다른 금액으로 기재됨.
    부분취소·오기재의 결정적 증거. 환자명·차트번호 확정. → 무조건 TOP1에 노출, "환자명(차트번호)" 명시.
- ★ 차이값 정확매칭 페어:
    한솔의 X원과 일마의 Y원의 차이가 채널 gap과 정확히 일치. 환자명은 추정. ★★ 없을 때 TOP1 후보.
- 한솔에만 존재★ / 일마에만 존재★:
    채널 gap과 금액 근접도가 높은 미매칭 거래. 환자 뒤 "✓"=승인번호로 확정, "?"=금액만으로 추정.
- 그 외(★ 없음): 참고용. 환자명이 비어있거나 ?면 신뢰도 낮음.

[추론 규칙]
1. ★★ 항목이 보이면 그것이 정답이다. 환자명·차트번호·승인번호·말미를 그대로 결론에 옮긴다.
2. ★★이 없고 ★(차이값 정확매칭 페어)가 있다면 그것을 TOP1으로 둔다.
3. 의심후보 TOP3은 위 우선순위로만 선정한다. 환자명이 비어있는 항목은 가능하면 제외.
4. "환자(추정)"이 여럿(A·B 형태)이면 모두 표기하되, 그 위의 ★★ 환자가 있으면 ★★ 환자가 우선.

800토큰 이내, 다음 형식만 출력:

### 채널별 차이 진단
각 차이가 0이 아닌 채널마다 ↓
- **{{채널}}**: 한솔-차트=?원 / 한솔-일마=?원 / 일마-차트=?원
  - 가장 유력한 원인: ★★/★ 항목의 환자명·차트번호·승인번호를 인용하여 한 문장.
  - 의심 후보 TOP3 (출처/환자/금액/조치) — 반드시 ★★→★→일반 순.

### 결론 (1문장)
"<환자명>(<차트번호>)의 한솔 X원 vs 일마/차트 Y원을 확인·수정" 형식으로 단일 환자 지목."""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI 엑셀 (4시트: P1차이 / P2-한솔 / P2-일마 / 합계)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_ai_excel(p1_diff, h_um, d_um, totals, channel_df=None, suspects_by_channel=None):
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
                    max_output_tokens=800,
                    temperature=0.2,
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

st.title("📊 병원 정산 3-Way 차이 추적기")
st.caption("★ 결제채널별 3개 파일 합계 차이를 먼저 산출 → 차이를 설명할 후보 거래 추적")

if "done" not in st.session_state:
    c1, c2, c3 = st.columns(3)
    with c1:
        f_h = st.file_uploader("한솔페이", type=["csv", "xlsx", "xls", "xlsb"], key="h")
        h_pw = st.text_input("비밀번호(선택)", type="password", key="h_pw")
    with c2:
        f_d = st.file_uploader("일일마감", type=["csv", "xlsx", "xls", "xlsb"], key="d")
        d_pw = st.text_input("비밀번호(선택)", type="password", key="d_pw")
    with c3:
        f_p = st.file_uploader("차트마감", type=["csv", "xlsx", "xls", "xlsb"], key="p")
        p_pw = st.text_input("비밀번호(선택)", type="password", key="p_pw")

    if f_h and f_d and f_p:
        if st.button("🚀 분석 시작", type="primary", width="stretch"):
            with st.spinner("분석 중..."):
                try:
                    hansol = parse_hansol(load_file(f_h, password=h_pw))
                    daily, daily_refund = parse_daily(load_file(f_d, password=d_pw))
                    patient = parse_patient(load_file(f_p, password=p_pw))
                except Exception as e:
                    st.error(f"파일 로딩 실패: {e}")
                    st.stop()
                if daily.empty:
                    st.error("일일마감 파싱 실패")
                    st.stop()

                match_df, matched_h, matched_dc = run_matching(hansol, daily, patient)
                p1_full, p1_diff = compute_p1(match_df, patient, daily)
                h_um, d_um = compute_p2(hansol, daily, matched_h, matched_dc)
                totals = compute_totals(hansol, daily, daily_refund, patient)
                channel_df = compute_channel_recon(totals)
                suspects_by_channel = {
                    ch: find_channel_suspects(ch, hansol, daily, patient, totals=totals)
                    for ch in ["카드", "현금+이체", "플랫폼"]
                }

                ss = st.session_state
                ss["done"] = True
                ss["hansol"], ss["daily"], ss["patient"] = hansol, daily, patient
                ss["match_df"], ss["matched_h"], ss["matched_dc"] = match_df, matched_h, matched_dc
                ss["p1_full"], ss["p1_diff"] = p1_full, p1_diff
                ss["h_um"], ss["d_um"] = h_um, d_um
                ss["totals"] = totals
                ss["channel_df"] = channel_df
                ss["suspects_by_channel"] = suspects_by_channel
            st.rerun()
    else:
        st.info("3개 파일을 모두 업로드하세요.")

else:
    ss = st.session_state
    hansol, daily, patient = ss["hansol"], ss["daily"], ss["patient"]
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

    nonzero_count = sum(
        1 for _, r in channel_df.iterrows()
        for col in ["한솔-차트", "한솔-일마", "일마-차트"]
        if _nonzero(r[col])
    )
    total_abs_gap = sum(
        abs(int(r[col])) for _, r in channel_df.iterrows()
        for col in ["한솔-차트", "한솔-일마", "일마-차트"]
        if _nonzero(r[col])
    )

    n_ok = len(hansol[hansol["tx_status"] == "정상"])
    n_m = len(matched_h)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("한솔 정상", n_ok)
    k2.metric("자동매칭", n_m, f"{n_m/n_ok*100:.0f}%" if n_ok else "0%")
    k3.metric("🔴 채널 차이 합계", f"{total_abs_gap:,}원", delta_color="inverse")
    k4.metric("⚠️ 차이있는 채널-쌍", nonzero_count, delta_color="inverse")

    st.markdown("### ★ 채널별 합계 대사 (3개 파일)")
    st.caption("각 채널의 한솔·일마·차트 합계를 비교 — **0이 아닌 차이값이 있으면 그 채널을 우선 추적**")
    disp_df = channel_df.copy()
    for c in disp_df.columns:
        if c == "채널":
            continue
        disp_df[c] = disp_df[c].apply(lambda v: "-" if v is None or (isinstance(v, float) and pd.isna(v)) else f"{int(v):,}")
    st.dataframe(disp_df, width="stretch", hide_index=True)

    if nonzero_count == 0:
        st.success("✅ 모든 채널 합계 일치 — 추가 분석 불필요")

    # 탭 3개 - 메인 탭은 의심 후보 추적
    tab1, tab2, tab3 = st.tabs([
        "🎯 차이 원인 추적 (★메인)",
        "🔬 1:1 매칭 상세 (보조)",
        "🤖 AI 진단",
    ])

    with tab1:
        st.markdown("**채널별 차이가 0이 아닌 항목** → 차이를 설명할 후보 거래를 확인하세요.")
        any_diff = False
        for _, r in channel_df.iterrows():
            ch = r["채널"]
            diffs = []
            for col in ["한솔-차트", "한솔-일마", "일마-차트"]:
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
                # ★★ → ★ → 그 외 순으로 정렬해서 사용자가 한눈에 정답 후보부터 보도록
                def _prio_row(s):
                    src = str(s.get("출처", ""))
                    if src.startswith("★★"):
                        return 0
                    if "★" in src:
                        return 1
                    return 2
                sus_sorted = sorted(sus, key=_prio_row)
                sus_df = pd.DataFrame(sus_sorted)
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

    with tab3:
        ai_text = build_ai_text(channel_df, suspects_by_channel)
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

        est_in = int(len(ai_text) / 2.5) + 150
        est_total = est_in + 800
        col = "🟢" if est_total < 3000 else "🟡" if est_total < 5000 else "🔴"
        rpd = GEMINI_MODELS[model_choice]["rpd"]
        rpm = GEMINI_MODELS[model_choice]["rpm"]
        st.caption(f"{col} 데이터 {len(ai_text):,}자 / 토큰 ~{est_in}입력+~800출력 = ~{est_total}  | 선택모델 무료한도 RPM{rpm}·RPD{rpd} (한도 시 다른 무료 모델로 자동 폴백)")

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
            excel = build_ai_excel(p1_diff, h_um, d_um, totals, channel_df, suspects_by_channel)
            st.download_button(
                "통합 엑셀 다운로드 (5시트)",
                data=excel,
                file_name=f"정산차이_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.caption("시트: 0_채널대사(★) / 0_의심후보(★) / 1_차트번호별차이 / 2_한솔미매칭 / 3_일마미매칭")
