"""
병원 정산 3-Way 대사 시스템 v2.1
한솔페이 × 일일마감 × 차트마감(환자별집계) 자동 매칭 + 의심건 즉시 탐지

v1 대비 주요 개선:
  1. parse_hansol_time 이중파싱 버그 수정
  2. 승인거절/취소 자동 분류 (한솔페이)
  3. 분할결제 매칭 (한솔 2~3건 합 = 일마 1건, 시간근접 ≤10분)
  4. 현금영수증·이체 매칭 (한솔 현금 ↔ 일마 현금/이체)
  5. 시간-순서 상관 매칭 (동일금액 다건 → 보간)
  6. 일자 합계 대사 선행 (전체 균형부터 확인)
  7. 환자별집계 결제수단 정밀분류 (카드/현금영수증/통장입금/기타 구분)
  8. 세무위험 자동 탐지 (과소·과다 신고 + 차트번호 불일치)
  9. 본부금(진료비) 차트 금액 통합 – 6,900원 등 본부금 수납 반영
  10. 카드사 정보 매칭 – 결제수단/매입사 카드사명으로 정밀 매칭
  11. 본부금 기반 분할결제 탐지 – 본부금 힌트로 2건 분할 정밀 매칭
"""

import importlib
import io
import re
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


def _fmt_money(v):
    try:
        return f"{int(v):,}원"
    except Exception:
        return "0원"


def _find_col(df, include_keywords, exclude_keywords=None):
    exclude_keywords = exclude_keywords or []
    for c in df.columns:
        cs = str(c)
        if all(k in cs for k in include_keywords) and not any(k in cs for k in exclude_keywords):
            return c
    return None


def build_messenger_summary(daily, hansol, tots):
    total_patients = int(len(daily))

    type_col = next((c for c in daily.columns if daily[c].astype(str).str.contains("신환|구환", na=False).any()), None)
    new_df = daily[daily[type_col].astype(str).str.contains("신환", na=False)] if type_col else daily.iloc[0:0]
    old_df = daily[daily[type_col].astype(str).str.contains("구환", na=False)] if type_col else daily.iloc[0:0]

    new_booked = int(len(new_df))
    old_booked = int(len(old_df))
    new_paid = int((new_df["총액"] > 0).sum()) if not new_df.empty else 0
    old_paid = int((old_df["총액"] > 0).sum()) if not old_df.empty else 0
    new_paid_amt = int(new_df["총액"].sum()) if not new_df.empty else 0
    old_paid_amt = int(old_df["총액"].sum()) if not old_df.empty else 0

    cancel_col = next((c for c in daily.columns if daily[c].astype(str).str.contains("취소|부도", na=False).any()), None)
    cancel_cnt = int(daily[cancel_col].astype(str).str.contains("취소|부도", na=False).sum()) if cancel_col else 0

    if cancel_cnt == 0:
        rej = hansol[hansol["tx_status"] == "승인거절"]
        can = hansol[hansol["tx_status"] == "취소"]
        cancel_cnt = int(len(rej) + len(can))

    today_amt = int(tots.get("p_tot", 0))
    date_line = pd.Timestamp.now().strftime("%Y년 %-m월 %-d일 %A")

    return "\n".join([
        f"{date_line}",
        "",
        "VS라인클리닉 인천점",
        "",
        f"* 총 내원 환자 : {total_patients}명",
        "",
        f"* 신환예약 : {new_booked}명 수납 : {new_paid}명 {_fmt_money(new_paid_amt)}",
        "",
        f"* 구환예약 : {old_booked}명 수납 : {old_paid}명 {_fmt_money(old_paid_amt)}",
        "",
        f"* 총 취소+부도 환자 : {cancel_cnt}명",
        "",
        f"Today : {_fmt_money(today_amt)}",
        "",
        f"- 이체 : {_fmt_money(tots.get('p_xfer', 0))}",
        "",
        f"- 현금 : {_fmt_money(tots.get('p_cash', 0))}",
        "",
        f"- 카드 : {_fmt_money(tots.get('p_card', 0))}",
        "",
        f"- 여신티켓 : {_fmt_money(int(daily.get('여신티켓', pd.Series(dtype=int)).sum()))}",
        "",
        f"- 강남언니 : {_fmt_money(int(daily.get('강남언니', pd.Series(dtype=int)).sum()))}",
        "",
        f"- 나만의닥터 : {_fmt_money(int(daily.get('나만의닥터', pd.Series(dtype=int)).sum()))}",
        "",
        f"- 제로페이 : {_fmt_money(int(daily.get('제로페이', pd.Series(dtype=int)).sum()))}",
        "",
        f"- 지역화폐 : {_fmt_money(int(daily.get('기타지역화폐', pd.Series(dtype=int)).sum()))}",
        "",
        f"- 환불+취소 : {_fmt_money(int(hansol[hansol['tx_status'].isin(['취소'])]['금액'].sum()))}",
        "",
        "----------------------",
        "",
        f"Total : {_fmt_money(today_amt)}",
    ])


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


def load_file(f, password=None, default_password="vsline99!!"):
    if f.name.lower().endswith(".csv"):
        try:
            return pd.read_csv(f, encoding="utf-8")
        except UnicodeDecodeError:
            f.seek(0)
            return pd.read_csv(f, encoding="cp949")

    raw = f.read()
    f.seek(0)

    attempts = [password.strip()] if isinstance(password, str) and password.strip() else [None, default_password]
    last_error = None

    for pw in attempts:
        try:
            if pw is None:
                return pd.read_excel(io.BytesIO(raw), header=None)

            if importlib.util.find_spec("msoffcrypto") is None:
                raise ValueError("암호화된 엑셀 처리를 위해 msoffcrypto-tool 설치가 필요합니다.")
            msoffcrypto = importlib.import_module("msoffcrypto")
            office = msoffcrypto.OfficeFile(io.BytesIO(raw))
            office.load_key(password=pw)
            decrypted = io.BytesIO()
            office.decrypt(decrypted)
            decrypted.seek(0)
            return pd.read_excel(decrypted, header=None)
        except Exception as e:
            last_error = e

    raise ValueError(f"엑셀 파일을 열 수 없습니다. 비밀번호를 확인해주세요. ({last_error})")


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

@@ -229,146 +335,188 @@ def parse_patient(raw):
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
    df["분류"] = "기타"
    df.loc[pay.str.startswith("카드", na=False), "분류"] = "카드"
    df.loc[pay.str.contains("현금영수증", na=False), "분류"] = "현금"
    df.loc[(pay == "통장입금") | pay.str.contains("이체", na=False), "분류"] = "이체"
    df.loc[pay.str.startswith("기타", na=False), "분류"] = "플랫폼"

    # 카드사 추출
    df["카드사"] = ""
    card_mask = df["분류"] == "카드"
    if card_mask.any():
        df.loc[card_mask, "카드사"] = pay[card_mask].apply(_extract_card_company)

    # 승인번호 추출
    df["승인번호목록"] = [[] for _ in range(len(df))]
    mcol = next((c for c in ["결제메모", "승인번호", "메모"] if c in df.columns), None)
    if mcol:
        memo = _pick_first_series(df, mcol)
        df["승인번호목록"] = memo.apply(lambda x: re.findall(r"\d{6,}", str(x)) if pd.notna(x) else [])

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
                한솔_시간=hr.get("시간표시", ""), 한솔_금액=int(hr["금액"]),
                한솔_카드번호=str(hr.get("카드번호", ""))[:12],
                한솔_카드사=str(hr.get("카드사", "")),
                한솔_유형="현금" if hr["is_현금"] else "카드",
                일마_순서=d_row["내원순서"], 일마_성명=d_row["성명"],
                일마_차트=d_row["차트번호"], 일마_카드=int(d_row["카드"]),
            ))
            matched_h.add(hi)
        matched_dc.add(d_row["d_idx"])

    # 환자별 인덱스/승인번호 인덱스 맵 (차트번호+이름을 함께 활용)
    patient_by_chart, patient_by_name = {}, {}
    approval_to_patient_idxs = {}
    for p_idx, pr in patient.iterrows():
        ch = clean_no(pr.get("차트번호", ""))
        nm = clean_name(pr.get("이름", ""))
        if ch:
            patient_by_chart.setdefault(ch, set()).add(p_idx)
        if nm:
            patient_by_name.setdefault(nm, set()).add(p_idx)
        for a in pr.get("승인번호목록", []):
            aa = clean_no(a)
            if aa:
                approval_to_patient_idxs.setdefault(aa, set()).add(p_idx)

    def _candidate_patient_idxs(dr):
        cand = set()
        ch = clean_no(dr.get("차트번호", ""))
        nm = clean_name(dr.get("성명", ""))
        if ch in patient_by_chart:
            cand |= patient_by_chart[ch]
        if nm in patient_by_name:
            cand |= patient_by_name[nm]
        return cand

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

    # P1 - 일마 차트번호/성명으로 차트정보를 묶고, 결제메모 승인번호와 한솔 승인번호를 직접 매칭
    if approval_to_patient_idxs:
        for _, dr in d_card.iterrows():
            if dr["d_idx"] in matched_dc:
                continue
            cand_pidx = _candidate_patient_idxs(dr)
            if not cand_pidx:
                continue

            cand_apprs = {appr for appr, pidxs in approval_to_patient_idxs.items() if pidxs & cand_pidx}
            if not cand_apprs:
                continue

            hc = h_card[(~h_card["h_idx"].isin(matched_h)) & (h_card["승인번호"].isin(cand_apprs))]
            if hc.empty:
                continue

            amt = int(dr["카드"])
            hc_amt = hc[hc["금액"] == amt]
            if len(hc_amt) == 1:
                add("P1_승인번호_차트연결", "🟢HIGH", [int(hc_amt.iloc[0]["h_idx"])], dr)
                continue
            if len(hc) == 1:
                add("P1_승인번호_차트연결", "🟡MED", [int(hc.iloc[0]["h_idx"])], dr)
                continue

            # 동일 승인번호 후보가 여러 건일 때는 금액 일치 우선, 그 외에는 가장 이른 건 1건 연결
            pick_src = hc_amt if not hc_amt.empty else hc
            pick = pick_src.sort_values(["시간_분", "h_idx"]).iloc[0]
            add("P1_승인번호_차트연결", "🟡MED", [int(pick["h_idx"])], dr)

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
@@ -517,84 +665,75 @@ def run_matching(hansol, daily, patient):
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

    return pd.DataFrame(results), matched_h, matched_dc


def build_hansol_chart_compare(match_df, patient):
    if match_df.empty:
        return pd.DataFrame(columns=[
            "차트번호", "한솔카드건수", "한솔카드번호", "한솔카드사(참고)", "차트카드사(참고)",
        ])

    hc = match_df[match_df["한솔_유형"] == "카드"].copy()
    if hc.empty:
        return pd.DataFrame(columns=[
            "차트번호", "한솔카드건수", "한솔카드번호", "한솔카드사(참고)", "차트카드사(참고)",
        ])

    hc["차트번호"] = hc["일마_차트"].apply(clean_no)
    hc["한솔카드번호_norm"] = hc["한솔_카드번호"].apply(lambda x: clean_no(x)[:12])
    grp = hc.groupby("차트번호").agg(
        한솔카드건수=("한솔카드번호_norm", "count"),
        한솔카드번호=("한솔카드번호_norm", lambda x: ", ".join(sorted(set([v for v in x if v])))),
        **{"한솔카드사(참고)": ("한솔_카드사", lambda x: ", ".join(sorted(set([str(v).strip() for v in x if str(v).strip()]))))},
    ).reset_index()

    p_card = patient[patient["분류"] == "카드"].copy()
    p_map = p_card.groupby("차트번호")["카드사"].apply(
        lambda x: ", ".join(sorted(set([str(v).strip() for v in x if str(v).strip()])))
    ).reset_index().rename(columns={"카드사": "차트카드사(참고)"})

    out = grp.merge(p_map, on="차트번호", how="left")
    out["차트카드사(참고)"] = out["차트카드사(참고)"].fillna("")
    return out.sort_values(["차트번호"]).reset_index(drop=True)


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

@@ -696,57 +835,63 @@ def tax_risk(hansol, daily, patient, matched_h):


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
        f_h = st.file_uploader("📥 한솔페이", type=["csv", "xlsx", "xls"], key="h")
    with c2:
        f_d = st.file_uploader("📥 일일마감", type=["csv", "xlsx", "xls"], key="d")
    with c3:
        f_p = st.file_uploader("📥 차트마감", type=["csv", "xlsx", "xls"], key="p")
        p_pw = st.text_input(
            "🔐 차트 파일 비밀번호 (선택)",
            type="password",
            key="p_pw",
            help="비워두면 비밀번호 없음 → 기본값(vsline99!!) 순서로 자동 시도합니다.",
        )

    if f_h and f_d and f_p:
        if st.button("🚀 정산 분석 시작", type="primary", use_container_width=True):
            with st.spinner("매칭 엔진 실행 중..."):
                hansol = parse_hansol(load_file(f_h))
                daily = parse_daily(load_file(f_d))
                patient = parse_patient(load_file(f_p, password=p_pw))
                if daily.empty:
                    st.error("일일마감 파일 파싱 실패")
                    st.stop()

                h_ok = hansol[hansol["tx_status"] == "정상"]
                tots = {
                    "h_card": int(h_ok[~h_ok["is_현금"]]["금액"].sum()),
                    "h_cash": int(h_ok[h_ok["is_현금"]]["금액"].sum()),
                    "d_card": int(daily["카드"].sum()),
                    "d_cash": int(daily["현금"].sum()),
                    "d_xfer": int(daily["이체"].sum()),
                    "d_plat": int(daily["플랫폼합"].sum()),
                    "d_tot": int(daily["총액"].sum()),
                    "p_card": int(patient[patient["분류"] == "카드"]["금액"].sum()),
                    "p_cash": int(patient[patient["분류"] == "현금"]["금액"].sum()),
                    "p_xfer": int(patient[patient["분류"] == "이체"]["금액"].sum()),
                    "p_plat": int(patient[patient["분류"] == "플랫폼"]["금액"].sum()),
                    "p_tot": int(patient["금액"].sum()),
                }

                match_df, matched_h, matched_dc = run_matching(hansol, daily, patient)
                hc_compare = build_hansol_chart_compare(match_df, patient)
                pc = build_patient_compare(daily, patient)
                tx = tax_risk(hansol, daily, patient, matched_h)

@@ -783,127 +928,155 @@ else:
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
    mapped_chart = hc_compare[hc_compare["한솔카드번호"] != ""] if not hc_compare.empty else pd.DataFrame()
    k6.metric("한솔↔차트매핑", f"{len(mapped_chart)}")

    # ── 탭 ──
    t0, t1, t2, t2b, t3, t4 = st.tabs([
        "📋 합계 매칭", "🚨 의심건", "💳 한솔↔일마", "🧩 한솔↔차트", "📊 일마↔차트", "🔒 세무위험",
    ])

    with t0:
        st.subheader("일자별 합계매칭")
        d_cash_xfer = tots["d_cash"] + tots["d_xfer"]
        p_cash_xfer = tots["p_cash"] + tots["p_xfer"]
        sm = pd.DataFrame({
            "구분": ["카드", "현금/영수증+이체", "플랫폼", "합계"],
            "한솔페이": [tots["h_card"], tots["h_cash"], "-", tots["h_card"] + tots["h_cash"]],
            "일일마감": [tots["d_card"], d_cash_xfer, tots["d_plat"], tots["d_tot"]],
            "차트마감": [tots["p_card"], p_cash_xfer, tots["p_plat"], tots["p_tot"]],
        })

        def _style_against_chart(row):
            styles = [""] * len(row)
            chart = row["차트마감"]
            for col in ["한솔페이", "일일마감"]:
                if isinstance(row[col], (int, float)) and isinstance(chart, (int, float)):
                    idx = row.index.get_loc(col)
                    styles[idx] = "background-color: #dbeafe" if int(row[col]) == int(chart) else "background-color: #fee2e2"
            return styles

        st.dataframe(sm.style.apply(_style_against_chart, axis=1), use_container_width=True, hide_index=True)

        diff_tbl = pd.DataFrame({
            "구분": ["카드", "현금+이체", "플랫폼", "합계"],
            "한솔-차트": [tots["h_card"] - tots["p_card"], tots["h_cash"] - p_cash_xfer, -tots["p_plat"], (tots["h_card"] + tots["h_cash"]) - tots["p_tot"]],
            "일마-차트": [tots["d_card"] - tots["p_card"], d_cash_xfer - p_cash_xfer, tots["d_plat"] - tots["p_plat"], tots["d_tot"] - tots["p_tot"]],
        })
        st.markdown("##### 구분별 차이 금액")
        st.dataframe(diff_tbl, use_container_width=True, hide_index=True)

        summary_text = build_messenger_summary(daily, hansol, tots)
        st.markdown("##### 메신저 공유용 요약 (차트기준)")
        st.text_area("복사해서 사용하세요", value=summary_text, height=420)

        rej = hansol[hansol["tx_status"] == "승인거절"]
        can = hansol[hansol["tx_status"] == "취소"]
        if len(rej) + len(can) > 0:
            st.info(f"📌 승인거절 {len(rej)}건 / 취소 {len(can)}건 (유효건에서 제외)")

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
        st.subheader("🧩 한솔↔차트 매칭 (일마 매칭 기반)")
        if not hc_compare.empty:
            st.caption("카드사 정보는 참고값이며, 불일치 판정은 제공하지 않습니다.")
            st.dataframe(hc_compare, use_container_width=True, hide_index=True)
        else:
            st.info("표시할 한솔↔차트 카드 매핑 정보가 없습니다.")

        st.markdown("##### 누락 추정 수납건 (한솔 기준)")
        h_unmatched_card = hansol[(hansol["tx_status"] == "정상") & (~hansol["is_현금"]) & (~hansol["h_idx"].isin(matched_h))].copy()
        if not h_unmatched_card.empty:
            p_card = patient[patient["분류"] == "카드"].copy()

            def _cand(hr):
                appr = clean_no(hr.get("승인번호", ""))
                by_appr = patient[patient["승인번호목록"].apply(lambda arr: appr in [clean_no(x) for x in arr])]
                by_amt = p_card[p_card["금액"] == int(hr["금액"])]
                merged = pd.concat([by_appr, by_amt], ignore_index=True).drop_duplicates()
                charts = ", ".join(sorted(set(merged["차트번호"].astype(str).tolist()[:5])))
                names = ", ".join(sorted(set(merged["이름"].astype(str).tolist()[:5])))
                reason = "승인번호/금액 후보" if len(merged) else "후보없음"
                return pd.Series([charts, names, reason])

            h_unmatched_card[["추정차트번호", "추정환자명", "근거"]] = h_unmatched_card.apply(_cand, axis=1)
            miss_cols = [c for c in ["시간표시", "금액", "카드번호", "승인번호", "추정차트번호", "추정환자명", "근거"] if c in h_unmatched_card.columns]
            st.dataframe(h_unmatched_card[miss_cols], use_container_width=True, hide_index=True)
        else:
            st.success("누락 추정 수납건이 없습니다.")

    with t3:
        st.subheader("📊 일마↔차트 수단별")
        if not pc.empty:
            vw = st.radio("표시", ["불일치만", "전체"], horizontal=True)
            disp = pc if vw == "전체" else pc[pc["불일치상세"] != "✅일치"]
            cols = [c for c in ["매칭", "차트번호", "성명", "불일치상세",
                                "[일마]카드", "[차트]카드", "[차트]본부금(참고)",
                                "[일마]현금+이체", "[차트]현금+이체",
                                "[일마]플랫폼", "[차트]플랫폼"] if c in disp.columns]
            st.dataframe(disp[cols], use_container_width=True, hide_index=True)

    with t4:
        st.subheader("🔒 세무위험")
        if not tx.empty:
            st.dataframe(tx.sort_values("위험등급"), use_container_width=True, hide_index=True)
            hi = tx[tx["위험등급"] == "🔴높음"]
            if len(hi):
                st.error(f"⚠️ 고위험 {len(hi)}건 (합계 {hi['금액'].sum():,}원)")
        else:
            st.success("✅ 세무위험 없음")
