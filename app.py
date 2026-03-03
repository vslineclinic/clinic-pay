"""
병원 정산 3-Way 대사 시스템 v2.0
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
"""

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


def load_file(f):
    if f.name.lower().endswith(".csv"):
        try:
            return pd.read_csv(f, encoding="utf-8")
        except UnicodeDecodeError:
            f.seek(0)
            return pd.read_csv(f, encoding="cp949")
    return pd.read_excel(f, header=None)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 파서
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def parse_hansol(raw):
    """한솔페이 파싱: 헤더 자동탐지, 시간 파싱, 거절/취소 분류"""
    hdr = 0
    for i, row in raw.iterrows():
        if row.astype(str).str.contains("금액|승인번호|카드번호", na=False).any():
            hdr = i
            break
    df = raw.iloc[hdr + 1:].copy()
    df.columns = [str(c).strip().replace("\n", "") for c in raw.iloc[hdr]]
    df = df.reset_index(drop=True)

    df["금액"] = df["금액"].apply(clean_money)
    df = df[df["금액"] > 0].copy()

    if "승인번호" in df.columns:
        df["승인번호"] = df["승인번호"].apply(clean_no)

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
        df.loc[s.str.contains("거절", na=False), "tx_status"] = "승인거절"
        df.loc[s.str.contains("취소", na=False), "tx_status"] = "취소"

    typcol = next((c for c in ["구분"] if c in df.columns), None)
    df["is_현금"] = False
    if typcol:
        df["is_현금"] = df[typcol].astype(str).str.contains("현금", na=False)

    # K/S: K=현금영수증, S=카드 → 모두 유지 (is_현금으로 구분)

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
    for c in amt_cols:
        df[c] = df[c].apply(clean_money)
    df["금액"] = df[amt_cols].sum(axis=1) if amt_cols else 0

    # 결제수단 정밀분류
    pay = df.get("결제수단", pd.Series(dtype=str)).astype(str)
    df["분류"] = "기타"
    df.loc[pay.str.startswith("카드"), "분류"] = "카드"
    df.loc[pay.str.contains("현금영수증", na=False), "분류"] = "현금"
    df.loc[(pay == "통장입금") | pay.str.contains("이체", na=False), "분류"] = "이체"
    df.loc[pay.str.startswith("기타"), "분류"] = "플랫폼"

    # 승인번호 추출
    df["승인번호목록"] = [[] for _ in range(len(df))]
    mcol = next((c for c in ["결제메모", "승인번호", "메모"] if c in df.columns), None)
    if mcol:
        df["승인번호목록"] = df[mcol].apply(lambda x: re.findall(r"\d{7,8}", str(x)) if pd.notna(x) else [])

    df["p_idx"] = range(len(df))
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 매칭 엔진
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def run_matching(hansol, daily, patient):
    """
    5-Pass 매칭:
      P1: 승인번호 직접매칭
      P2: 유일 금액 1:1
      P3: 분할결제 2~3건 합 (시간근접)
      P4: 시간-순서 상관 (동일금액 다건)
      P5: 현금영수증 + 이체
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
                한솔_유형="현금" if hr["is_현금"] else "카드",
                일마_순서=d_row["내원순서"], 일마_성명=d_row["성명"],
                일마_차트=d_row["차트번호"], 일마_카드=int(d_row["카드"]),
            ))
            matched_h.add(hi)
        matched_dc.add(d_row["d_idx"])

    # 승인번호→차트번호 맵
    appr_map = {}
    for _, pr in patient.iterrows():
        for a in pr["승인번호목록"]:
            appr_map[clean_no(a)] = pr["차트번호"]

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

            best = hc.iloc[(hc["시간_분"] - exp).abs().argsort()[:1]]
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
                    한솔_카드번호=str(hr.get("카드번호", "")), 한솔_유형="현금영수증",
                    일마_순서=dr["내원순서"], 일마_성명=dr["성명"],
                    일마_차트=dr["차트번호"], 일마_카드=int(dr["카드"]),
                ))
                matched_h.add(hr["h_idx"])

    return pd.DataFrame(results), matched_h, matched_dc


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

    mg = d_agg.merge(p_piv, on="차트번호", how="outer", indicator=True)
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

    mg = mg.fillna(0)
    for pay in ["카드", "현금", "이체", "플랫폼"]:
        ic, pc = f"[일마]{pay}", f"[차트]{pay}"
        if ic in mg.columns and pc in mg.columns:
            mg[f"△{pay}"] = mg[ic] - mg[pc]

    def detail(row):
        r = []
        for pay, ico in [("카드", "💳"), ("현금", "💵"), ("이체", "🏦"), ("플랫폼", "📱")]:
            c = f"△{pay}"
            if c in row and row[c] != 0:
                r.append(f"{ico}{pay}({row[c]:+,.0f})")
        return " / ".join(r) if r else "✅일치"

    mg["불일치상세"] = mg.apply(detail, axis=1)
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
# UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

st.title("📊 병원 정산 3-Way 대사 v2")
st.caption("한솔페이 × 일일마감 × 차트마감 | 자동 매칭 → 의심건 즉시 탐지")

c1, c2, c3 = st.columns(3)
with c1:
    f_h = st.file_uploader("📥 한솔페이", type=["csv", "xlsx", "xls"], key="h")
with c2:
    f_d = st.file_uploader("📥 일일마감", type=["csv", "xlsx", "xls"], key="d")
with c3:
    f_p = st.file_uploader("📥 차트마감", type=["csv", "xlsx", "xls"], key="p")

if f_h and f_d and f_p:
    if st.button("🚀 정산 분석 시작", type="primary", use_container_width=True):
        with st.spinner("매칭 엔진 실행 중..."):
            hansol = parse_hansol(load_file(f_h))
            daily = parse_daily(load_file(f_d))
            patient = parse_patient(load_file(f_p))
            if daily.empty:
                st.stop()

            # 합계
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
            pc = build_patient_compare(daily, patient)
            tx = tax_risk(hansol, daily, patient, matched_h)

            h_um = h_ok[~h_ok["h_idx"].isin(matched_h)]
            d_um = daily[(daily["카드"] > 0) & (~daily["d_idx"].isin(matched_dc))]
            n_ok = len(h_ok)
            n_m = len(matched_h)
            rate = n_m / n_ok * 100 if n_ok else 0

        # ── KPI ──
        st.divider()
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("한솔 유효건", f"{n_ok}")
        k2.metric("자동매칭", f"{n_m}", f"{rate:.0f}%")
        k3.metric("한솔 미매칭", f"{len(h_um)}", delta_color="inverse")
        k4.metric("일마 미매칭", f"{len(d_um)}", delta_color="inverse")
        k5.metric("세무위험", f"{len(tx)}", delta_color="inverse")

        # ── 탭 ──
        t0, t1, t2, t3, t4 = st.tabs([
            "📋 합계 대사", "🚨 의심건", "💳 한솔↔일마", "📊 일마↔차트", "🔒 세무위험",
        ])

        with t0:
            st.subheader("일자별 합계 대사")
            sm = pd.DataFrame({
                "구분": ["카드", "현금/영수증", "이체", "플랫폼", "합계"],
                "한솔페이": [tots["h_card"], tots["h_cash"], "-", "-", tots["h_card"] + tots["h_cash"]],
                "일일마감": [tots["d_card"], tots["d_cash"], tots["d_xfer"], tots["d_plat"], tots["d_tot"]],
                "차트마감": [tots["p_card"], tots["p_cash"], tots["p_xfer"], tots["p_plat"], tots["p_tot"]],
            })
            st.dataframe(sm, use_container_width=True, hide_index=True)

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

        with t3:
            st.subheader("📊 일마↔차트 수단별")
            if not pc.empty:
                vw = st.radio("표시", ["불일치만", "전체"], horizontal=True)
                disp = pc if vw == "전체" else pc[pc["불일치상세"] != "✅일치"]
                cols = [c for c in ["매칭", "차트번호", "성명", "불일치상세",
                                    "[일마]카드", "[차트]카드", "[일마]현금", "[차트]현금",
                                    "[일마]이체", "[차트]이체", "[일마]플랫폼", "[차트]플랫폼"] if c in disp.columns]
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
else:
    st.info("👆 3개 파일을 모두 업로드해주세요.")
