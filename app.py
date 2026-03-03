import streamlit as st
import pandas as pd
import numpy as np
import re
from collections import defaultdict

st.set_page_config(page_title="병원 정산 3-Way 대사 시스템", layout="wide")

st.title("📊 병원 정산 3-Way 대사 시스템")
st.markdown("한솔페이, 일일마감, 차트마감을 비교하여 **결제수단별 상세 차이**를 분석합니다.")
st.info("👇 3개의 파일을 업로드한 후 **[분석 시작]** 버튼을 눌러주세요.")

# -----------------------------
# Helpers
# -----------------------------
def load_data(file):
    """CSV/Excel 로드. CSV는 UTF-8 우선, 실패 시 CP949"""
    name = file.name.lower()
    if name.endswith(".csv"):
        try:
            return pd.read_csv(file, encoding="utf-8")
        except UnicodeDecodeError:
            file.seek(0)
            return pd.read_csv(file, encoding="cp949")
    return pd.read_excel(file)

def clean_money(x) -> int:
    """금액 문자열/숫자 -> int"""
    if pd.isna(x):
        return 0
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return 0
    s = s.replace(",", "").replace(" ", "")
    # 괄호 음수(예: (1,000)) 처리
    if re.match(r"^\(.*\)$", s):
        s = "-" + s.strip("()")
    # 숫자/소수점/부호만 남김
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in ("", "-", ".", "-."):
        return 0
    try:
        return int(float(s))
    except:
        return 0

def clean_no(x) -> str:
    """
    차트번호/승인번호 등 번호계열:
    - 소수점 제거
    - 숫자만 남김
    - 비어있으면 '-'
    """
    if pd.isna(x):
        return "-"
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return "-"
    # 엑셀에서 12345.0 처럼 들어온 것 제거
    s = s.split(".")[0]
    s = re.sub(r"\D", "", s)
    return s if s != "" else "-"

def extract_appr_numbers(text):
    """문자열에서 8자리 승인번호 후보 추출"""
    if pd.isna(text):
        return []
    return re.findall(r"\b\d{8}\b", str(text))

def normalize_columns(df):
    """개행/양끝공백 제거"""
    df = df.copy()
    df.columns = [str(c).replace("\n", "").strip() for c in df.columns]
    return df

def ensure_cols(df, cols, default=0):
    """없으면 생성"""
    for c in cols:
        if c not in df.columns:
            df[c] = default
    return df

def safe_str_series(s):
    return s.astype(str).fillna("")

# -----------------------------
# UI Upload
# -----------------------------
col1, col2, col3 = st.columns(3)
with col1:
    file_hansol = st.file_uploader("📥 1. [한솔] 한솔페이 내역", type=["csv", "xlsx", "xls"])
with col2:
    file_daily = st.file_uploader("📥 2. [일마] 일일마감 장부", type=["csv", "xlsx", "xls"])
with col3:
    file_patient = st.file_uploader("📥 3. [차트] 차트마감 데이터", type=["csv", "xlsx", "xls"])

if file_hansol and file_daily and file_patient:
    if st.button("🚀 정산 데이터 분석 시작하기", type="primary"):
        with st.spinner("결제수단별로 데이터를 꼼꼼하게 분류 중입니다..."):
            # -----------------------------
            # Load
            # -----------------------------
            df_h = normalize_columns(load_data(file_hansol))
            df_d = normalize_columns(load_data(file_daily))
            df_p = normalize_columns(load_data(file_patient))

            # -----------------------------
            # [일마] 전처리
            # -----------------------------
            # "내원" 문자열이 포함된 행을 헤더로 추정하여 헤더 재설정
            try:
                header_idx = df_d[df_d.apply(lambda x: x.astype(str).str.contains("내원").any(), axis=1)].index
                if len(header_idx) > 0:
                    df_d.columns = df_d.iloc[header_idx[0]]
                    df_d = df_d.iloc[header_idx[0] + 1 :].reset_index(drop=True)
                    df_d = normalize_columns(df_d)
            except:
                # 실패해도 진행
                pass

            # 필수 컬럼 방어
            # 일마에서 흔히: 성명, 차트번호, 카드, 현금, 이체, 강남언니, 여신티켓, 기타-지역화폐, 나만의닥터
            df_d = ensure_cols(df_d, ["성명", "차트번호"], default=np.nan)

            # 합계행 제거
            if "성명" in df_d.columns:
                df_d = df_d[df_d["성명"].notna()]
                df_d = df_d[~safe_str_series(df_d["성명"]).str.contains("합계")]

            df_d["차트번호"] = df_d["차트번호"].apply(clean_no)

            pay_cols = ["카드", "현금", "이체", "강남언니", "여신티켓", "기타-지역화폐", "나만의닥터"]
            df_d = ensure_cols(df_d, pay_cols, default=0)
            for c in pay_cols:
                df_d[c] = df_d[c].apply(clean_money)

            df_d["[일마] 플랫폼합계"] = df_d["강남언니"] + df_d["여신티켓"] + df_d["기타-지역화폐"] + df_d["나만의닥터"]
            df_d["[일마] 총액"] = df_d["카드"] + df_d["현금"] + df_d["이체"] + df_d["[일마] 플랫폼합계"]

            # -----------------------------
            # [차트] 전처리
            # -----------------------------
            df_p = ensure_cols(df_p, ["차트번호", "이름", "결제수단"], default=np.nan)
            df_p["차트번호"] = df_p["차트번호"].apply(clean_no)

            # 금액 컬럼 후보
            calc_candidates = ["비급여(과세총금액)", "비급여(비과세)", "본부금"]
            calc_cols = [c for c in calc_candidates if c in df_p.columns]
            for c in calc_cols:
                df_p[c] = df_p[c].apply(clean_money)

            df_p["[차트] 총수납액"] = df_p[calc_cols].sum(axis=1) if len(calc_cols) > 0 else 0

            # 결제수단 분류: 우선순위 명확화 + NaN 안전
            pay = safe_str_series(df_p["결제수단"])

            df_p["분류"] = "기타"
            # 플랫폼 키워드(필요하면 병원 환경에 맞게 추가)
            df_p.loc[pay.str.contains("강남|여신|닥터|지역화폐|페이|쿠폰|앱", na=False), "분류"] = "플랫폼"
            df_p.loc[pay.str.contains("통장|이체", na=False), "분류"] = "이체"
            df_p.loc[pay.str.contains("현금", na=False), "분류"] = "현금"
            df_p.loc[pay.str.contains("카드", na=False), "분류"] = "카드"

            # 승인번호 추출(승인번호 컬럼 우선, 없으면 결제메모에서 8자리 추출)
            df_p["추출된_승인번호리스트"] = [[] for _ in range(len(df_p))]
            if "승인번호" in df_p.columns:
                # 콤마/공백 분리 지원
                def parse_appr_cell(x):
                    if pd.isna(x):
                        return []
                    s = str(x).replace(" ", "")
                    parts = re.split(r"[,\|/;]+", s)
                    out = []
                    for p in parts:
                        n = clean_no(p)
                        if n != "-" and n != "":
                            out.append(n)
                    return out

                df_p["추출된_승인번호리스트"] = df_p["승인번호"].apply(parse_appr_cell)
            elif "결제메모" in df_p.columns:
                df_p["추출된_승인번호리스트"] = df_p["결제메모"].apply(extract_appr_numbers)
                df_p["추출된_승인번호리스트"] = df_p["추출된_승인번호리스트"].apply(lambda lst: [clean_no(x) for x in lst if clean_no(x) != "-"])

            # 승인번호 -> 차트번호 매핑(중복 가능성 대비: 리스트로 저장)
            appr_to_charts = defaultdict(list)
            for _, row in df_p.iterrows():
                cno = row.get("차트번호", "-")
                if cno == "-" or pd.isna(cno):
                    continue
                for appr in row.get("추출된_승인번호리스트", []):
                    if appr and appr != "-":
                        appr_to_charts[appr].append(cno)

            # 차트 데이터 피벗: 환자(차트번호+이름)별 결제수단 합계
            p_pivot = (
                df_p.pivot_table(
                    index=["차트번호", "이름"],
                    columns="분류",
                    values="[차트] 총수납액",
                    aggfunc="sum",
                )
                .fillna(0)
                .reset_index()
            )

            for c in ["카드", "현금", "이체", "플랫폼"]:
                if c not in p_pivot.columns:
                    p_pivot[c] = 0

            p_pivot = p_pivot.rename(
                columns={
                    "카드": "[차트] 카드",
                    "현금": "[차트] 현금",
                    "이체": "[차트] 이체",
                    "플랫폼": "[차트] 플랫폼",
                }
            )

            p_pivot["[차트] 총액"] = (
                p_pivot["[차트] 카드"]
                + p_pivot["[차트] 현금"]
                + p_pivot["[차트] 이체"]
                + p_pivot["[차트] 플랫폼"]
            )

            # -----------------------------
            # [한솔] 전처리
            # -----------------------------
            # 필수 컬럼 방어
            df_h = ensure_cols(df_h, ["금액", "승인번호"], default=np.nan)

            if "K/S" in df_h.columns:
                df_h = df_h[df_h["K/S"].astype(str).str.upper().str.strip() == "S"].copy()

            df_h["금액"] = df_h["금액"].apply(clean_money)
            df_h["승인번호"] = df_h["승인번호"].apply(clean_no)

            # 중복 제거는 승인번호만으로 하면 위험: (승인번호, 금액) 기준으로 완화
            df_h = df_h.drop_duplicates(subset=["승인번호", "금액"], keep="first").reset_index(drop=True)
            df_h["Hansol_ID"] = df_h.index.astype(int)

            # -----------------------------
            # 매칭 로직: 한솔 카드 승인 <-> 일마 카드
            # -----------------------------
            df_d_card = df_d[df_d["카드"] > 0].reset_index(drop=False)  # 원 index 보존
            matched_h = set()
            matched_d = set()
            matches = []
            h_to_chart = {}

            # 1) Direct 승인번호 매칭: 차트(승인번호->차트번호) -> 일마(차트번호) -> 한솔(승인번호)
            for _, h_row in df_h.iterrows():
                hid = int(h_row["Hansol_ID"])
                appr_no = h_row["승인번호"]
                if appr_no in appr_to_charts:
                    # 후보 차트번호가 여러 개면 일단 첫 후보 사용(중복은 의심으로 분리 가능)
                    chart_candidates = appr_to_charts[appr_no]
                    c_no = chart_candidates[0] if len(chart_candidates) > 0 else None
                    if c_no:
                        d_cands = df_d_card[
                            (df_d_card["차트번호"] == c_no) & (~df_d_card["index"].isin(matched_d))
                        ]
                        if not d_cands.empty:
                            d_target = d_cands.iloc[0]
                            matched_h.add(hid)
                            matched_d.add(d_target["index"])
                            h_to_chart[hid] = c_no
                            matches.append(
                                {
                                    "상태": "🔗 Direct 승인매칭",
                                    "차트번호": c_no,
                                    "환자명": d_target.get("성명", ""),
                                    "[일마]금액": int(d_target.get("카드", 0)),
                                    "[한솔]금액": int(h_row.get("금액", 0)),
                                    "승인번호": appr_no,
                                    "비고": "승인번호 일치",
                                }
                            )

            # 2) 남은 건: 금액 매칭(동일 금액 다건이면 순차 매칭)
            rem_h = df_h[~df_h["Hansol_ID"].isin(matched_h)].copy()
            rem_d = df_d_card[~df_d_card["index"].isin(matched_d)].copy()

            common_amounts = sorted(set(rem_h["금액"]).intersection(set(rem_d["카드"])))
            for amt in common_amounts:
                h_sub = rem_h[rem_h["금액"] == amt].copy()
                d_sub = rem_d[rem_d["카드"] == amt].copy()
                n = min(len(h_sub), len(d_sub))
                if n <= 0:
                    continue
                for i in range(n):
                    hid = int(h_sub.iloc[i]["Hansol_ID"])
                    did = d_sub.iloc[i]["index"]
                    cno = d_sub.iloc[i]["차트번호"]

                    matched_h.add(hid)
                    matched_d.add(did)
                    h_to_chart[hid] = cno

                    matches.append(
                        {
                            "상태": "✅ 금액매칭",
                            "차트번호": cno,
                            "환자명": d_sub.iloc[i].get("성명", ""),
                            "[일마]금액": int(amt),
                            "[한솔]금액": int(amt),
                            "승인번호": h_sub.iloc[i].get("승인번호", "-"),
                            "비고": "금액 일치(승인번호 불명)",
                        }
                    )

            match_df = pd.DataFrame(matches)

            # 미매칭 목록
            unmatched_h = df_h[~df_h["Hansol_ID"].isin(matched_h)][["승인번호", "금액"]].copy()
            unmatched_h = unmatched_h.rename(columns={"승인번호": "[한솔] 승인번호", "금액": "[한솔] 금액"})

            unmatched_d = df_d_card[~df_d_card["index"].isin(matched_d)][["차트번호", "성명", "카드"]].copy()
            unmatched_d = unmatched_d.rename(columns={"카드": "[일마] 카드금액"})

            # -----------------------------
            # Tabs
            # -----------------------------
            tab1, tab2, tab3 = st.tabs(["💳 [한솔] vs [일마]", "🏥 [차트] vs [한솔] (카드 다이렉트)", "📊 [차트] vs [일마] (수단별 분석)"])

            # ---- Tab1
            with tab1:
                st.subheader("💳 [한솔] vs [일마] 카드 승인 대사")

                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("매칭 건수", f"{len(match_df):,} 건")
                with c2:
                    st.metric("한솔 미매칭", f"{len(unmatched_h):,} 건")
                with c3:
                    st.metric("일마 미매칭", f"{len(unmatched_d):,} 건")

                st.markdown("### ✅ 매칭 결과")
                if len(match_df) > 0:
                    st.dataframe(match_df, use_container_width=True)
                else:
                    st.warning("매칭된 결과가 없습니다. (승인번호/금액/차트번호 형식 확인 필요)")

                st.markdown("### ⚠️ 미매칭: [한솔]")
                st.dataframe(unmatched_h, use_container_width=True)

                st.markdown("### ⚠️ 미매칭: [일마](카드)")
                st.dataframe(unmatched_d, use_container_width=True)

            # ---- Tab2
            with tab2:
                st.subheader("🏥 [차트] 카드수납액 vs [한솔] 실제 승인액 (연결된 차트 기준)")

                df_h2 = df_h.copy()
                df_h2["연결차트"] = df_h2["Hansol_ID"].map(h_to_chart).fillna("-")
                h_sum = df_h2[df_h2["연결차트"] != "-"].groupby("연결차트")["금액"].sum().reset_index()
                h_sum = h_sum.rename(columns={"연결차트": "차트번호", "금액": "[한솔] 카드승인합"})

                # 차트 카드 수납액
                p_card = df_p[df_p["분류"] == "카드"].groupby("차트번호")["[차트] 총수납액"].sum().reset_index()
                p_card = p_card.rename(columns={"[차트] 총수납액": "[차트] 카드수납합"})

                direct_merge = pd.merge(p_card, h_sum, on="차트번호", how="outer").fillna(0)
                direct_merge["차액(차트-한솔)"] = direct_merge["[차트] 카드수납합"] - direct_merge["[한솔] 카드승인합"]

                # 숫자 표시(소수점 없음) 보장
                for col in ["[차트] 카드수납합", "[한솔] 카드승인합", "차액(차트-한솔)"]:
                    direct_merge[col] = direct_merge[col].apply(lambda x: int(x) if pd.notna(x) else 0)

                st.markdown("### 차액 발생 건")
                st.dataframe(direct_merge[direct_merge["차액(차트-한솔)"] != 0], use_container_width=True)

                st.markdown("### 전체")
                st.dataframe(direct_merge.sort_values("차트번호"), use_container_width=True)

            # ---- Tab3
            with tab3:
                st.subheader("📊 [차트] vs [일마] 결제수단별 상세 비교")

                d_grouped = df_d.groupby(["차트번호", "성명"])[["카드", "현금", "이체", "[일마] 플랫폼합계"]].sum().reset_index()
                d_grouped = d_grouped.rename(
                    columns={
                        "카드": "[일마] 카드",
                        "현금": "[일마] 현금",
                        "이체": "[일마] 이체",
                        "[일마] 플랫폼합계": "[일마] 플랫폼",
                    }
                )

                # (차트번호, 이름)까지 병합하면 더 정확하지만, 일마는 성명/차트는 이름이 달라질 수 있어 outer 후 표시이름 통합
                final_merge = pd.merge(
                    d_grouped,
                    p_pivot,
                    left_on="차트번호",
                    right_on="차트번호",
                    how="outer",
                ).fillna(0)

                # 표시 이름 통합
                final_merge["표시이름"] = final_merge.get("성명", "")
                if "이름" in final_merge.columns:
                    mask = safe_str_series(final_merge["표시이름"]).str.strip().isin(["", "0", "nan"])
                    final_merge.loc[mask, "표시이름"] = final_merge.loc[mask, "이름"]

                # 필수 컬럼 보장
                final_merge = ensure_cols(final_merge, ["[일마] 카드", "[일마] 현금", "[일마] 이체", "[일마] 플랫폼"], default=0)
                final_merge = ensure_cols(final_merge, ["[차트] 카드", "[차트] 현금", "[차트] 이체", "[차트] 플랫폼"], default=0)

                # 차이 계산
                final_merge["카드차이(일마-차트)"] = final_merge["[일마] 카드"] - final_merge["[차트] 카드"]
                final_merge["현금차이(일마-차트)"] = final_merge["[일마] 현금"] - final_merge["[차트] 현금"]
                final_merge["이체차이(일마-차트)"] = final_merge["[일마] 이체"] - final_merge["[차트] 이체"]
                final_merge["플랫폼차이(일마-차트)"] = final_merge["[일마] 플랫폼"] - final_merge["[차트] 플랫폼"]

                # 정수화(소수점 표시 방지)
                int_cols = [
                    "[일마] 카드", "[차트] 카드",
                    "[일마] 현금", "[차트] 현금",
                    "[일마] 이체", "[차트] 이체",
                    "[일마] 플랫폼", "[차트] 플랫폼",
                    "카드차이(일마-차트)", "현금차이(일마-차트)", "이체차이(일마-차트)", "플랫폼차이(일마-차트)",
                ]
                for c in int_cols:
                    final_merge[c] = final_merge[c].apply(lambda x: int(float(x)) if pd.notna(x) else 0)

                def get_detail_reason(row):
                    reasons = []
                    if row["카드차이(일마-차트)"] != 0:
                        reasons.append(f"💳 카드({row['카드차이(일마-차트)']:,})")
                    if row["현금차이(일마-차트)"] != 0:
                        reasons.append(f"💵 현금({row['현금차이(일마-차트)']:,})")
                    if row["이체차이(일마-차트)"] != 0:
                        reasons.append(f"🏦 이체({row['이체차이(일마-차트)']:,})")
                    if row["플랫폼차이(일마-차트)"] != 0:
                        reasons.append(f"📱 플랫폼({row['플랫폼차이(일마-차트)']:,})")
                    return " / ".join(reasons) if reasons else "✅ 일치"

                final_merge["💡 상세 불일치 수단"] = final_merge.apply(get_detail_reason, axis=1)

                diff_df = final_merge[final_merge["💡 상세 불일치 수단"] != "✅ 일치"].copy()
                show_cols = [
                    "차트번호",
                    "표시이름",
                    "💡 상세 불일치 수단",
                    "[일마] 카드", "[차트] 카드",
                    "[일마] 현금", "[차트] 현금",
                    "[일마] 이체", "[차트] 이체",
                    "[일마] 플랫폼", "[차트] 플랫폼",
                ]

                st.markdown("### 불일치 건")
                st.dataframe(diff_df[show_cols].sort_values(["차트번호", "표시이름"]), use_container_width=True)

                st.markdown("### 전체")
                st.dataframe(final_merge[show_cols].sort_values(["차트번호", "표시이름"]), use_container_width=True)
else:
    st.warning("3개 파일을 모두 업로드해 주세요.")
