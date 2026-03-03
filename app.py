
 import streamlit as st
 import pandas as pd
 import numpy as np
 from io import BytesIO
 import itertools
 import re
 
 st.set_page_config(page_title="병원 정산 3-Way 대사 시스템", layout="wide")
 
 st.title("📊 병원 정산 3-Way 대사 시스템")
 st.markdown("한솔페이, 일일마감, 차트마감을 비교하여 **결제수단별 상세 차이**를 분석합니다.")
 
 st.info("👇 3개의 파일을 업로드한 후 **[분석 시작]** 버튼을 눌러주세요.")
 
 col1, col2, col3 = st.columns(3)
 with col1:
     file_hansol = st.file_uploader("📥 1. [한솔] 한솔페이 내역", type=['csv', 'xlsx', 'xls'])
 with col2:
     file_daily = st.file_uploader("📥 2. [일마] 일일마감 장부", type=['csv', 'xlsx', 'xls'])
 with col3:
     file_patient = st.file_uploader("📥 3. [차트] 차트마감 데이터", type=['csv', 'xlsx', 'xls'])
 
+
 def load_data(file):
     if file.name.lower().endswith('.csv'):
         try:
             return pd.read_csv(file, encoding='utf-8')
         except UnicodeDecodeError:
             file.seek(0)
             return pd.read_csv(file, encoding='cp949')
-    else:
-        return pd.read_excel(file)
+    return pd.read_excel(file)
+
 
 def clean_money(x):
-    if pd.isna(x): return 0
-    try: return int(float(str(x).replace(',', '').replace(' ', '')))
-    except: return 0
+    if pd.isna(x):
+        return 0
+    try:
+        return int(float(str(x).replace(',', '').replace(' ', '')))
+    except Exception:
+        return 0
+
 
 def clean_no(x):
     """차트번호, 승인번호 등 소수점 제거"""
-    if pd.isna(x) or str(x).strip() == '' or str(x).lower() == 'nan': return '-'
+    if pd.isna(x) or str(x).strip() == '' or str(x).lower() == 'nan':
+        return '-'
     try:
-        val = str(x).split('.')[0] # 소수점 앞자리만 취함
-        return re.sub(r'\D', '', val) # 숫자만 남김
-    except: return str(x).strip()
+        val = str(x).split('.')[0]
+        digits = re.sub(r'\D', '', val)
+        return digits if digits else '-'
+    except Exception:
+        return str(x).strip()
+
 
 def extract_appr_numbers(text):
-    if pd.isna(text): return []
+    if pd.isna(text):
+        return []
     return re.findall(r'\b\d{8}\b', str(text))
 
+
+def clean_name(x):
+    if pd.isna(x):
+        return ''
+    return re.sub(r'\s+', '', str(x)).strip()
+
+
+def similar_chart_no(left, right):
+    """차트번호 오기 의심: 동일 길이에서 1자리 이하 차이 또는 앞/뒤 1자리 누락 허용"""
+    a, b = clean_no(left), clean_no(right)
+    if '-' in (a, b):
+        return False
+    if a == b:
+        return True
+    if abs(len(a) - len(b)) > 1:
+        return False
+    if len(a) == len(b):
+        mismatch = sum(ch1 != ch2 for ch1, ch2 in zip(a, b))
+        return mismatch <= 1
+    # 길이 1 차이인 경우, 긴 번호에서 한 자리 제거시 동일하면 유사로 판단
+    longer, shorter = (a, b) if len(a) > len(b) else (b, a)
+    for i in range(len(longer)):
+        if longer[:i] + longer[i + 1:] == shorter:
+            return True
+    return False
+
+
+def parse_hansol_time(df):
+    time_candidates = ['승인일시', '거래일시', '일시', '거래시간', '승인시간']
+    chosen = next((c for c in time_candidates if c in df.columns), None)
+    if chosen:
+        parsed = pd.to_datetime(df[chosen], errors='coerce')
+    else:
+        parsed = pd.Series(pd.NaT, index=df.index)
+    # 시간정보가 없으면 기존 순서를 시간 대체키로 사용
+    return parsed.fillna(pd.Timestamp('1970-01-01') + pd.to_timedelta(df.index, unit='m'))
+
+
 if file_hansol and file_daily and file_patient:
-    
     if st.button("🚀 정산 데이터 분석 시작하기", type="primary"):
         with st.spinner('결제수단별로 데이터를 꼼꼼하게 분류 중입니다...'):
-            
             df_h = load_data(file_hansol)
             df_d = load_data(file_daily)
             df_p = load_data(file_patient)
-            
+
             # --- 1. [일마] 전처리 ---
             header_idx = df_d[df_d.apply(lambda x: x.astype(str).str.contains('내원').any(), axis=1)].index
             if len(header_idx) > 0:
                 df_d.columns = df_d.iloc[header_idx[0]]
-                df_d = df_d.iloc[header_idx[0]+1:].reset_index(drop=True)
+                df_d = df_d.iloc[header_idx[0] + 1:].reset_index(drop=True)
             col_map = {str(col): str(col).replace('\n', '') for col in df_d.columns}
             df_d.rename(columns=col_map, inplace=True)
-            
+
             if '성명' in df_d.columns:
                 df_d = df_d[df_d['성명'].notna() & ~df_d['성명'].astype(str).str.contains('합계')]
-            
+
             df_d['차트번호'] = df_d['차트번호'].apply(clean_no)
+            df_d['성명'] = df_d['성명'].apply(clean_name)
+            df_d['일마순번'] = np.arange(len(df_d))
 
             for col in ['카드', '현금', '이체', '강남언니', '여신티켓', '기타-지역화폐', '나만의닥터']:
-                if col in df_d.columns: df_d[col] = df_d[col].apply(clean_money)
-                else: df_d[col] = 0
-                    
+                if col in df_d.columns:
+                    df_d[col] = df_d[col].apply(clean_money)
+                else:
+                    df_d[col] = 0
+
             df_d['[일마] 플랫폼합계'] = df_d['강남언니'] + df_d['여신티켓'] + df_d['기타-지역화폐'] + df_d['나만의닥터']
             df_d['[일마] 총액'] = df_d['카드'] + df_d['현금'] + df_d['이체'] + df_d['[일마] 플랫폼합계']
-            
+
             # --- 2. [차트] 전처리 ---
             df_p['차트번호'] = df_p['차트번호'].apply(clean_no)
+            df_p['이름'] = df_p['이름'].apply(clean_name)
             calc_cols = [c for c in ['비급여(과세총금액)', '비급여(비과세)', '본부금'] if c in df_p.columns]
-            for c in calc_cols: df_p[c] = df_p[c].apply(clean_money)
+            for c in calc_cols:
+                df_p[c] = df_p[c].apply(clean_money)
             df_p['[차트] 총수납액'] = df_p[calc_cols].sum(axis=1) if calc_cols else 0
-            
-            # 결제수단별 분류 (카드/현금/이체/플랫폼)
+
             df_p['분류'] = '기타'
-            df_p.loc[df_p['결제수단'].astype(str).str.contains('카드'), '분류'] = '카드'
-            df_p.loc[df_p['결제수단'].astype(str).str.contains('현금'), '분류'] = '현금'
-            df_p.loc[df_p['결제수단'].astype(str).str.contains('통장|이체'), '분류'] = '이체'
-            df_p.loc[df_p['결제수단'].astype(str).str.contains('기타|강남|여신|닥터'), '분류'] = '플랫폼'
+            df_p.loc[df_p['결제수단'].astype(str).str.contains('카드', na=False), '분류'] = '카드'
+            df_p.loc[df_p['결제수단'].astype(str).str.contains('현금', na=False), '분류'] = '현금'
+            df_p.loc[df_p['결제수단'].astype(str).str.contains('통장|이체', na=False), '분류'] = '이체'
+            df_p.loc[df_p['결제수단'].astype(str).str.contains('기타|강남|여신|닥터', na=False), '분류'] = '플랫폼'
 
-            # 승인번호 추출
             df_p['추출된_승인번호리스트'] = [[] for _ in range(len(df_p))]
             if '승인번호' in df_p.columns:
-                df_p['추출된_승인번호리스트'] = df_p['승인번호'].apply(lambda x: [clean_no(i) for i in str(x).replace(' ', '').split(',') if clean_no(i) != '-'])
+                df_p['추출된_승인번호리스트'] = df_p['승인번호'].apply(
+                    lambda x: [clean_no(i) for i in str(x).replace(' ', '').split(',') if clean_no(i) != '-']
+                )
             elif '결제메모' in df_p.columns:
-                df_p['추출된_승인번호리스트'] = df_p['결제메메모'].apply(extract_appr_numbers)
+                df_p['추출된_승인번호리스트'] = df_p['결제메모'].apply(extract_appr_numbers)
 
             appr_to_chart = {}
             for _, row in df_p.iterrows():
-                for appr in row['추출된_승인번호리스트']: appr_to_chart[appr] = row['차트번호']
+                for appr in row['추출된_승인번호리스트']:
+                    appr_to_chart[appr] = row['차트번호']
 
-            # 차트 데이터 피벗 (환자별 결제수단 금액 합계)
             p_pivot = df_p.pivot_table(index=['차트번호', '이름'], columns='분류', values='[차트] 총수납액', aggfunc='sum').fillna(0).reset_index()
             for c in ['카드', '현금', '이체', '플랫폼']:
-                if c not in p_pivot.columns: p_pivot[c] = 0
+                if c not in p_pivot.columns:
+                    p_pivot[c] = 0
             p_pivot.columns = [f'[차트] {c}' if c in ['카드', '현금', '이체', '플랫폼'] else c for c in p_pivot.columns]
             p_pivot['[차트] 총액'] = p_pivot.filter(like='[차트]').sum(axis=1)
 
             # --- 3. [한솔] 전처리 ---
-            if 'K/S' in df_h.columns: df_h = df_h[df_h['K/S'] == 'S'].copy()
+            if 'K/S' in df_h.columns:
+                df_h = df_h[df_h['K/S'] == 'S'].copy()
             df_h['금액'] = df_h['금액'].apply(clean_money)
             df_h['승인번호'] = df_h['승인번호'].apply(clean_no)
             df_h = df_h.drop_duplicates(subset=['승인번호'], keep='first').reset_index(drop=True)
             df_h['Hansol_ID'] = df_h.index
+            df_h['한솔시간키'] = parse_hansol_time(df_h)
 
             # === [매칭 로직] ===
             df_d_card = df_d[df_d['카드'] > 0].reset_index()
             matches = []
             matched_h, matched_d = set(), set()
             h_to_chart = {}
 
-            # 승인번호 Direct 매칭
-            for idx, h_row in df_h.iterrows():
+            # 1) 승인번호 Direct 매칭
+            for _, h_row in df_h.iterrows():
                 appr_no = h_row['승인번호']
                 if appr_no in appr_to_chart:
                     c_no = appr_to_chart[appr_no]
                     d_cands = df_d_card[(df_d_card['차트번호'] == c_no) & (~df_d_card['index'].isin(matched_d))]
                     if not d_cands.empty:
                         d_target = d_cands.iloc[0]
-                        matched_h.add(h_row['Hansol_ID']); matched_d.add(d_target['index']); h_to_chart[h_row['Hansol_ID']] = c_no
-                        matches.append({'상태': '🔗 Direct 승인매칭', '차트번호': c_no, '환자명': d_target['성명'], '[일마]금액': d_target['카드'], '[한솔]금액': h_row['금액'], '비고': '승인번호 일치'})
-
-            # 나머지 금액 매칭 (간략화)
-            rem_h = df_h[~df_h['Hansol_ID'].isin(matched_h)]
-            rem_d = df_d_card[~df_d_card['index'].isin(matched_d)]
-            for amt in set(rem_h['금액']).intersection(set(rem_d['카드'])):
-                h_sub, d_sub = rem_h[rem_h['금액']==amt], rem_d[rem_d['카드']==amt]
-                for i in range(min(len(h_sub), len(d_sub))):
-                    matched_h.add(h_sub.iloc[i]['Hansol_ID']); matched_d.add(d_sub.iloc[i]['index'])
-                    h_to_chart[h_sub.iloc[i]['Hansol_ID']] = d_sub.iloc[i]['차트번호']
-                    matches.append({'상태': '✅ 금액매칭', '차트번호': d_sub.iloc[i]['차트번호'], '환자명': d_sub.iloc[i]['성명'], '[일마]금액': amt, '[한솔]금액': amt, '비고': '금액 일치'})
+                        matched_h.add(h_row['Hansol_ID'])
+                        matched_d.add(d_target['index'])
+                        h_to_chart[h_row['Hansol_ID']] = c_no
+                        matches.append({
+                            '상태': '🔗 Direct 승인매칭',
+                            '차트번호': c_no,
+                            '환자명': d_target['성명'],
+                            '[일마]금액': d_target['카드'],
+                            '[한솔]금액': h_row['금액'],
+                            '비고': '승인번호 일치'
+                        })
+
+            # 2) 금액 + 순서/시간 기반 추정 매칭
+            rem_h = df_h[~df_h['Hansol_ID'].isin(matched_h)].sort_values('한솔시간키').copy()
+            rem_d = df_d_card[~df_d_card['index'].isin(matched_d)].sort_values('일마순번').copy()
+
+            amount_groups = rem_h.groupby('금액')
+            for _, d_row in rem_d.iterrows():
+                amt = d_row['카드']
+                if amt in amount_groups.groups:
+                    h_indices = [idx for idx in amount_groups.groups[amt] if rem_h.loc[idx, 'Hansol_ID'] not in matched_h]
+                    if h_indices:
+                        h_idx = h_indices[0]
+                        h_row = rem_h.loc[h_idx]
+                        matched_h.add(h_row['Hansol_ID'])
+                        matched_d.add(d_row['index'])
+                        h_to_chart[h_row['Hansol_ID']] = d_row['차트번호']
+                        matches.append({
+                            '상태': '🟨 추정매칭',
+                            '차트번호': d_row['차트번호'],
+                            '환자명': d_row['성명'],
+                            '[일마]금액': amt,
+                            '[한솔]금액': h_row['금액'],
+                            '비고': '금액 일치 + 일마순서/한솔시간순 상관 기반 추정'
+                        })
+
+            match_df = pd.DataFrame(matches)
+            unmatched_h = df_h[~df_h['Hansol_ID'].isin(matched_h)][['승인번호', '금액', '한솔시간키']].copy()
+            unmatched_h['상태'] = '❌ 한솔 미매칭'
+            unmatched_d = df_d_card[~df_d_card['index'].isin(matched_d)][['차트번호', '성명', '카드', '일마순번']].copy()
+            unmatched_d['상태'] = '❌ 일마 미매칭'
 
             # === 탭 구성 ===
             tab1, tab2, tab3 = st.tabs(["💳 [한솔] vs [일마]", "🏥 [차트] vs [한솔] (다이렉트)", "📊 [차트] vs [일마] (수단별 분석)"])
-            
+
             with tab1:
-                st.subheader("카드 승인 대사 (의심 거래)")
-                # 미매칭 한솔/일마 정리 (생략)
-                st.write("차액 및 누락 건 리스트...") # 결과 데이터프레임 출력 부분
+                st.subheader("카드 승인 대사 (의심 거래 포함)")
+                st.caption("※ 결제메모에 승인번호 누적 입력이 늘수록 추정매칭(🟨) 건수는 자연스럽게 감소합니다.")
+                st.dataframe(match_df if not match_df.empty else pd.DataFrame(columns=['상태', '차트번호', '환자명', '[일마]금액', '[한솔]금액', '비고']))
+                st.markdown("#### 미매칭 목록")
+                st.dataframe(unmatched_h)
+                st.dataframe(unmatched_d)
 
             with tab2:
                 st.subheader("🏥 [차트] 카드수납액 vs [한솔] 실제승인액")
-                df_h['연결차트'] = df_h['Hansol_ID'].map(h_to_chart).fillna(0)
+                df_h['연결차트'] = df_h['Hansol_ID'].map(h_to_chart).fillna('0')
                 h_sum = df_h.groupby('연결차트')['금액'].sum().reset_index()
                 p_card = df_p[df_p['분류'] == '카드'].groupby('차트번호')['[차트] 총수납액'].sum().reset_index()
-                
+
                 direct_merge = pd.merge(p_card, h_sum, left_on='차트번호', right_on='연결차트', how='outer').fillna(0)
                 direct_merge['차액'] = direct_merge['[차트] 총수납액'] - direct_merge['금액']
-                
+
                 st.dataframe(direct_merge[direct_merge['차액'] != 0])
 
             with tab3:
                 st.subheader("📊 [차트] vs [일마] 결제수단별 상세 비교")
-                d_grouped = df_d.groupby(['차트번호', '성명'])[['카드', '현금', '이체', '[일마] 플랫폼합계']].sum().reset_index()
+                d_grouped = df_d.groupby(['차트번호', '성명'], as_index=False)[['카드', '현금', '이체', '[일마] 플랫폼합계']].sum()
                 d_grouped.columns = ['차트번호', '성명', '[일마] 카드', '[일마] 현금', '[일마] 이체', '[일마] 플랫폼']
-                
-                final_merge = pd.merge(d_grouped, p_pivot, on='차트번호', how='outer').fillna(0)
-                
-                # 수단별 차이 계산
+
+                # 1) 기본 차트번호 정확 매칭
+                p_ready = p_pivot.copy()
+                p_ready = p_ready.rename(columns={'이름': '성명'})
+                exact_merge = pd.merge(d_grouped, p_ready, on='차트번호', how='left', suffixes=('', '_차트'))
+                exact_merge['매칭상태'] = np.where(exact_merge['성명_차트'].isna(), '미매칭', '✅ 차트번호 일치')
+
+                # 2) 이름 동일 + 차트번호 유사 추정매칭
+                unmatched_daily = exact_merge[exact_merge['매칭상태'] == '미매칭'][['차트번호', '성명', '[일마] 카드', '[일마] 현금', '[일마] 이체', '[일마] 플랫폼']].copy()
+                used_chart_idx = set(p_ready[p_ready['차트번호'].isin(exact_merge['차트번호'])].index)
+                fuzzy_rows = []
+
+                for _, d_row in unmatched_daily.iterrows():
+                    candidates = p_ready[~p_ready.index.isin(used_chart_idx)]
+                    candidates = candidates[candidates['성명'] == d_row['성명']]
+                    candidates = candidates[candidates['차트번호'].apply(lambda x: similar_chart_no(d_row['차트번호'], x))]
+                    if not candidates.empty:
+                        p_row = candidates.iloc[0]
+                        used_chart_idx.add(p_row.name)
+                        fuzzy_rows.append({
+                            **d_row.to_dict(),
+                            '차트번호_차트': p_row['차트번호'],
+                            '[차트] 카드': p_row.get('[차트] 카드', 0),
+                            '[차트] 현금': p_row.get('[차트] 현금', 0),
+                            '[차트] 이체': p_row.get('[차트] 이체', 0),
+                            '[차트] 플랫폼': p_row.get('[차트] 플랫폼', 0),
+                            '매칭상태': '⚠️ 차트번호 유사(기재오류 의심)'
+                        })
+
+                fuzzy_df = pd.DataFrame(fuzzy_rows)
+
+                exact_valid = exact_merge[exact_merge['매칭상태'] == '✅ 차트번호 일치'].copy()
+                exact_valid['차트번호_차트'] = exact_valid['차트번호']
+
+                final_merge = pd.concat([
+                    exact_valid[['차트번호', '차트번호_차트', '성명', '[일마] 카드', '[일마] 현금', '[일마] 이체', '[일마] 플랫폼', '[차트] 카드', '[차트] 현금', '[차트] 이체', '[차트] 플랫폼', '매칭상태']],
+                    fuzzy_df[['차트번호', '차트번호_차트', '성명', '[일마] 카드', '[일마] 현금', '[일마] 이체', '[일마] 플랫폼', '[차트] 카드', '[차트] 현금', '[차트] 이체', '[차트] 플랫폼', '매칭상태']] if not fuzzy_df.empty else pd.DataFrame()
+                ], ignore_index=True).fillna(0)
+
                 final_merge['카드차이'] = final_merge['[일마] 카드'] - final_merge['[차트] 카드']
                 final_merge['현금차이'] = final_merge['[일마] 현금'] - final_merge['[차트] 현금']
                 final_merge['이체차이'] = final_merge['[일마] 이체'] - final_merge['[차트] 이체']
-                final_merge['플랫폼차이'] = final_merge['[일마] 플랫폼'] - final_merge['[차트] 플랫폼(기타)']
-                
-                # 구체적 분석 메시지
+                final_merge['플랫폼차이'] = final_merge['[일마] 플랫폼'] - final_merge['[차트] 플랫폼']
+
                 def get_detail_reason(row):
                     reasons = []
-                    if row['카드차이'] != 0: reasons.append(f"💳 카드({row['카드차이']:,})")
-                    if row['현금차이'] != 0: reasons.append(f"💵 현금({row['현금차이']:,})")
-                    if row['이체차이'] != 0: reasons.append(f"🏦 이체({row['이체차이']:,})")
-                    if row['플랫폼차이'] != 0: reasons.append(f"📱 플랫폼({row['플랫폼차이']:,})")
+                    if row['카드차이'] != 0:
+                        reasons.append(f"💳 카드({row['카드차이']:,})")
+                    if row['현금차이'] != 0:
+                        reasons.append(f"💵 현금({row['현금차이']:,})")
+                    if row['이체차이'] != 0:
+                        reasons.append(f"🏦 이체({row['이체차이']:,})")
+                    if row['플랫폼차이'] != 0:
+                        reasons.append(f"📱 플랫폼({row['플랫폼차이']:,})")
                     return " / ".join(reasons) if reasons else "✅ 일치"
 
                 final_merge['💡 상세 불일치 수단'] = final_merge.apply(get_detail_reason, axis=1)
-                
-                st.dataframe(final_merge[final_merge['💡 상세 불일치 수단'] != "✅ 일치"][
-                    ['차트번호', '성명', '💡 상세 불일치 수단', '[일마] 카드', '[차트] 카드', '[일마] 현금', '[차트] 현금', '[일마] 이체', '[차트] 이체']
-                ])
+
+                st.dataframe(final_merge[[
+                    '매칭상태', '차트번호', '차트번호_차트', '성명', '💡 상세 불일치 수단',
+                    '[일마] 카드', '[차트] 카드', '[일마] 현금', '[차트] 현금', '[일마] 이체', '[차트] 이체', '[일마] 플랫폼', '[차트] 플랫폼'
+                ]])
