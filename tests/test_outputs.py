"""산출물 생성 테스트: build_ai_text / build_ai_excel / build_3way_table + import-safety."""
import io

import pandas as pd

import factories as F


def _full_pipeline(app, scenario):
    hansol, daily, patient = scenario()
    H = app.parse_hansol(hansol)
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    match_df, mh, mdc = app.run_matching(H, D, P)
    p1_full, p1_diff = app.compute_p1(match_df, P, D)
    h_um, d_um = app.compute_p2(H, D, mh, mdc)
    totals = app.compute_totals(H, D, DR, P)
    channel = app.compute_channel_recon(totals)
    suspects = {c: app.find_channel_suspects(c, H, D, P, totals=totals, top_n=15)
                for c in ["카드", "현금+이체", "플랫폼"]}
    return dict(H=H, D=D, DR=DR, P=P, match_df=match_df, mh=mh, mdc=mdc,
                p1_full=p1_full, p1_diff=p1_diff, h_um=h_um, d_um=d_um,
                totals=totals, channel=channel, suspects=suspects)


def test_ai_text_all_match_short_circuits(app):
    ctx = _full_pipeline(app, F.basic_three_files)
    text = app.build_ai_text(ctx["H"], ctx["D"], ctx["DR"], ctx["P"], ctx["channel"],
                             ctx["p1_full"], ctx["h_um"], ctx["d_um"], ctx["suspects"],
                             totals=ctx["totals"])
    assert "[META]" in text and "[채널대사]" in text
    assert "합계 일치" in text                          # 차이 없으면 단축 종료


def test_ai_text_mismatch_has_star2_section(app):
    ctx = _full_pipeline(app, F.mismatch_three_files)
    text = app.build_ai_text(ctx["H"], ctx["D"], ctx["DR"], ctx["P"], ctx["channel"],
                             ctx["p1_full"], ctx["h_um"], ctx["d_um"], ctx["suspects"],
                             totals=ctx["totals"])
    assert "★★ 승인번호확정매칭" in text
    assert "차트번호별 3-way 통합" in text


def test_ai_text_gap_summary_single_cause(app):
    """차트 200만 35,000(일마·한솔 30,000) → 오차요약에 차트 기준 서술 + 단일일치후보(유일)."""
    ctx = _full_pipeline(app, F.mismatch_three_files)
    text = app.build_ai_text(ctx["H"], ctx["D"], ctx["DR"], ctx["P"], ctx["channel"],
                             ctx["p1_full"], ctx["h_um"], ctx["d_um"], ctx["suspects"],
                             totals=ctx["totals"])
    assert "[오차요약" in text
    assert "일마가 차트보다 5,000원 적음(-5,000)" in text       # 차트 기준 부호·금액
    assert "한솔이 차트보다 5,000원 적음(-5,000)" in text
    assert "단일일치후보(유일): 이영희(차트#200)" in text        # 단일 원인 힌트
    assert "전액 환자단위 분해·잔여 0" in text                   # 검산


def test_ai_text_gap_summary_front_only_hint(app):
    """일마에만 카드 40,000(한솔·차트에 없음) → '한솔(PG)에 없음 = 미수납 의심' 위치 힌트."""
    def scenario():
        hansol, daily, patient = F.basic_three_files()
        daily.loc[len(daily)] = [4, "400", "최지우", 40000, 0, 0]
        return hansol, daily, patient

    ctx = _full_pipeline(app, scenario)
    text = app.build_ai_text(ctx["H"], ctx["D"], ctx["DR"], ctx["P"], ctx["channel"],
                             ctx["p1_full"], ctx["h_um"], ctx["d_um"], ctx["suspects"],
                             totals=ctx["totals"])
    assert "일마가 차트보다 40,000원 많음(+40,000)" in text
    assert "단일일치후보(유일): 최지우(차트#400)[일일마감에만 존재]" in text
    assert "한솔(PG)에 없음" in text and "미수납" in text


def test_ai_text_gap_summary_combo_cause(app):
    """단일 일치가 없고 2건 조합(−5,000 + −5,000 = −10,000)일 때 조합일치후보 제시."""
    def scenario():
        hansol, daily, patient = F.mismatch_three_files()   # 200: 일-차 -5,000
        daily.iloc[1, 3] = 45000                            # 100 김철수 카드 45,000(차트 50,000) → -5,000
        return hansol, daily, patient

    ctx = _full_pipeline(app, scenario)
    text = app.build_ai_text(ctx["H"], ctx["D"], ctx["DR"], ctx["P"], ctx["channel"],
                             ctx["p1_full"], ctx["h_um"], ctx["d_um"], ctx["suspects"],
                             totals=ctx["totals"])
    assert "일마가 차트보다 10,000원 적음(-10,000)" in text
    assert "단일일치후보" not in text
    assert "조합일치후보(2건 복합)" in text
    assert "김철수(차트#100)" in text and "이영희(차트#200)" in text


def test_find_exact_combos_bounded_and_exact(app):
    items = [("100", -5000), ("200", -5000), ("300", 7000), ("400", -12000)]
    combos = app._find_exact_combos(items, -10000)
    assert combos, "합계 -10,000 조합을 찾아야 함"
    assert all(sum(v for _, v in c) == -10000 for c in combos)
    # (-5000, -5000) 페어 포함
    assert any({ch for ch, _ in c} == {"100", "200"} for c in combos)
    # 일치 조합이 없으면 빈 목록
    assert app._find_exact_combos(items, 999) == []


def test_patient_channel_diffs_channels(app):
    pp = {"100": {"카드": 50000, "현금": 0, "이체": 0, "플랫폼": 0},
          "300": {"카드": 0, "현금": 20000, "이체": 0, "플랫폼": 0}}
    dp = {"100": {"카드": 45000, "현금": 0, "이체": 0, "플랫폼": 0},
          "300": {"카드": 0, "현금": 0, "이체": 20000, "플랫폼": 0}}
    diffs = app._patient_channel_diffs(pp, dp)
    assert diffs["카드"] == [("100", -5000)]
    # 현금→이체 이동은 현금+이체 채널 합계로는 0 → 미포함 (채널 정의 = compute_channel_recon)
    assert diffs["현금+이체"] == []
    assert diffs["플랫폼"] == []


def test_ai_text_respects_max_chars(app):
    ctx = _full_pipeline(app, F.mismatch_three_files)
    text = app.build_ai_text(ctx["H"], ctx["D"], ctx["DR"], ctx["P"], ctx["channel"],
                             ctx["p1_full"], ctx["h_um"], ctx["d_um"], ctx["suspects"],
                             totals=ctx["totals"], max_chars=200)
    assert len(text) <= 200 + len("\n…(축약)")


def test_build_3way_table(app):
    ctx = _full_pipeline(app, F.mismatch_three_files)
    tw = app.build_3way_table(ctx["H"], ctx["D"], ctx["P"], ctx["p1_full"])
    assert not tw.empty
    assert {"차트번호", "차트카드", "일마카드", "일-차카드차"}.issubset(tw.columns)
    row200 = tw[tw["차트번호"] == "200"].iloc[0]
    assert row200["일-차카드차"] == 30000 - 35000        # 일마 30000 - 차트 35000


def test_build_ai_excel_sheets(app):
    ctx = _full_pipeline(app, F.mismatch_three_files)
    data = app.build_ai_excel(
        ctx["p1_diff"], ctx["h_um"], ctx["d_um"], ctx["totals"],
        ctx["channel"], ctx["suspects"],
        hansol=ctx["H"], daily=ctx["D"], patient=ctx["P"], p1_full=ctx["p1_full"],
    )
    assert isinstance(data, (bytes, bytearray)) and len(data) > 0
    xls = pd.ExcelFile(io.BytesIO(data))
    # 핵심 시트 존재 확인
    for name in ["0_채널대사", "0_3way통합", "4_합계"]:
        assert name in xls.sheet_names


def test_app_is_import_safe_and_exposes_main(app):
    """app.py가 import-safe(UI는 main() 안)임을 보장 — main 호출 가능 객체 존재."""
    assert callable(app.main)
