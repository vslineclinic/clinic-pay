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
