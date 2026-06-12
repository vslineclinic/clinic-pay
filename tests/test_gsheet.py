"""구글시트 연동(일일마감) 테스트: ID 추출 / 탭 이름 후보 / gviz CSV·오류 파싱 /
날짜→탭 자동 매칭(폴백 탐지). 네트워크 fetch는 monkeypatch로 대체한다."""
from datetime import date
from urllib.parse import urlparse, parse_qs, unquote

import pytest


def test_extract_sheet_id_from_url(app):
    url = "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789/edit#gid=0"
    assert app._extract_sheet_id(url) == "1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"


def test_extract_sheet_id_from_bare_id(app):
    sid = "1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"
    assert app._extract_sheet_id(sid) == sid


def test_extract_sheet_id_rejects_garbage(app):
    assert app._extract_sheet_id("") is None
    assert app._extract_sheet_id(None) is None
    assert app._extract_sheet_id("https://example.com/not-a-sheet") is None
    assert app._extract_sheet_id("short") is None


def test_daily_sheet_date_format(app):
    # 기본(첫 후보) 형식은 "26.06.02"(%y.%m.%d).
    assert date(2026, 6, 2).strftime(app.DAILY_SHEET_DATE_FMT) == "26.06.02"
    assert date(2025, 12, 31).strftime(app.DAILY_SHEET_DATE_FMT) == "25.12.31"


def test_candidate_tab_names_covers_observed_formats(app):
    # 실측 형식: 26.06.02(인천·잠실), 06.02(엔디어트 최근), 6.2(엔디어트 과거), 26.6.2(잠실 과거)
    cands = app._candidate_tab_names(date(2026, 6, 2))
    for expected in ["26.06.02", "06.02", "6.2", "26.6.2", "2026.06.02"]:
        assert expected in cands, f"{expected} 누락: {cands}"
    assert cands[0] == "26.06.02"               # 가장 흔한 형식이 1순위
    assert len(cands) == len(set(cands))         # 중복 없음


def test_get_clinic_daily_sheets_default(app):
    sheets = app.get_clinic_daily_sheets()
    assert isinstance(sheets, dict)
    assert set(sheets.keys()) == set(app.CLINIC_DAILY_SHEETS.keys())


# ── 날짜별 시트 전환 (시트 파일 이전 지점: 엔디어트 26.06.12~ 새 시트) ──────────

NEW_URL = "https://docs.google.com/spreadsheets/d/NEW" + "n" * 30 + "/edit"
OLD_URL = "https://docs.google.com/spreadsheets/d/OLD" + "o" * 30 + "/edit"
DATED_ENTRY = [("2026-06-12", NEW_URL), ("", OLD_URL)]


def test_resolve_daily_sheet_url_passthrough_for_plain_url(app):
    url = "https://docs.google.com/spreadsheets/d/" + "A" * 30 + "/edit"
    assert app.resolve_daily_sheet_url(url, date(2026, 6, 12)) == url


def test_resolve_daily_sheet_url_switches_on_cutoff(app):
    # 시작일 당일부터 새 시트(포함), 그 전날까지는 기존 시트.
    assert app.resolve_daily_sheet_url(DATED_ENTRY, date(2026, 6, 12)) == NEW_URL
    assert app.resolve_daily_sheet_url(DATED_ENTRY, date(2026, 6, 13)) == NEW_URL
    assert app.resolve_daily_sheet_url(DATED_ENTRY, date(2027, 1, 1)) == NEW_URL
    assert app.resolve_daily_sheet_url(DATED_ENTRY, date(2026, 6, 11)) == OLD_URL
    assert app.resolve_daily_sheet_url(DATED_ENTRY, date(2025, 12, 31)) == OLD_URL


def test_resolve_daily_sheet_url_order_independent(app):
    # 설정에 옛 시트를 위에 적어도(정렬로 보정) 결과는 같아야 한다.
    rev = list(reversed(DATED_ENTRY))
    assert app.resolve_daily_sheet_url(rev, date(2026, 6, 12)) == NEW_URL
    assert app.resolve_daily_sheet_url(rev, date(2026, 6, 11)) == OLD_URL


def test_resolve_daily_sheet_url_accepts_secrets_list_of_lists(app):
    # Streamlit Secrets(TOML)에서는 튜플이 아닌 list-of-list로 들어온다.
    entry = [["2026-06-12", NEW_URL], ["", OLD_URL]]
    assert app.resolve_daily_sheet_url(entry, date(2026, 6, 12)) == NEW_URL
    assert app.resolve_daily_sheet_url(entry, date(2026, 6, 11)) == OLD_URL


def test_resolve_daily_sheet_url_all_dated_falls_back_to_oldest(app):
    # 빈 시작일 기본값이 없고 분석 날짜가 모든 시작일 이전이면 가장 오래된 시트로.
    entry = [("2026-06-12", NEW_URL), ("2026-01-01", OLD_URL)]
    assert app.resolve_daily_sheet_url(entry, date(2025, 5, 1)) == OLD_URL


def test_sheet_entry_configured(app):
    assert app.sheet_entry_configured("https://docs.google.com/spreadsheets/d/X/edit")
    assert app.sheet_entry_configured(DATED_ENTRY)
    assert not app.sheet_entry_configured("")
    assert not app.sheet_entry_configured("   ")
    assert not app.sheet_entry_configured([])
    assert not app.sheet_entry_configured([("2026-06-12", "")])


def test_clinic_sheets_endiart_switches_sheet_on_2026_06_12(app):
    # 엔디어트: 26.06.12부터 새 스프레드시트, 그 이전 날짜는 기존 시트(요건 회귀 고정).
    entry = app.CLINIC_DAILY_SHEETS["엔디어트"]
    new_sid = "1e1BMgUAF_GGAAJBib8gVAEVoJC6ZjunQ3K399tdc_mA"
    old_sid = "1YdruwnAghZARwLALGD9rvbDZsnw4b4FKSt6l3_sLgd4"
    assert app._extract_sheet_id(
        app.resolve_daily_sheet_url(entry, date(2026, 6, 12))) == new_sid
    assert app._extract_sheet_id(
        app.resolve_daily_sheet_url(entry, date(2026, 6, 11))) == old_sid


def test_load_gsheet_daily_resolves_dated_entry_per_date(app, monkeypatch):
    # 단일일·기간 분석이 공유하는 load_gsheet_daily에 [(시작일, URL)] 설정값을 그대로
    # 주면 날짜에 맞는 스프레드시트로 요청해야 한다(시트별 캐시 분리 포함).
    csv = "내원순서,차트번호,성명,카드\n1,100,김철수,50000\n"
    fallback = "견본\n구분\n\n"
    seen = []

    def fake(url, timeout=20):
        seen.append(url)
        if url.endswith("/edit"):
            return b"<html>no gid pattern</html>"      # gid 매핑 없음 → gviz 경로
        name = unquote(parse_qs(urlparse(url).query).get("sheet", [""])[0])
        ok = name in ("26.06.12", "26.06.11")
        return (csv if ok else fallback).encode("utf-8")

    monkeypatch.setattr(app, "_fetch_url_bytes", fake)
    cache = {}
    app.load_gsheet_daily(DATED_ENTRY, date(2026, 6, 12), cache=cache)
    assert any("/NEW" in u for u in seen) and not any("/OLD" in u for u in seen)
    seen.clear()
    app.load_gsheet_daily(DATED_ENTRY, date(2026, 6, 11), cache=cache)
    assert any("/OLD" in u for u in seen) and not any("/NEW" in u for u in seen)


def test_parse_gviz_csv_returns_object_grid_consumable_by_parse_daily(app):
    csv = "내원순서,차트번호,성명,카드,현금\n1,100,김철수,50000,0\n2,200,이영희,0,30000\n"
    raw = app._parse_gviz_csv(csv.encode("utf-8"), sheet_name="26.06.02")
    assert raw.shape == (3, 5)
    assert str(raw.dtypes.iloc[0]) == "object"   # PyArrow string 추론 방지(엑셀 경로와 동일)
    assert str(raw.iloc[0, 1]) == "차트번호"      # 0행=헤더 텍스트(데이터로 보존)
    d, _refund = app.parse_daily(raw)
    assert len(d) == 2 and int(d["카드"].sum()) == 50000 and int(d["현금"].sum()) == 30000


def test_parse_gviz_csv_handles_missing_platform_columns(app):
    # 플랫폼 컬럼(여신티켓 등)이 일부만 있어도 합산 시 타입 오류 없이 파싱돼야 한다.
    csv = "내원순서,차트번호,성명,카드,나만의닥터\n1,100,김철수,50000,3000\n"
    raw = app._parse_gviz_csv(csv.encode("utf-8"), sheet_name="26.05.31")
    d, _ = app.parse_daily(raw)
    assert len(d) == 1
    assert int(d["나만의닥터"].sum()) == 3000
    assert int(d["플랫폼합"].sum()) == 3000


def test_parse_gviz_csv_sheet_not_found_raises_lookup(app):
    body = (
        "/*O_o*/\n"
        'google.visualization.Query.setResponse({"version":"0.6",'
        '"status":"error","errors":[{"reason":"invalid_query",'
        '"message":"Invalid query: sheet not found"}]});'
    )
    with pytest.raises(LookupError):
        app._parse_gviz_csv(body.encode("utf-8"), sheet_name="26.06.02")


def test_parse_gviz_csv_permission_html_raises_permission(app):
    body = "<!DOCTYPE html><html><head><title>Sign in</title></head><body>...</body></html>"
    with pytest.raises(PermissionError):
        app._parse_gviz_csv(body.encode("utf-8"), sheet_name="26.06.02")


# ── 날짜 → 탭 자동 매칭 (gviz 폴백 탐지) ─────────────────────────

def _fake_fetch(tabs, fallback):
    """sheet= 파라미터를 보고 응답 bytes를 돌려주는 가짜 _fetch_url_bytes.
    tabs에 없는 이름(=존재하지 않는 탭)은 fallback(첫 시트)을 반환 — 실제 gviz 동작 모사."""
    def _fetch(url, timeout=20):
        name = unquote(parse_qs(urlparse(url).query).get("sheet", [""])[0])
        return tabs.get(name, fallback).encode("utf-8")
    return _fetch


URL = "https://docs.google.com/spreadsheets/d/" + "A" * 30 + "/edit"


def test_load_gsheet_daily_picks_existing_tab_06_02(app, monkeypatch):
    # 엔디어트형: '26.06.02'는 없고 '06.02'만 존재. 자동으로 '06.02'를 골라야 한다.
    fallback = "견본\n구분,차트번호,성명\n,,\n"               # 첫 시트(폴백)
    tabs = {"06.02": "내원순서,차트번호,성명,카드\n1,100,김철수,50000\n"}
    monkeypatch.setattr(app, "_fetch_url_bytes", _fake_fetch(tabs, fallback))
    raw, name = app.load_gsheet_daily(URL, date(2026, 6, 2), cache={})
    assert name == "06.02"
    d, _ = app.parse_daily(raw)
    assert len(d) == 1 and int(d["카드"].sum()) == 50000


def test_load_gsheet_daily_prefers_26_06_02(app, monkeypatch):
    # 인천·잠실형: '26.06.02' 존재 → 1순위로 즉시 선택.
    fallback = "견본\n구분\n\n"
    tabs = {
        "26.06.02": "내원순서,차트번호,성명,카드\n1,100,김철수,11000\n",
        "06.02": "내원순서,차트번호,성명,카드\n1,200,다른시트,99999\n",
    }
    monkeypatch.setattr(app, "_fetch_url_bytes", _fake_fetch(tabs, fallback))
    raw, name = app.load_gsheet_daily(URL, date(2026, 6, 2), cache={})
    assert name == "26.06.02"
    d, _ = app.parse_daily(raw)
    assert int(d["카드"].sum()) == 11000


def test_load_gsheet_daily_not_found_raises_instead_of_returning_fallback(app, monkeypatch):
    # 어떤 후보도 없으면 폴백(첫 시트)을 조용히 반환하지 않고 LookupError를 내야 한다.
    fallback = "견본양식\n구분,차트번호\n,\n"
    monkeypatch.setattr(app, "_fetch_url_bytes", _fake_fetch({}, fallback))
    with pytest.raises(LookupError):
        app.load_gsheet_daily(URL, date(2026, 6, 2), cache={})


def test_daily_format_warning_flags_missing_chart_no(app):
    # 차트번호 컬럼이 없는 양식(엔디어트형) → 경고문 반환.
    # 헤더 탐지(성명+'차트번호' 키워드 2개)는 통과하되, 정확한 '차트번호' 컬럼은 없는 상태.
    csv = (
        "구분,성명,결제단말기,현금,나만의 닥터,이름/차트번호 중복여부\n"
        "초진,김철수,0,10000,0,\n"
    )
    raw = app._parse_gviz_csv(csv.encode("utf-8"), sheet_name="06.02")
    daily, _ = app.parse_daily(raw)
    assert not daily.empty
    assert daily["차트번호"].astype(str).str.strip().eq("").all()  # 차트번호 비어 있음
    assert app.daily_format_warning(daily) is not None


def test_daily_format_warning_none_for_standard(app):
    # 차트번호가 있는 표준 양식(인천·잠실형) → 경고 없음.
    csv = "내원순서,차트번호,성명,카드,현금\n1,100,김철수,50000,0\n"
    raw = app._parse_gviz_csv(csv.encode("utf-8"), sheet_name="26.06.02")
    daily, _ = app.parse_daily(raw)
    assert app.daily_format_warning(daily) is None


def test_standard_template_csv_round_trips_through_parser(app):
    # templates/일일마감_표준양식.csv 가 parse_daily와 호환됨을 회귀 검증.
    import pathlib
    import pandas as pd

    p = pathlib.Path(__file__).resolve().parent.parent / "templates" / "일일마감_표준양식.csv"
    raw = pd.read_csv(p, header=None, dtype=object, keep_default_na=False, encoding="utf-8-sig")
    daily, refund = app.parse_daily(raw)
    assert len(daily) == 4 and len(refund) == 1            # 환불 1행 자동 분리
    assert int(daily["카드"].sum()) == 150000
    assert int(daily["이체"].sum()) == 50000
    assert int(daily["나만의닥터"].sum()) == 120000
    assert app.daily_format_warning(daily) is None          # 표준 양식 → 경고 없음
    tot = app.compute_totals(pd.DataFrame(), daily, refund, pd.DataFrame())
    assert tot["d_card"] == 150000
    assert tot["d_cashxfer"] == 100000                      # 현금80000-환불30000+이체50000


# ── 차트마감↔일일마감 교차검증 (타 지점/날짜 엿보기 방지) ──────────

def _chart_df(charts):
    import pandas as pd
    return pd.DataFrame({"차트번호": [str(c) for c in charts]})


def test_cross_check_pass_when_charts_overlap(app):
    d = _chart_df([100, 101, 102, 103])
    p = _chart_df([100, 101, 102, 103, 200, 201])   # 일마 ⊆ 차트 → 100%
    status, msg, info = app.cross_check_daily_patient(d, p)
    assert status == "ok" and msg is None and info["rate"] == 1.0


def test_cross_check_blocks_different_branch_or_date(app):
    d = _chart_df([100, 101, 102, 103, 104])
    p = _chart_df([900, 901, 902, 903, 904])        # 겹침 0 → 차단
    status, msg, _ = app.cross_check_daily_patient(d, p)
    assert status == "block" and "다른 지점" in msg


def test_cross_check_warns_when_daily_has_no_chart_no(app):
    # 차트번호 없는 비표준 일일마감 → 차단이 아니라 경고 후 진행(전환기 편의).
    d = _chart_df(["", "", ""])
    p = _chart_df([100, 101, 102])
    status, msg, _ = app.cross_check_daily_patient(d, p)
    assert status == "warn" and "표준 양식" in msg


def test_cross_check_warns_when_patient_unreadable(app):
    d = _chart_df([100, 101, 102])
    p = _chart_df([])                                # 차트마감에서 차트번호 0 → 경고
    status, msg, _ = app.cross_check_daily_patient(d, p)
    assert status == "warn" and "차트마감" in msg


def test_cross_check_threshold_boundary_60pct(app):
    # 기본 기준 0.6: 3/5=60% 통과, 2/5=40% 차단.
    status1, _, info1 = app.cross_check_daily_patient(
        _chart_df([1, 2, 3, 4, 5]), _chart_df([1, 2, 3, 90, 91])
    )
    assert info1["rate"] == 0.6 and status1 == "ok"
    status2, _, _ = app.cross_check_daily_patient(
        _chart_df([1, 2, 3, 4, 5]), _chart_df([1, 2, 90, 91, 92])
    )
    assert status2 == "block"


def test_cross_check_respects_custom_min_rate(app):
    # min_rate를 0.8로 올리면 60% 쌍도 차단.
    status, _, _ = app.cross_check_daily_patient(
        _chart_df([1, 2, 3, 4, 5]), _chart_df([1, 2, 3, 90, 91]), min_rate=0.8
    )
    assert status == "block"


def test_load_gsheet_daily_caches_fallback_signature(app, monkeypatch):
    # cache를 주면 폴백 시그니처를 재사용해 sentinel 호출을 매번 반복하지 않는다.
    fallback = "견본\n구분\n\n"
    tabs = {"26.06.02": "내원순서,차트번호,성명,카드\n1,100,김,1000\n"}
    calls = {"n": 0}
    base = _fake_fetch(tabs, fallback)

    def counting(url, timeout=20):
        calls["n"] += 1
        return base(url, timeout)

    monkeypatch.setattr(app, "_fetch_url_bytes", counting)
    cache = {}
    app.load_gsheet_daily(URL, date(2026, 6, 2), cache=cache)
    first = calls["n"]
    app.load_gsheet_daily(URL, date(2026, 6, 2), cache=cache)
    # 2번째 호출은 sentinel 재조회 없이 1회 fetch (직전 성공 형식 우선)
    assert calls["n"] - first == 1


# ── export(gid) 경로 — 숫자열 머리글 보존 (gviz 누락 문제 회피) ──────────

def _gid_blob(name, gid):
    r"""edit 페이지 부트스트랩의 escape된 {gid↔탭이름} 인코딩 한 토막 재현.
    실제 형식: \"<gid>\",[{\"1\":[[0,0,\"<name>\"]"""
    return '\\"' + gid + '\\",[{\\"1\\":[[0,0,\\"' + name + '\\"]'


def test_export_csv_url_targets_gid(app):
    u = app._export_csv_url("SID123", "925828584")
    assert "/SID123/export" in u and "format=csv" in u and "gid=925828584" in u


def test_candidate_tab_names_includes_apgujeong_korean_format(app):
    # 압구정 탭 이름 형식 '26년 06월 02일'을 후보로 생성해야 한다.
    assert "26년 06월 02일" in app._candidate_tab_names(date(2026, 6, 2))


def test_clinic_sheets_include_ilsan_and_apgujeong(app):
    assert "일산점" in app.CLINIC_DAILY_SHEETS
    assert "압구정" in app.CLINIC_DAILY_SHEETS


def test_sheet_gid_map_parses_edit_bootstrap(app, monkeypatch):
    html = "junk" + _gid_blob("26.06.04", "925828584") + "mid" \
        + _gid_blob("26년 06월 02일", "2134713726") + "end"
    monkeypatch.setattr(app, "_fetch_url_bytes", lambda url, timeout=20: html.encode("utf-8"))
    m = app._sheet_gid_map("SID")
    assert m["26.06.04"] == "925828584"
    assert m["26년 06월 02일"] == "2134713726"


def test_sheet_gid_map_empty_on_fetch_error(app, monkeypatch):
    # edit 페이지를 못 읽으면(구글 포맷 변경·네트워크) 빈 dict → gviz 폴백을 유도.
    def boom(url, timeout=20):
        raise ConnectionError("net")
    monkeypatch.setattr(app, "_fetch_url_bytes", boom)
    assert app._sheet_gid_map("SID") == {}


def test_load_gsheet_daily_uses_export_preserving_numeric_headers(app, monkeypatch):
    # 핵심: gviz는 '숫자 열'의 텍스트 머리글(카드/현금/이체)을 떨구지만 export는 보존한다.
    # export 경로가 성공하면 gviz는 아예 호출되지 않아야 한다.
    html = "x" + _gid_blob("26.06.04", "925828584") + "y"
    export_csv = "내원순서,차트번호,성명,이체,현금,카드\n1,100,김철수,0,0,50000\n"

    def fake(url, timeout=20):
        if url.endswith("/edit"):
            return html.encode("utf-8")
        if "/export?" in url:
            gid = parse_qs(urlparse(url).query).get("gid", [""])[0]
            assert gid == "925828584"
            return export_csv.encode("utf-8")
        raise AssertionError("export 성공 시 gviz를 호출하면 안 됨: " + url)

    monkeypatch.setattr(app, "_fetch_url_bytes", fake)
    raw, name = app.load_gsheet_daily(URL, date(2026, 6, 4), cache={})
    assert name == "26.06.04"
    d, _ = app.parse_daily(raw)
    assert int(d["카드"].sum()) == 50000   # 머리글 보존 → '카드'로 정확 매핑


def test_load_gsheet_daily_falls_back_to_gviz_when_no_gid_map(app, monkeypatch):
    # gid 매핑을 못 얻으면(edit에 패턴 없음) 기존 gviz 폴백으로 정상 동작.
    fallback = "견본\n구분\n\n"
    tabs = {"26.06.04": "내원순서,차트번호,성명,카드\n1,100,김,11000\n"}

    def fake(url, timeout=20):
        if url.endswith("/edit"):
            return b"<html>no gid pattern</html>"     # 매핑 0건
        name = unquote(parse_qs(urlparse(url).query).get("sheet", [""])[0])
        return tabs.get(name, fallback).encode("utf-8")

    monkeypatch.setattr(app, "_fetch_url_bytes", fake)
    raw, name = app.load_gsheet_daily(URL, date(2026, 6, 4), cache={})
    assert name == "26.06.04"
    d, _ = app.parse_daily(raw)
    assert int(d["카드"].sum()) == 11000
