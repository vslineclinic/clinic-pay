"""구글시트 연동(일일마감) 테스트: ID 추출 / 날짜 탭 포맷 / gviz CSV 파싱.

네트워크가 필요한 fetch는 제외하고, 순수 로직(ID 추출·날짜 포맷·CSV/오류 파싱)만 검증한다.
"""
from datetime import date

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
    # 시트 탭(워크시트) 이름은 "26.06.02" 형식이어야 한다.
    assert date(2026, 6, 2).strftime(app.DAILY_SHEET_DATE_FMT) == "26.06.02"
    assert date(2025, 12, 31).strftime(app.DAILY_SHEET_DATE_FMT) == "25.12.31"


def test_get_clinic_daily_sheets_default(app):
    # Secrets가 비어 있으면(conftest 스텁) 기본 dict를 반환.
    sheets = app.get_clinic_daily_sheets()
    assert isinstance(sheets, dict)
    assert set(sheets.keys()) == set(app.CLINIC_DAILY_SHEETS.keys())


def test_parse_gviz_csv_returns_raw_grid_consumable_by_parse_daily(app):
    # gviz CSV(헤더 없음) → header=None 그리드. parse_daily가 그대로 소비 가능해야 한다.
    csv = "내원순서,차트번호,성명,카드,현금\n1,100,김철수,50000,0\n2,200,이영희,0,30000\n"
    raw = app._parse_gviz_csv(csv.encode("utf-8"), sheet_name="26.06.02")
    assert raw.shape == (3, 5)
    assert str(raw.iloc[0, 1]) == "차트번호"      # 0행이 헤더 텍스트(데이터로 보존됨)
    d, _refund = app.parse_daily(raw)
    assert len(d) == 2
    assert int(d["카드"].sum()) == 50000
    assert int(d["현금"].sum()) == 30000


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
