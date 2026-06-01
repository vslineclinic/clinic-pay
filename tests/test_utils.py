"""유틸 함수 단위 테스트 (clean_*, _parse_clock, 카드사 매칭)."""
import pandas as pd
import pytest


def test_clean_money(app):
    assert app.clean_money("1,234,500") == 1234500
    assert app.clean_money("₩ 50,000 ") == 50000
    assert app.clean_money("50000.0") == 50000
    assert app.clean_money("-3,000") == -3000
    assert app.clean_money(None) == 0
    assert app.clean_money(float("nan")) == 0
    assert app.clean_money("문자열") == 0
    assert app.clean_money("") == 0


def test_clean_no(app):
    assert app.clean_no("12345.0") == "12345"        # 엑셀 float → 정수부
    assert app.clean_no("00-123-4567") == "001234567"
    assert app.clean_no("승인 987654") == "987654"
    assert app.clean_no(None) == ""
    assert app.clean_no("nan") == ""
    assert app.clean_no("   ") == ""


def test_clean_name(app):
    assert app.clean_name(" 김 철수 ") == "김철수"
    assert app.clean_name("이*영희") == "이영희"
    assert app.clean_name("박-민수") == "박민수"
    assert app.clean_name(None) == ""


# ── _parse_clock: 시간 파싱 버그 회귀 방지 (이전 zfill 방식이 오독하던 케이스) ──
@pytest.mark.parametrize("raw, minutes, disp", [
    ("143025", 14 * 60 + 30, "14:30:25"),                 # HHMMSS
    ("160000", 16 * 60, "16:00:00"),                       # HHMMSS 정각
    ("14:30:25", 14 * 60 + 30, "14:30:25"),               # 콜론 HH:MM:SS
    ("14:30", 14 * 60 + 30, "14:30:00"),                  # 콜론, 초 없음 (이전: 00:14)
    ("0930", 9 * 60 + 30, "09:30:00"),                    # HHMM (이전: 00:09)
    ("930", 9 * 60 + 30, "09:30:00"),                     # 3자리
    ("2024-01-15 15:00:10", 15 * 60, "15:00:10"),         # 날짜+시간 (이전: 20:24)
    ("1430.0", 14 * 60 + 30, "14:30:00"),                 # 엑셀 숫자형 (이전: 01:43)
])
def test_parse_clock_formats(app, raw, minutes, disp):
    assert app._parse_clock(raw) == (minutes, disp)


@pytest.mark.parametrize("raw", ["", "nan", None, float("nan"), "25:99", "99:99"])
def test_parse_clock_invalid_returns_zero(app, raw):
    assert app._parse_clock(raw) == (0, "")


def test_extract_card_company(app):
    assert app._extract_card_company("카드-삼성카드") == "삼성"
    assert app._extract_card_company("카드 현대") == "현대"
    assert app._extract_card_company("현금영수증") == ""


def test_card_company_match(app):
    assert app.card_company_match("현대", "현대카드") is True
    assert app.card_company_match("삼성카드", "삼성") is True
    assert app.card_company_match("삼성", "현대") is False
    assert app.card_company_match("", "삼성") is False
