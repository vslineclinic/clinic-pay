"""pytest 공통 설정.

app.py는 Streamlit 스크립트이지만 UI 로직이 main() 안에 있어
`import app` 만으로는 UI가 실행되지 않는다(=import-safe). 다만 모듈 최상단의
`import streamlit as st` 때문에 streamlit 모듈 자체는 존재해야 하므로,
streamlit이 설치돼 있지 않아도 순수 로직 테스트가 돌도록 경량 스텁을 주입한다.
(streamlit이 설치돼 있어도, 테스트의 결정성을 위해 스텁으로 통일한다.)
"""
import sys
import types
import pathlib
from unittest.mock import MagicMock

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class _StreamlitStub(types.ModuleType):
    """함수 런타임에서 호출하는 st.* (error/spinner/session_state 등)를 무력화하는 스텁."""

    def __init__(self):
        super().__init__("streamlit")
        self.session_state = {}
        self.secrets = {}

    def __getattr__(self, name):
        # st.error/st.warning/st.spinner(...) 등 임의 호출·컨텍스트매니저를 모두 no-op 처리
        return MagicMock()


# app import 이전에 스텁을 강제 주입 (UI는 main() 안에만 있으므로 import 시 호출되지 않음)
sys.modules["streamlit"] = _StreamlitStub()

import app as _app  # noqa: E402


@pytest.fixture
def app():
    """검증 대상 모듈(app.py)을 반환. 매 테스트마다 streamlit 세션 상태를 초기화한다."""
    sys.modules["streamlit"].session_state = {}
    return _app
