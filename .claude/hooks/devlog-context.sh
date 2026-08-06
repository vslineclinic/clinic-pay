#!/usr/bin/env bash
# SessionStart 훅 — docs/DEVLOG.md의 '최근 작업' 섹션을 세션 컨텍스트로 주입한다.
#
# 목적: 이 저장소는 여러 계정에서 번갈아 개발된다. 세션이 시작되면 직전 세션(다른 계정일
# 수 있음)이 무엇을 왜 바꿨는지 자동으로 보이게 해서, 이미 내려진 결정을 되돌리거나 같은
# 문제를 다시 푸는 사고를 막는다.
#
# 실패해도 세션을 막지 않는다(파일이 없거나 jq가 없으면 조용히 아무것도 주입하지 않음).
set -uo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$PWD}"
DEVLOG="$ROOT/docs/DEVLOG.md"
MAX_LINES=60

command -v jq >/dev/null 2>&1 || exit 0
[ -f "$DEVLOG" ] || exit 0

# '## 최근 작업'부터 다음 '## ' 제목 직전까지 = 최신 항목들
BODY="$(awk '/^## 최근 작업/{f=1;next} /^## /{f=0} f' "$DEVLOG" | head -n "$MAX_LINES")"
[ -n "${BODY//[[:space:]]/}" ] || exit 0

printf '%s' "$BODY" | jq -Rs \
  '{hookSpecificOutput:{hookEventName:"SessionStart",additionalContext:(
     "[docs/DEVLOG.md — 이 저장소의 직전 작업 내역 (다른 계정의 작업일 수 있음)]\n"
     + .
     + "\n※ 이 저장소는 여러 계정이 번갈아 개발합니다. 코드를 바꾸면 docs/DEVLOG.md 맨 위에"
     + " 항목을 추가하고(코드와 같은 커밋), 구조가 바뀌면 HANDOVER.md를 갱신하세요."
     + " 전체 규칙은 CLAUDE.md 참고."
   )}}' 2>/dev/null || exit 0
