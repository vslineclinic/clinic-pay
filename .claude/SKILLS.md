# Claude Code 스킬 구성

이 저장소는 Claude Code 스킬을 두 가지 방식으로 제공합니다. 두 방식 모두 저장소를
클론한 팀원과 Claude Code on the web 세션에 자동으로 적용됩니다.

## 1. 마켓플레이스 플러그인 (`settings.json` 선언)

`settings.json`의 `extraKnownMarketplaces` / `enabledPlugins`에 선언되어 있습니다.
플러그인 본체는 저장소에 포함되지 않고 각자의 머신에 내려받아지므로, 처음 세션을
열면 설치 확인 프롬프트가 뜨거나 아래 명령이 안내됩니다.

```
claude plugin install <plugin>@<marketplace> --scope project
```

| 분류 | 플러그인 | 마켓플레이스 (출처) | 스킬 수 |
| --- | --- | --- | --- |
| 문서·범용 | `example-skills` | `anthropic-agent-skills` (anthropics/skills) | 18 |
| 문서·범용 | `document-skills` | `anthropic-agent-skills` (anthropics/skills) | 18 |
| SEO | `claude-seo` | `agricidaniel-claude-seo` (AgriciDaniel/claude-seo) | 33 |
| SEO | `rampstack-seo` | `rampstack` (rampstackco/claude-skills) | 14 |
| 광고 | `claude-ads` | `ai-marketing-hub-claude-ads` (AgriciDaniel/claude-ads) | 34 |
| 마케팅 | `marketing-skills` | `marketingskills` (coreyhaines31/marketingskills) | 49 |
| 금융 | `financial-analysis` | `claude-for-financial-services` (anthropics/financial-services-plugins) | 13 |
| 금융 | `core` | `finance-skills` (JoelLewis/finance_skills) | 3 |

플러그인 스킬은 플러그인 이름으로 네임스페이스가 붙습니다 (예: `claude-seo:seo-audit`).

## 2. 저장소에 포함된 스킬 (`.claude/skills/`)

아래 출처들은 플러그인 마켓플레이스가 아닌 일반 스킬 저장소라 플러그인으로 설치할
수 없습니다. 그래서 `.claude/skills/<이름>/SKILL.md` 규약에 맞게 평탄화해 직접
포함시켰습니다. 총 180개이며 별도 설치 없이 바로 로드됩니다.

| 접두어 | 출처 | 스킬 수 |
| --- | --- | --- |
| `legal-contract-review` | evolsb/claude-legal-skill | 1 |
| `legal-*` | zubair-trabzada/ai-legal-claude | 13 |
| `corp-*` | w95/awesome-claude-corporate-skills | 166 |

`corp-*`는 원본 저장소의 14개 카테고리 디렉터리를 접두어로 옮긴 것입니다.

| 접두어 | 원본 카테고리 | 스킬 수 |
| --- | --- | --- |
| `corp-meta-` | 00-meta | 1 |
| `corp-exec-` | 01-executive-leadership | 12 |
| `corp-finance-` | 02-finance-accounting | 42 |
| `corp-hr-` | 03-human-resources | 9 |
| `corp-marketing-` | 04-marketing | 15 |
| `corp-sales-` | 05-sales | 16 |
| `corp-legal-` | 06-legal-compliance | 7 |
| `corp-ops-` | 07-operations | 11 |
| `corp-eng-` | 08-it-engineering | 14 |
| `corp-product-` | 09-product-management | 10 |
| `corp-data-` | 10-data-analytics | 9 |
| `corp-cs-` | 11-customer-success | 10 |
| `corp-procurement-` | 12-procurement-supply-chain | 6 |
| `corp-docs-` | 13-document-processing | 4 |

### 원본 대비 수정 사항

평탄화 과정에서 다음을 손댔습니다. 업스트림을 다시 가져올 때 반복해야 합니다.

- 각 `SKILL.md`의 frontmatter `name`을 새 디렉터리 이름과 일치시켰습니다.
  원본에는 카테고리가 다른데 이름이 같은 스킬이 있어 (`vendor-management`,
  `account-research`, `call-prep`, `prospect`) 그대로 두면 충돌합니다.
- w95 `02-finance-accounting`의 28개 스킬은 frontmatter 구분선(`---`)이 없고
  `description:`이 본문에 노출된 상태였습니다. 정상 frontmatter로 복구했습니다.
- zubair `skills/` 중 6개(`legal-review`, `legal-privacy`, `legal-compliance`,
  `legal-agreement`, `legal-freelancer`, `legal-report-pdf`)는 frontmatter가 아예
  없었습니다. 같은 저장소의 오케스트레이터 문서(`legal/SKILL.md`)에 있던 설명을
  기준으로 채웠습니다.
- evolsb 저장소는 파일명이 소문자 `skill.md`였습니다. `SKILL.md`로 바꿨습니다.
- zubair `legal/SKILL.md`(명령 라우팅용 문서)는 스킬이 아니라서 제외했습니다.

## 컨텍스트 비용

스킬 이름과 설명은 매 세션 시작 시 컨텍스트에 올라갑니다. 현재 구성은 포함된
스킬 180개 + 플러그인 스킬 약 138개로, 설명만 대략 25~30k 토큰입니다. 무거우면
`settings.json`의 `enabledPlugins`에서 값을 `false`로 바꿔 분류별로 끄거나,
쓰지 않는 `.claude/skills/` 디렉터리를 삭제하세요.

`document-skills`의 `docx` / `pdf` / `pptx` / `xlsx`와 `corp-docs-*`는 Claude Code
기본 제공 스킬과 기능이 겹칩니다.
