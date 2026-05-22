# OxideLog AntD Console Redesign Design

## Goal

Redesign the OxideLog web UI as an Ant Design / Ant Design Pro operations console where log search is the primary workflow, the home page acts as a NOC-style health overview, and the adaptive parser engine has its own secondary operations workspace.

## Confirmed Direction

The redesign is based on `ant-design/ant-design` conventions and the project’s existing `antd` + `@ant-design/pro-components` stack.

The confirmed information architecture is:

- **Primary workflow:** Log search and investigation.
- **Home page:** NOC-style operational health overview with stronger real-time trend, anomaly, and device-health emphasis.
- **Parser page:** Adaptive parser engine operations workspace for active/shadow/disabled rules, diagnostics, profiles, scope state, rollback, and quarantine visibility.
- **Management pages:** Source governance and archive assets remain management-style table pages.

## Current Context

The current OxideLog frontend lives under:

- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/LogSearchPanel.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/style.less`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts`

Problems to fix:

- `index.tsx` has grown into a large multi-responsibility page that mixes data loading, layout, modals, tables, renderer functions, and page-specific business logic.
- `LogSearchPanel.tsx` contains mojibake Chinese text and should be rewritten as a clean investigation workspace.
- The page uses Ant Design components, but the information hierarchy is uneven: cards, tables, alerts, and page sections compete for attention.
- Parser visibility exists but needs to feel like an operations console for the new adaptive engine rather than a generic table dump.

## UX Principles

The UI should feel like a professional operations and audit tool.

- Use a narrow icon rail + top header + light gray workspace shell as the default frame structure, matching the confirmed reference direction.
- Prefer dense, scan-friendly layouts over marketing composition.
- Use Ant Design defaults where possible: restrained blue primary color, standard status colors, predictable table and form controls.
- Avoid nested cards. Page sections should be full-width bands or simple layout containers; cards are for metrics, repeated items, modals, and bounded tools.
- Use compact controls for frequent workflows.
- Use icon buttons only where the symbol is standard and the action is obvious; otherwise use text buttons.
- Keep all Chinese interface text readable and consistent.
- Do not introduce a new UI framework.

## Information Architecture

### Navigation

Keep a custom shell because `config/config.ts` has `layout: false`.

Main navigation:

1. 运行态势
2. 日志检索
3. 设备来源
4. 准入控制
5. 解析器管理
6. 归档资产

The shell should follow the approved reference structure:

- A 72px collapsed icon rail is the default navigation surface.
- The rail shows the OxideLog mark, navigation icons, selected state, and a bottom health dot.
- A collapse/expand affordance sits near the rail top; expanded mode may reveal labels, but the compact mode must remain fully usable.
- A 56px top header shows the product name/subtitle on the left and utility/user actions on the right.
- Page content sits on a light gray workspace with white bordered panels and compact AntD-style controls.
- Main content must reclaim the width saved by the collapsed rail.

### 运行态势

The home page answers: “Is the system healthy right now?”

Content:

- Metric strip: today total logs, parse rate, partial/failed counts, worker errors, archive capacity.
- Real-time/NOC area: 24-hour trend chart, recent minute throughput, recent raw stream.
- Attention area: parser failures, admission pending cases, quarantined scopes, devices with no recent logs.
- Shortcuts into log search with prefilled filters for failed/partial logs and selected devices.

### 日志检索

This is the primary workspace.

Layout:

- Left filter rail on desktop, collapsible filter drawer/section on smaller screens.
- Main content contains result summary metrics, action bar, and unified result table.
- The table must prioritize investigation fields:
  - result source
  - ingest time
  - parse status
  - device/source
  - source IP/port
  - destination IP/port
  - protocol
  - action
  - region tags
  - raw log
  - archive path

Behavior:

- Keep hot, archive, and all-scope searches.
- Route export actions to the same 日志工作台 as export task creation; download format is selected later from the task panel.
- Keep calendar hot/cold day indicators.
- Make `include_failed=false` wording clear: it hides failed rows but keeps partial rows.
- Preserve `initialFilters` so home/device/parser pages can deep-link into investigations.

### 日志工作台内导出

The log search page owns export task creation and the export task panel.

Content:

- The user creates export tasks from the search results on the same page.
- Download format is selected later, when downloading a completed export file from the task panel.
- Short-range/small-result downloads can offer CSV, ZST, or Parquet.
- Long-range bulk downloads such as half-year or one-year log queries offer ZST or Parquet only.
- The export task panel shows queued, running, completed, and failed jobs with progress, cancel, retry, and download actions.
- Recent generated files and risk markers remain visible in the same working area.

### 解析器管理

This is the adaptive parser engine operations workspace.

Top overview:

- Active rules
- Shadow rules
- Disabled rules
- Quarantined scopes
- Diagnostics count
- Current checkpoint/version information if API exposes it later

Tabs:

- **规则:** adaptive rules table with status, confidence, wins/sample count, disabled reason, enable/disable actions.
- **诊断:** parser diagnostics grouped by fingerprint, scope, reason, sample raw, count, last seen.
- **Profiles:** parser profile success/partial/fail counts by scope and parser.
- **Scopes:** adaptive learning enabled, metrics gap, unknown bucket, quarantine, last seen.

The page should make rollback and quarantine visible without implying unsafe one-click automation. Enable/disable rule actions stay explicit.

### 来源治理

Merge device source management and admission control into one page because they represent one operational workflow: observed source -> admission decision -> managed device/trusted/blocked history.

Tabs:

- **设备与来源:** managed devices and observed sources in one table with clear tags.
- **待准入:** unknown sources waiting for approval/block/ignore.
- **信任历史:** trusted devices and approved sources.
- **阻断历史:** blocked sources with audit history.

Columns include object, ingest entry, type, parse health, last log time, admission state, risk, and actions.

Actions:

- Managed devices: edit, disable/enable, delete.
- Observed sources: approve, block, ignore, or convert to managed device.
- Blocked/trusted history: reopen when supported.

Important actions should use AntD modal confirmations/forms.

### 归档资产

Keep as storage management:

- Metric strip for parquet count, frozen count, total bytes.
- Archive assets table.
- Frozen index table with rebuild action.
- Search links should route users back to 日志检索 where possible; export links should open the export task panel with prefilled conditions.

## Component Architecture

Create smaller files under `src/pages/oxidelog/`:

- `index.tsx`: shell, data loading orchestration, page selection, common derived data.
- `types.ts`: local UI row types and page key types.
- `utils.tsx`: formatting helpers and reusable render helpers that return React nodes.
- `components/Shell.tsx`: sidebar, header, page container.
- `components/MetricCard.tsx`: compact metric card.
- `components/StatusTags.tsx`: parse status, admission state, protocol, adaptive rule status.
- `pages/OverviewPage.tsx`: NOC-style home.
- `pages/LogSearchPage.tsx`: rewritten log search workspace, replacing `LogSearchPanel.tsx` or wrapping the new implementation.
- `pages/SourceGovernancePage.tsx`: combined devices/source/admission governance.
- `pages/ParserPage.tsx`: adaptive parser workspace.
- `pages/AssetsPage.tsx`: archive assets.
- `style.less`: AntD console visual system and page-specific classes.

Keep service calls in `src/services/oxidelog.ts`. Do not change backend APIs unless implementation finds a hard missing field.

## Data Loading

The first implementation can keep the existing 30-second refresh loop and `Promise.all` loading strategy to avoid broad behavior changes.

The refactor should isolate derived data into memoized helpers:

- device row construction
- device lookup by source key
- hot/cold day set construction
- parser summary counts
- overview metric calculations

Errors should continue using AntD `message.error`.

## Testing Strategy

Frontend tests should focus on behavior and text, not snapshots.

Add tests for:

- utility functions: formatting, status classification, archive day extraction.
- `StatusTags`: partial status renders distinctly from parsed and failed.
- `LogSearchPage`: renders readable Chinese labels, exposes hot/archive/all search actions, keeps failed/partial wording correct.
- `ParserPage`: active/shadow/disabled/quarantine counts are computed and rendered.
- `Shell`: navigation changes selected page and exposes expected AntD menu labels.

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
npm test -- src/pages/oxidelog
npm run build
```

If the build is too slow during implementation, run `npm run tsc` and focused Jest tests first, then build before final handoff.

## Acceptance Criteria

- The app opens at `/oxidelog` and shows the redesigned AntD console.
- The default page is the NOC-style 运行态势 page.
- 日志检索 is the primary investigation workspace and all Chinese labels are readable.
- Parser management clearly shows adaptive rule lifecycle and diagnostics.
- Existing device, admission, archive, search, export, parser-rule enable/disable flows remain available.
- `index.tsx` no longer owns every page renderer and modal implementation.
- No new UI framework is added.
- No frontend change requires backend schema changes.
- The known backend/static `/umi.` assertion should be fixed only if the build output and static assets are regenerated as part of final frontend verification.

## Out Of Scope

- Dark theme.
- Custom charting beyond simple AntD/Pro-compatible visuals already available in dependencies.
- Backend API redesign.
- Authentication UI redesign.
- Replacing Ant Design Pro routing/layout configuration.
