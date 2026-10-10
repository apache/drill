# Prospector Reports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a Prospector analysis be kept — generated as a report, saved into a project wiki under a `Reports` folder, and downloaded as a PDF.

**Architecture:** Report generation is a quick-action prompt through the existing `streamChat` path; no new backend. The report is stored as markdown on the existing `WikiPage` record, which gains one optional `folder` field. All report logic (detection, title, provenance, query appendix) lives in pure functions in `utils/report.ts` so it is unit-testable. The PDF is produced by the browser via a print stylesheet, never stored.

**Tech Stack:** React 18 + TypeScript + Ant Design 5, vitest, `react-markdown` via `components/MarkdownView.tsx`, Jersey/Jackson REST in `ProjectResources.java`.

**Spec:** [`docs/dev/ProspectorReports.md`](../ProspectorReports.md)

## Global Constraints

- Every new source file (`.java`, `.ts`, `.tsx`, `.css`) carries the Apache 2.0 license header. Copy the header verbatim from a neighbouring file in the same directory.
- After touching anything under `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. Common failures: `if` without braces, unused imports, missing license header.
- Checkstyle requires braces on every `if`.
- Do not add Claude as a git co-author. Commit messages are imperative ("Add …", not "Added …").
- Frontend commands run from `exec/java-exec/src/main/resources/webapp`.
- Typecheck with `npx tsc --noEmit`; test with `npx vitest run <path>`.
- No new npm or Maven dependencies. Everything needed is already installed.
- `ChatContext.feature` is a typed union — a new slug must be added to `constants/aiFeatures.ts` or `tsc` rejects it at the call site.
- Prose in UI copy and docs: no em-dashes, no "X, not Y" contrast phrasing.

---

### Task 1: Add the `folder` field to wiki pages

Grouping reports in the wiki needs one optional field, end to end. Null or absent means the page sits at the root, so existing pages are unaffected.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectResources.java:163-208` (the `WikiPage` class), `:400-410` (`WikiPageRequest`), `:1271-1284` (`createWikiPage`), and the `updateWikiPage` body below it
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/index.ts:311-318`
- Modify: `exec/java-exec/src/main/resources/webapp/src/api/projects.ts:185-212`

**Interfaces:**
- Consumes: nothing
- Produces: `WikiPage.folder?: string`; `createWikiPage(projectId, { title, content?, order?, folder? })`; `updateWikiPage(projectId, pageId, { title?, content?, order?, folder? })`

- [ ] **Step 1: Add the field to the Java model**

In `ProjectResources.java`, inside `public static class WikiPage`, add the field after `order`:

```java
    @JsonProperty
    private String folder;
```

Add the parameter to the `@JsonCreator` constructor, after `@JsonProperty("order") int order`:

```java
        @JsonProperty("folder") String folder,
```

and assign it in the body:

```java
      this.folder = folder;
```

Add the accessors next to the others:

```java
    public String getFolder() { return folder; }
    public void setFolder(String folder) { this.folder = folder; }
```

- [ ] **Step 2: Add the field to the request body**

In `public static class WikiPageRequest`, add:

```java
    @JsonProperty
    public String folder;
```

- [ ] **Step 3: Pass it through on create**

In `createWikiPage`, the `new WikiPage(...)` call currently passes six arguments. Add `request.folder` after the `order` argument so the call reads:

```java
      WikiPage page = new WikiPage(
          UUID.randomUUID().toString(),
          request.title.trim(),
          request.content != null ? request.content : "",
          request.order != null ? request.order : project.getWikiPages().size(),
          request.folder,
          now,
          now
      );
```

Move the `folder` parameter in the `@JsonCreator` constructor to sit between `order` and `createdAt` so the positional order matches.

- [ ] **Step 4: Pass it through on update**

In `updateWikiPage`, next to the existing `if (request.title != null) { ... }` style blocks, add:

```java
      if (request.folder != null) {
        page.setFolder(request.folder);
      }
```

Braces are required by checkstyle even for a one-line body.

- [ ] **Step 5: Run checkstyle**

Run: `mvn checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS

- [ ] **Step 6: Add the field to the TypeScript type**

In `types/index.ts`, in `export interface WikiPage`, after `order: number;`:

```ts
  /** Group this page belongs to in the wiki list. Absent means the page sits at the root. */
  folder?: string;
```

- [ ] **Step 7: Accept it in the API client**

In `api/projects.ts`, widen both signatures:

```ts
export async function createWikiPage(
  projectId: string,
  page: { title: string; content?: string; order?: number; folder?: string }
): Promise<WikiPage> {
```

```ts
export async function updateWikiPage(
  projectId: string,
  pageId: string,
  page: { title?: string; content?: string; order?: number; folder?: string }
): Promise<WikiPage> {
```

The bodies are unchanged; they already forward the whole object.

- [ ] **Step 8: Typecheck**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: no output

- [ ] **Step 9: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectResources.java \
        exec/java-exec/src/main/resources/webapp/src/types/index.ts \
        exec/java-exec/src/main/resources/webapp/src/api/projects.ts
git commit -m "Add an optional folder field to project wiki pages"
```

---

### Task 2: Report utilities

All the report logic that is worth testing, as pure functions. This is the one task with real TDD; everything after it is wiring.

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/report.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/report.test.ts`

**Interfaces:**
- Consumes: `ChatMessage` from `types/ai.ts`
- Produces:
  - `looksLikeReport(markdown: string): boolean`
  - `reportTitle(markdown: string, now?: Date): string`
  - `queryAppendix(messages: ChatMessage[]): string`
  - `provenanceFooter(p: ReportProvenance): string`
  - `buildReportMarkdown(content: string, messages: ChatMessage[], p: ReportProvenance): string`
  - `interface ReportProvenance { generatedAt: number; model?: string; provider?: string; conversationId?: string }`

- [ ] **Step 1: Write the failing tests**

Create `utils/report.test.ts`. Copy the Apache license header from `utils/sql.test.ts`, then:

```ts
import { describe, expect, it } from 'vitest';
import {
  looksLikeReport,
  reportTitle,
  queryAppendix,
  provenanceFooter,
  buildReportMarkdown,
} from './report';
import type { ChatMessage } from '../types/ai';

const REPORT = [
  '# Cyber Attack Analysis Report',
  '',
  '## Executive Summary',
  'A complete intrusion, start to finish.',
  '',
  '## Key Actors',
  '| Role | IP |',
  '| --- | --- |',
  '| Attacker | 98.114.205.102 |',
].join('\n');

describe('looksLikeReport', () => {
  it('accepts a headed document containing a table', () => {
    expect(looksLikeReport(REPORT)).toBe(true);
  });

  it('accepts a long headed document with no table', () => {
    const long = `# Findings\n\n## Detail\n\n${'word '.repeat(400)}`;
    expect(looksLikeReport(long)).toBe(true);
  });

  it('rejects a short answer with no headings', () => {
    expect(looksLikeReport('The table has 412 rows.')).toBe(false);
  });

  it('rejects a single-heading reply', () => {
    expect(looksLikeReport('# Result\n\nThree matching hosts.')).toBe(false);
  });

  it('rejects empty content', () => {
    expect(looksLikeReport('')).toBe(false);
  });
});

describe('reportTitle', () => {
  it('uses the first heading', () => {
    expect(reportTitle(REPORT)).toBe('Cyber Attack Analysis Report');
  });

  it('strips markdown emphasis from the heading', () => {
    expect(reportTitle('# **Q3** Review\n\ntext')).toBe('Q3 Review');
  });

  it('falls back to a timestamped title when there is no heading', () => {
    const now = new Date('2026-09-17T14:30:00Z');
    expect(reportTitle('just prose', now)).toBe('Prospector report 2026-09-17 14:30');
  });
});

describe('queryAppendix', () => {
  const withSql = (sql: string): ChatMessage => ({
    role: 'assistant',
    content: null,
    toolCalls: [{ id: '1', name: 'execute_sql', arguments: JSON.stringify({ sql }) }],
  });

  it('collects executed SQL into a fenced appendix', () => {
    const out = queryAppendix([withSql('SELECT 1')]);
    expect(out).toContain('## Appendix: queries run');
    expect(out).toContain('```sql\nSELECT 1\n```');
  });

  it('de-duplicates repeated statements', () => {
    const out = queryAppendix([withSql('SELECT 1'), withSql('SELECT 1')]);
    expect(out.match(/SELECT 1/g)).toHaveLength(1);
  });

  it('returns nothing when no queries ran', () => {
    expect(queryAppendix([{ role: 'assistant', content: 'hello' }])).toBe('');
  });

  it('skips tool calls whose arguments are not valid JSON', () => {
    const broken: ChatMessage = {
      role: 'assistant',
      content: null,
      toolCalls: [{ id: '1', name: 'execute_sql', arguments: '{not json' }],
    };
    expect(queryAppendix([broken])).toBe('');
  });
});

describe('provenanceFooter', () => {
  it('names the model, provider and time', () => {
    const out = provenanceFooter({
      generatedAt: Date.UTC(2026, 8, 17, 14, 30, 0),
      model: 'claude-opus-5',
      provider: 'anthropic',
      conversationId: 'tab-7',
    });
    expect(out).toContain('anthropic / claude-opus-5');
    expect(out).toContain('2026-09-17 14:30:00');
    expect(out).toContain('tab-7');
  });

  it('still renders when the model is unknown', () => {
    const out = provenanceFooter({ generatedAt: Date.UTC(2026, 8, 17) });
    expect(out).toContain('an unspecified model');
  });
});

describe('buildReportMarkdown', () => {
  it('puts the appendix and the footer after the report body', () => {
    const msgs: ChatMessage[] = [{
      role: 'assistant',
      content: null,
      toolCalls: [{ id: '1', name: 'execute_sql', arguments: JSON.stringify({ sql: 'SELECT 1' }) }],
    }];
    const out = buildReportMarkdown(REPORT, msgs, { generatedAt: Date.UTC(2026, 8, 17) });
    expect(out.indexOf('Executive Summary')).toBeLessThan(out.indexOf('Appendix: queries run'));
    expect(out.indexOf('Appendix: queries run')).toBeLessThan(out.indexOf('Generated by Prospector'));
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run src/utils/report.test.ts`
Expected: FAIL, "Failed to resolve import ./report"

- [ ] **Step 3: Write the implementation**

Create `utils/report.ts` with the Apache license header copied from `utils/sql.ts`, then:

```ts
import type { ChatMessage } from '../types/ai';

/** Where a saved report came from, recorded in its footer. */
export interface ReportProvenance {
  generatedAt: number;
  model?: string;
  provider?: string;
  conversationId?: string;
}

/** Minimum characters for a heading-bearing message with no table to count as a report. */
const LENGTH_THRESHOLD = 1500;

/**
 * Whether an assistant message reads like a report worth keeping.
 *
 * Deliberately conservative. A suggestion chip that fires on every long answer
 * teaches people to ignore it, and a missed report costs one menu click.
 */
export function looksLikeReport(markdown: string): boolean {
  if (!markdown) {
    return false;
  }
  const headings = markdown.match(/^#{1,3} /gm)?.length ?? 0;
  if (headings < 2) {
    return false;
  }
  return /^\|.*\|/m.test(markdown) || markdown.length >= LENGTH_THRESHOLD;
}

/** Title for a report: its first heading, else a timestamped default. */
export function reportTitle(markdown: string, now: Date = new Date()): string {
  const heading = markdown.match(/^#{1,3}\s+(.+)$/m);
  if (heading) {
    const cleaned = heading[1].replace(/[*_`]/g, '').trim();
    if (cleaned) {
      return cleaned.slice(0, 120);
    }
  }
  return `Prospector report ${now.toISOString().slice(0, 16).replace('T', ' ')}`;
}

/**
 * The SQL behind the findings, so a second reader can re-run it rather than
 * taking the report on faith. Empty when the conversation ran no queries.
 */
export function queryAppendix(messages: ChatMessage[]): string {
  const seen = new Set<string>();
  const statements: string[] = [];

  for (const message of messages) {
    for (const call of message.toolCalls ?? []) {
      if (call.name !== 'execute_sql') {
        continue;
      }
      let parsed: { sql?: string };
      try {
        parsed = JSON.parse(call.arguments) as { sql?: string };
      } catch {
        continue;
      }
      const sql = parsed.sql?.trim();
      if (!sql || seen.has(sql)) {
        continue;
      }
      seen.add(sql);
      statements.push(sql);
    }
  }

  if (statements.length === 0) {
    return '';
  }
  const blocks = statements.map((sql) => `\`\`\`sql\n${sql}\n\`\`\``).join('\n\n');
  return `\n\n## Appendix: queries run\n\n${blocks}\n`;
}

/**
 * Footer recording how the report was produced. Reports recommend operational
 * action, so a reader finding one months later needs to know it came from a
 * model, which one, and when.
 */
export function provenanceFooter(p: ReportProvenance): string {
  const when = new Date(p.generatedAt).toISOString().replace('T', ' ').slice(0, 19);
  const by = [p.provider, p.model].filter(Boolean).join(' / ') || 'an unspecified model';
  const source = p.conversationId ? ` Conversation \`${p.conversationId}\`.` : '';
  return `\n\n---\n\n*Generated by Prospector using ${by} on ${when} UTC.${source} `
    + 'Review its findings before acting on them.*\n';
}

/** The full markdown written to the wiki: body, then evidence, then provenance. */
export function buildReportMarkdown(
  content: string,
  messages: ChatMessage[],
  p: ReportProvenance
): string {
  return `${content.trim()}${queryAppendix(messages)}${provenanceFooter(p)}`;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run src/utils/report.test.ts`
Expected: PASS, 15 tests

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/utils/report.ts \
        exec/java-exec/src/main/resources/webapp/src/utils/report.test.ts
git commit -m "Add report detection, titling and markdown assembly utilities"
```

---

### Task 3: Group the wiki page list by folder

Reports land in a `Reports` folder, so the list has to render groups. This is a generic wiki feature; reports are its first user.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProjectWikiPage.tsx:95-100` (the `sortedPages` memo) and `:315-342` (the list render)
- Modify: `exec/java-exec/src/main/resources/webapp/src/styles/` — add rules next to the existing `.wiki-pagelist-*` rules (find the file with `grep -rl "wiki-pagelist-items" src/`)
- Modify: `docs/dev/ui/pages/project-wiki.md`

**Interfaces:**
- Consumes: `WikiPage.folder` from Task 1
- Produces: nothing other tasks depend on

- [ ] **Step 1: Group the sorted pages**

Alongside the existing `sortedPages` memo, add:

```tsx
  /** Root-level pages first, then each folder in alphabetical order. */
  const pageGroups = useMemo(() => {
    const root: WikiPage[] = [];
    const folders = new Map<string, WikiPage[]>();
    for (const page of sortedPages) {
      if (!page.folder) {
        root.push(page);
        continue;
      }
      const existing = folders.get(page.folder);
      if (existing) {
        existing.push(page);
      } else {
        folders.set(page.folder, [page]);
      }
    }
    return {
      root,
      folders: [...folders.entries()].sort(([a], [b]) => a.localeCompare(b)),
    };
  }, [sortedPages]);
```

- [ ] **Step 2: Render root pages and folder groups**

Extract the existing `<li>` body into a local renderer so it is not duplicated, directly above the `return`:

```tsx
  const renderPageItem = (page: WikiPage) => {
    const selected = selectedPage?.id === page.id;
    return (
      <li
        key={page.id}
        className={`wiki-pagelist-item${selected ? ' is-selected' : ''}`}
        onClick={() => navigate(`/projects/${projectId}/wiki/${page.id}`)}
      >
        <div className="wiki-pagelist-item-title">{page.title}</div>
        <div className="wiki-pagelist-item-preview">
          {previewFromMarkdown(page.content) || <em>No content</em>}
        </div>
        <div className="wiki-pagelist-item-meta">{formatRelative(page.updatedAt)}</div>
      </li>
    );
  };
```

Then replace the `<ul className="wiki-pagelist-items">…</ul>` block with:

```tsx
            <>
              {pageGroups.root.length > 0 && (
                <ul className="wiki-pagelist-items" role="list">
                  {pageGroups.root.map(renderPageItem)}
                </ul>
              )}
              {pageGroups.folders.map(([folder, pages]) => (
                <section key={folder} className="wiki-pagelist-group">
                  <h3 className="wiki-pagelist-group-title">{folder}</h3>
                  <ul className="wiki-pagelist-items" role="list">
                    {pages.map(renderPageItem)}
                  </ul>
                </section>
              ))}
            </>
```

- [ ] **Step 3: Style the group heading**

In the stylesheet holding `.wiki-pagelist-items`, add:

```css
.wiki-pagelist-group-title {
  margin: 12px 12px 4px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--text-tertiary, #8c8c8c);
}
```

If the file has no `--text-tertiary` token, use the colour the neighbouring `.wiki-pagelist-item-meta` rule uses.

- [ ] **Step 4: Typecheck and view the page**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: no output

Then `npm run dev`, open a project wiki, and confirm existing pages still render in a flat list (they all have no folder, so only the root list shows).

- [ ] **Step 5: Document it**

In `docs/dev/ui/pages/project-wiki.md`, add a short paragraph to the page-list section: pages with a `folder` render in a titled group below the ungrouped pages, folders sorted alphabetically, and pages without a folder sit at the root.

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src docs/dev/ui/pages/project-wiki.md
git commit -m "Group project wiki pages by folder"
```

---

### Task 4: Download a wiki page as PDF

The browser renders the PDF. This gives selectable text, real page breaks, working emoji and tables, and a small file, for a print stylesheet and one line of TypeScript.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProjectWikiPage.tsx` (header actions, near the "New page" button at `:301-311`)
- Modify: the stylesheet holding the `.wiki-*` rules
- Modify: `docs/dev/ui/pages/project-wiki.md`

**Interfaces:**
- Consumes: nothing
- Produces: nothing other tasks depend on

- [ ] **Step 1: Add the print handler**

In `ProjectWikiPage.tsx`, above the `return`:

```tsx
  /**
   * Print the rendered page. The browser's "Save as PDF" gives selectable text,
   * real page breaks and correct emoji, none of which a canvas screenshot does.
   * document.title drives the suggested filename, so it is set and restored.
   */
  const handlePrint = () => {
    if (!selectedPage) {
      return;
    }
    const previous = document.title;
    document.title = selectedPage.title;
    window.addEventListener('afterprint', () => { document.title = previous; }, { once: true });
    window.print();
  };
```

- [ ] **Step 2: Add the button**

Next to the "New page" button in the header `<Space>`, add:

```tsx
            <Tooltip title="Download as PDF">
              <Button
                size="small"
                icon={<FilePdfOutlined />}
                onClick={handlePrint}
                disabled={!selectedPage}
              />
            </Tooltip>
```

Add `FilePdfOutlined` to the existing `@ant-design/icons` import block.

- [ ] **Step 3: Mark the printable region**

Find the element wrapping the rendered `MarkdownView` for the selected page and add `className="wiki-printable"` to it, keeping any existing classes.

- [ ] **Step 4: Write the print stylesheet**

In the same stylesheet as Task 3, append:

```css
@media print {
  /* Print the page body alone: no shell, no chrome, no controls. */
  body * {
    visibility: hidden;
  }

  .wiki-printable,
  .wiki-printable * {
    visibility: visible;
  }

  .wiki-printable {
    position: absolute;
    left: 0;
    top: 0;
    width: 100%;
    padding: 0;
    overflow: visible;
  }

  /* Keep a table, a code block or a heading from being split across a page. */
  .wiki-printable table,
  .wiki-printable pre,
  .wiki-printable blockquote {
    break-inside: avoid;
  }

  .wiki-printable h1,
  .wiki-printable h2,
  .wiki-printable h3 {
    break-after: avoid;
  }

  .wiki-printable a::after {
    content: ' (' attr(href) ')';
    font-size: 0.85em;
    word-break: break-all;
  }
}
```

- [ ] **Step 5: Verify by hand**

Run `npm run dev`, open a wiki page with a table and a code block, click the PDF button, and in the print preview confirm: the sidebar and buttons are gone, the text is selectable in the saved PDF, tables are not split mid-row, and emoji render.

- [ ] **Step 6: Document it**

In `docs/dev/ui/pages/project-wiki.md`, note the PDF button, that it goes through the browser print dialog, and that the print stylesheet is why the output has real text rather than a screenshot.

- [ ] **Step 7: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src docs/dev/ui/pages/project-wiki.md
git commit -m "Add PDF download for project wiki pages"
```

---

### Task 5: Save a Prospector message as a report

The save path both the chip (Task 6) and the tool (Task 8) call into.

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/components/prospector/SaveReportModal.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/index.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/ProspectorPanel.tsx`

**Interfaces:**
- Consumes: `buildReportMarkdown`, `reportTitle` (Task 2); `createWikiPage`, `updateWikiPage` with `folder` (Task 1)
- Produces: `REPORTS_FOLDER = 'Reports'`; `<SaveReportModal open content messages defaultProjectId conversationId onClose />`

- [ ] **Step 1: Write the modal**

Create `SaveReportModal.tsx` with the Apache license header copied from `ProspectorSettingsModal.tsx`, then:

```tsx
import { useEffect, useState } from 'react';
import { Modal, Form, Input, Select, Radio, message } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { getProjects, createWikiPage, updateWikiPage, getProject } from '../../api/projects';
import { getAiStatus } from '../../api/ai';
import { buildReportMarkdown, reportTitle } from '../../utils/report';
import type { ChatMessage } from '../../types/ai';

/** Wiki folder every saved report goes into. */
export const REPORTS_FOLDER = 'Reports';

interface SaveReportModalProps {
  open: boolean;
  /** Markdown of the assistant message being saved. */
  content: string;
  /** Conversation so far, used to build the query appendix. */
  messages: ChatMessage[];
  defaultProjectId?: string;
  /** Recorded in the provenance footer so a saved report points back at its thread. */
  conversationId?: string;
  onClose: () => void;
}

export default function SaveReportModal({
  open,
  content,
  messages,
  defaultProjectId,
  conversationId,
  onClose,
}: SaveReportModalProps) {
  const [projectId, setProjectId] = useState<string | undefined>(defaultProjectId);
  const [title, setTitle] = useState('');
  const [mode, setMode] = useState<'new' | 'update'>('new');
  const [saving, setSaving] = useState(false);

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: open,
  });

  // The footer names the model that wrote the report, so it is read here rather
  // than threaded down through the panel.
  const { data: aiStatus } = useQuery({
    queryKey: ['aiStatus'],
    queryFn: getAiStatus,
    enabled: open,
  });

  // The chosen project's existing reports decide whether an update is on offer.
  const { data: project } = useQuery({
    queryKey: ['project', projectId],
    queryFn: () => getProject(projectId as string),
    enabled: open && Boolean(projectId),
  });

  useEffect(() => {
    if (open) {
      setProjectId(defaultProjectId);
      setTitle(reportTitle(content));
      setMode('new');
    }
  }, [open, content, defaultProjectId]);

  const existing = (project?.wikiPages ?? []).find(
    (page) => page.folder === REPORTS_FOLDER
      && page.title.trim().toLowerCase() === title.trim().toLowerCase()
  );

  // A title that no longer collides must not keep an Update selection alive.
  useEffect(() => {
    if (!existing) {
      setMode('new');
    }
  }, [existing]);

  const handleSave = async () => {
    if (!projectId || !title.trim()) {
      return;
    }
    setSaving(true);
    try {
      const markdown = buildReportMarkdown(content, messages, {
        generatedAt: Date.now(),
        model: aiStatus?.model,
        provider: aiStatus?.provider,
        conversationId,
      });
      if (mode === 'update' && existing) {
        await updateWikiPage(projectId, existing.id, { title: title.trim(), content: markdown });
        message.success('Report updated');
      } else {
        await createWikiPage(projectId, {
          title: title.trim(),
          content: markdown,
          folder: REPORTS_FOLDER,
        });
        message.success('Report saved');
      }
      onClose();
    } catch (e) {
      message.error(`Could not save the report: ${(e as Error).message}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      open={open}
      title="Save as report"
      onCancel={onClose}
      onOk={handleSave}
      okText="Save"
      okButtonProps={{ loading: saving, disabled: !projectId || !title.trim() }}
    >
      <Form layout="vertical">
        <Form.Item label="Project">
          <Select
            value={projectId}
            onChange={setProjectId}
            placeholder="Choose a project"
            options={(projects ?? []).map((p) => ({ value: p.id, label: p.name }))}
            showSearch
            optionFilterProp="label"
          />
        </Form.Item>

        <Form.Item label="Title">
          <Input value={title} onChange={(e) => setTitle(e.target.value)} />
        </Form.Item>

        {existing && (
          <Form.Item label="A report with this title already exists">
            <Radio.Group value={mode} onChange={(e) => setMode(e.target.value)}>
              <Radio value="update">Update it</Radio>
              <Radio value="new">Save as a new page</Radio>
            </Radio.Group>
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}
```

- [ ] **Step 2: Export it**

In `components/prospector/index.ts`, add:

```ts
export { default as SaveReportModal, REPORTS_FOLDER } from './SaveReportModal';
```

- [ ] **Step 3: Hold the modal state in the panel**

In `ProspectorPanel.tsx`, add state for the message being saved:

```tsx
  const [reportContent, setReportContent] = useState<string | null>(null);
```

Render the modal near the panel's other modals:

```tsx
      <SaveReportModal
        open={reportContent !== null}
        content={reportContent ?? ''}
        messages={messages}
        defaultProjectId={context.projectId}
        onClose={() => setReportContent(null)}
      />
```

`messages` is already destructured from `prospector` at the top of the component, and `context` is a required prop, so both are in scope. The modal reads the AI status itself, so nothing else needs threading.

- [ ] **Step 4: Typecheck**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: no output

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/components/prospector
git commit -m "Add a save-as-report modal for Prospector messages"
```

---

### Task 6: Suggest saving report-like messages

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/ChatMessageBubble.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/ChatMessageList.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/ProspectorPanel.tsx`
- Modify: the Prospector stylesheet (find it with `grep -rl "prospector-message-bubble" src/`)

**Interfaces:**
- Consumes: `looksLikeReport` (Task 2); the `reportContent` state from Task 5
- Produces: `ChatMessageBubbleProps.onSaveReport?: (content: string) => void`

- [ ] **Step 1: Take the callback in the bubble**

In `ChatMessageBubble.tsx`, extend the props interface:

```tsx
  /** Offer to save this message as a report. Omitted when saving is unavailable. */
  onSaveReport?: (content: string) => void;
```

Add `onSaveReport` to the destructured parameters.

- [ ] **Step 2: Render the chip**

Inside the bubble, after the `{isStreaming && <span className="prospector-cursor" />}` line:

```tsx
        {!isUser && !isStreaming && !isError && onSaveReport && message.content
          && looksLikeReport(message.content) && !dismissed && (
          <div className="prospector-report-suggestion">
            <span>This looks like a report.</span>
            <Button type="link" size="small" onClick={() => onSaveReport(message.content as string)}>
              Save to project
            </Button>
            <Button type="text" size="small" onClick={() => setDismissed(true)}>
              Dismiss
            </Button>
          </div>
        )}
```

Add `import { looksLikeReport } from '../../utils/report';` and `import { useState } from 'react';` if not already imported.

- [ ] **Step 3: Make dismissal stick**

A `useState` inside the bubble resets whenever the list re-renders the component, so dismissal has to live above it. In `ChatMessageList.tsx`, hold the set and pass it down:

```tsx
  const [dismissedReports, setDismissedReports] = useState<Set<number>>(new Set());
```

Key on the message's index in the conversation, since `ChatMessage` has no id. Pass `dismissed={dismissedReports.has(index)}` and `onDismissReport={() => setDismissedReports((prev) => new Set(prev).add(index))}` to each bubble, and replace the bubble's local `dismissed` state with those two props.

- [ ] **Step 4: Persist dismissal across reload**

`useProspector` already persists messages per tab through `saveChat`/`loadChat` in `localStorage`. Store the dismissed indices the same way, under `${storageKey}:dismissedReports`, reading them on mount and writing on change. Wrap both accesses in `try`/`catch`, matching how `saveChat` guards its own `localStorage` use.

- [ ] **Step 5: Wire the panel**

In `ProspectorPanel.tsx`, pass `onSaveReport={setReportContent}` down through `ChatMessageList` to the bubbles.

- [ ] **Step 6: Style the chip**

```css
.prospector-report-suggestion {
  display: flex;
  align-items: center;
  gap: 4px;
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px solid var(--border-subtle, #f0f0f0);
  font-size: 12px;
  color: var(--text-tertiary, #8c8c8c);
}
```

- [ ] **Step 7: Verify by hand**

Run `npm run dev`, ask Prospector for an analysis long enough to trip the heuristic, and confirm: the chip appears once streaming ends, Save opens the modal with the title pre-filled from the first heading, Dismiss hides it, and it stays hidden after a page reload. Confirm no chip appears under a one-line answer.

- [ ] **Step 8: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src
git commit -m "Suggest saving report-like Prospector messages"
```

---

### Task 7: Generate Report quick action

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/constants/aiFeatures.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/QuickActionBar.tsx`
- Modify: `docs/dev/ui/pages/ai-analytics.md`

**Interfaces:**
- Consumes: the existing `onAction(prompt: string)` contract
- Produces: the `report_generation` feature slug

- [ ] **Step 1: Add the feature slug**

In `constants/aiFeatures.ts`, in `FEATURE_LABEL`, next to `wiki_generation`:

```ts
  report_generation: 'Report generation',
```

- [ ] **Step 2: Add the quick action**

In `QuickActionBar.tsx`, add a button to the default (non-notebook) action set:

```tsx
          <Button
            size="small"
            icon={<FileTextOutlined />}
            onClick={() => onAction(
              'Write a standalone report covering everything analyzed in this conversation. '
              + 'Start with a markdown H1 title, then an executive summary a non-specialist can '
              + 'follow. Follow it with the key entities involved, a timeline of what happened, '
              + 'the findings as markdown tables where the data is tabular, any indicators worth '
              + 'recording, and concrete recommendations. Base every claim on query results from '
              + 'this conversation and say so plainly where the evidence is thin. The report must '
              + 'read on its own, so do not refer to "the above" or to our conversation.'
            )}
            disabled={disabled}
          >
            Generate Report
          </Button>
```

Add `FileTextOutlined` to the `@ant-design/icons` import.

- [ ] **Step 3: Tag the call**

`ProspectorPanel` receives `context: ChatContext` as a prop and passes it to `sendMessage`, so every quick action currently shares the caller's feature slug. Widen the callback rather than retagging the others.

In `QuickActionBar.tsx`, widen the prop:

```tsx
  onAction: (prompt: string, feature?: AiFeature) => void;
```

with `import type { AiFeature } from '../../constants/aiFeatures';`, and have the Generate Report button call `onAction(prompt, 'report_generation')`.

In `ProspectorPanel.tsx`, the handler passed as `onAction` applies the override:

```tsx
  const handleQuickAction = (prompt: string, feature?: AiFeature) => {
    sendMessage(prompt, feature ? { ...context, feature } : context);
  };
```

- [ ] **Step 4: Typecheck**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: no output. An unlabelled slug fails here, which is the point of the union.

- [ ] **Step 5: Document the slug**

`constants/aiFeatures.ts` says to keep the map in sync with the table in `docs/dev/ui/pages/ai-analytics.md`. Add a `report_generation` / `Report generation` row there.

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src docs/dev/ui/pages/ai-analytics.md
git commit -m "Add a Generate Report quick action to Prospector"
```

---

### Task 8: The `save_report` tool

Lets Prospector offer in conversation and act on a yes, the same way `save_query` does.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.ts:44-175` (tool definitions) and the tool `switch` around `:518`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java:752-790` (system prompt)
- Modify: `docs/dev/PROSPECTOR.md`, `docs/dev/ui/components/prospector.md`

**Interfaces:**
- Consumes: `buildReportMarkdown` (Task 2), `REPORTS_FOLDER` (Task 5), `createWikiPage` (Task 1)
- Produces: the `save_report` tool

- [ ] **Step 1: Declare the tool**

In `TOOL_DEFINITIONS`, after `save_query`:

```ts
  {
    name: 'save_report',
    description: 'Save a report you have written into the current project\'s wiki, under the '
      + 'Reports folder. Only call this after the user has agreed to save it.',
    parameters: {
      type: 'object',
      properties: {
        title: { type: 'string', description: 'Title for the report' },
        content: { type: 'string', description: 'The full report in markdown' },
      },
      required: ['title', 'content'],
    },
  },
```

- [ ] **Step 2: Handle the call**

In the tool `switch`, after `case 'save_query':`:

```ts
        case 'save_report': {
          if (!context?.projectId) {
            return JSON.stringify({ error: 'No active project — a report can only be saved '
              + 'inside a project. Ask the user to open one.' });
          }
          const markdown = buildReportMarkdown(
            args.content as string,
            messagesRef.current,
            { generatedAt: Date.now(), conversationId: tabId }
          );
          const page = await createWikiPage(context.projectId, {
            title: (args.title as string).trim(),
            content: markdown,
            folder: REPORTS_FOLDER,
          });
          return JSON.stringify({
            id: page.id,
            title: page.title,
            message: `Report saved to the project wiki under ${REPORTS_FOLDER}.`,
            viewPath: `/projects/${context.projectId}/wiki/${page.id}`,
          });
        }
```

`tabId` is a parameter of the hook and so is already in scope. `messages` is state, and naming it directly would churn `executeToolCall`'s dependency array on every token. Follow the pattern the hook already uses for `sendDataToAiRef` and add a ref beside `abortRef` at line 290:

```tsx
  // Read inside executeToolCall, which must not re-create itself on every token.
  const messagesRef = useRef<ChatMessage[]>(messages);
  messagesRef.current = messages;
```

- [ ] **Step 3: Tell Prospector to offer**

In `ProspectorResources.java`, in the `buildSystemPrompt` section, append:

```java
    systemPrompt.append("\nWhen you have produced a full report — a titled document with "
        + "sections, tables or a summary rather than a short answer — offer to save it to the "
        + "project. If the user agrees, call save_report with the complete markdown. Do not "
        + "call save_report without being asked or agreed to.\n");
```

Run `mvn checkstyle:check -pl exec/java-exec` afterwards.

- [ ] **Step 4: Verify by hand**

Run `npm run dev` inside a project, generate a report, and reply "yes please" to Prospector's offer. Confirm a page appears in the project wiki under Reports, with the provenance footer at the bottom. Then confirm the chip's Save path still works independently.

- [ ] **Step 5: Document it**

Add `save_report` to the tool list in `docs/dev/PROSPECTOR.md`, and describe the report flow end to end in `docs/dev/ui/components/prospector.md`: quick action, the chip, the tool, where reports land, and how to get the PDF.

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src \
        exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        docs/dev/PROSPECTOR.md docs/dev/ui/components/prospector.md
git commit -m "Let Prospector save a report with the save_report tool"
```

---

### Task 9: Mark the design implemented

- [ ] **Step 1: Update the status line**

In `docs/dev/ProspectorReports.md`, change `Status: design agreed, not yet implemented.` to `Status: implemented.` and link this plan.

- [ ] **Step 2: Update the index**

In `docs/dev/DevDocs.md`, drop "Design agreed, not yet implemented" from the Prospector reports entry and point it at `plans/2026-09-17-prospector-reports.md`.

- [ ] **Step 3: Run the full frontend check**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit && npx vitest run && npm run build`
Expected: no type errors, all tests pass, build succeeds

- [ ] **Step 4: Run checkstyle**

Run: `mvn checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add docs/dev/ProspectorReports.md docs/dev/DevDocs.md
git commit -m "Mark the Prospector reports design implemented"
```
