# Prospector Reports

Prospector analyses are good enough to be worth keeping, but today they live
and die inside the chat panel. This design gives a Prospector answer two
durable forms: a page in a project's wiki, and a PDF.

Status: design agreed, not yet implemented.

## Goals

- Ask Prospector to compose a standalone report from the current conversation.
- Save that report into a project, grouped under a `Reports` folder in the wiki.
- Download any wiki page as a PDF.

## Non-goals (v1)

- **Email delivery.** Drill has SMTP settings (`rest/smtp/SmtpConfig.java`,
  `/api/v1/smtp/config`) but no mail sender anywhere in the tree, and no
  `jakarta.mail` dependency. The config endpoint only opens a socket and does an
  EHLO handshake. Adding a real sender is its own piece of work; see
  [Later](#later).
- **Headless or scheduled report generation.** v1 renders the PDF in the
  browser, so a report requires a user in front of it.
- **Server-side PDF rendering.** See [Later](#later).

The data model is chosen so that neither non-goal requires a migration when it
lands: the report is stored as markdown, and the PDF is always a derived
artifact.

## Design

### 1. Generating the report

`QuickActionBar` already takes an `onAction: (prompt: string) => void` and its
buttons are literal prompt strings (see the `logAnalysisMode` block). A
**Generate Report** action is one more button of the same kind, carrying a
report-composition prompt: compose a standalone document from everything
analyzed in this conversation, leading with an executive summary, then key
actors, a timeline, findings as tables, indicators, and recommendations.

The reply streams back as an ordinary assistant message. No new endpoint, no
change to the streaming path, no new state. The prompt is a constant in the
frontend alongside the existing quick-action prompts.

There is a close precedent to follow: `ProjectWikiPage.tsx` already generates a
whole wiki page from the LLM, sending a long structured prompt through
`streamChat` with `context: { feature: 'wiki_generation' }` and writing the
result with `createWikiPage`. Report generation uses the same shape, tagged
`feature: 'report_generation'` so it is separable in the AI analytics
dashboard.

The report is markdown, and it will contain headings, tables, fenced code and
emoji. Every later stage has to survive all four.

### 2. Saving into a project

A **Save to project** action on assistant message bubbles
(`ChatMessageBubble`) opens a small modal with two fields:

- Project — defaults to the current project when Prospector is open inside one.
- Title — pre-filled from the markdown's first `#` heading.

On confirm it POSTs the raw markdown to the existing
`POST /api/v1/projects/{id}/wiki` with `folder` set to `Reports`.

Because the report is stored as markdown rather than as a rendered document, it
stays editable, searchable, diffable, and it already travels in
`ProjectExportBundle`.

### 3. The `folder` field

`WikiPage` is flat today: `id`, `title`, `content`, `order`, `createdAt`,
`updatedAt`. Grouping needs one optional field.

Add `folder` (nullable `String`) to `WikiPage` and `WikiPageRequest` in
`ProjectResources.java`, and to the `WikiPage` interface in `types/index.ts`.
Null or absent means the page sits at the root, so every page that exists today
is unaffected and no migration is needed.

`ProjectWikiPage.tsx` groups its page list by `folder`, rendering root-level
pages as it does now and each distinct folder as a collapsible group. This is a
generic wiki feature; reports are just its first user.

### 4. PDF download

A **Download PDF** button on the wiki page view, available for any wiki page.

It works through a `@media print` stylesheet plus `window.print()`. The browser
does the rendering, which means selectable and searchable text, working links,
page breaks that fall between blocks rather than through them, correct emoji and
table rendering, and a file measured in tens of kilobytes.

Wiki pages and Prospector messages both render markdown through the shared
`components/MarkdownView.tsx`, so one set of print rules covers both.

The print stylesheet hides the application shell — sidebar, header, tab bar,
action buttons — and prints the rendered markdown alone, with
`break-inside: avoid` on tables and code blocks. The button sets
`document.title` to the report title beforehand so the browser suggests a
sensible filename, then restores it.

The trade-off accepted here: the user passes through the browser's print dialog
and picks "Save as PDF" rather than getting an immediate download.

#### Why not the dashboard's exporter

`DashboardViewPage.tsx:560` already exports to PDF with `html2canvas` + `jsPDF`,
and reusing it would give a one-click download. It was rejected because it
screenshots the DOM: the entire report becomes an image, so the text is neither
selectable nor searchable, the file runs to several megabytes, and page breaks
slice through tables and paragraphs. That is acceptable for a dashboard, which
is a picture anyway, and poor for a multi-page text document.

Emitting PDF text primitives from the markdown AST with `jsPDF` was also
rejected: it means hand-writing a layout engine for headings, lists and tables,
and `jsPDF`'s built-in fonts have no emoji coverage.

## Files touched

| File | Change |
|---|---|
| `components/prospector/QuickActionBar.tsx` | Generate Report action and its prompt |
| `components/prospector/ChatMessageBubble.tsx` | Save to project action |
| `components/prospector/SaveReportModal.tsx` | New: project + title modal |
| `api/projects.ts` | `folder` on the wiki page create/update calls |
| `components/MarkdownView.tsx` | Print-safe class hooks on the rendered output |
| `types/index.ts` | `folder?: string` on `WikiPage` |
| `pages/ProjectWikiPage.tsx` | Group by folder; Download PDF button; print styles |
| `rest/ProjectResources.java` | `folder` on `WikiPage` and `WikiPageRequest` |
| `docs/dev/ui/pages/project-wiki.md` | Folders, reports, PDF download |

## Testing

- `ProjectResources` round-trips a wiki page with a folder, and a page saved
  without one reads back with a null folder.
- Wiki pages group correctly by folder, including the all-root case.
- Title extraction pulls the first `#` heading, and falls back to a timestamped
  default when the markdown has no heading.

The print stylesheet and `window.print()` are verified by hand; asserting on
browser print output is not worth the harness.

## Later

Both deferred pieces build on the stored markdown without changing it.

**Email.** Needs a real sender: `angus-mail` (Apache-licensed, roughly 700KB,
excluded from `jdbc-all`) driven by the existing `SmtpConfig`. Hand-rolling
SMTP AUTH, the STARTTLS upgrade and multipart MIME over the existing socket code
is possible but is protocol work with security-relevant edges, and the
dependency does it correctly.

**Headless generation.** A scheduled report needs the PDF produced on the
server, which means a markdown-to-HTML-to-PDF renderer and an embedded font with
emoji coverage. Because v1 stores markdown and treats the PDF as derived, that
renderer can be added as a new consumer of the same records.
