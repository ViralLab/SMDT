// scripts/split-api.mjs
// Split site/api/{README.md|index.md} into many pages,
// put them in logical folders, add frontmatter titles,
// and build a grouped sidebar file for VitePress.

import fs from 'node:fs';
import path from 'node:path';

const SRC = ['site/api/README.md', 'site/api/index.md'].find(fs.existsSync);
if (!SRC) {
  console.error('No API source markdown at site/api/README.md or site/api/index.md');
  process.exit(1);
}
const OUT_DIR = 'site/api';

// ---- helpers ---------------------------------------------------------------

const toTitle = (s) =>
  s
    .replace(/[_.-]+/g, ' ')
    .replace(/\b([a-z])/g, (m, c) => c.toUpperCase());

function groupFor(mod) {
  // mod like: smdt.enrichers.server.textgen.textgen
  const parts = mod.split('.');
  // smdt | enrichers | readers | standardizers | store | ingest | inspector | io | ...
  if (parts[1] === 'enrichers') return 'enrichers';
  if (parts[1] === 'standardizers') return 'standardizers';
  if (parts[1] === 'store') return 'store';
  if (parts[1] === 'ingest') return 'ingest';
  if (parts[1] === 'inspector') return 'inspector';
  if (parts[1] === 'io') return 'io';
  return 'misc';
}

function destPathFor(mod) {
  const grp = groupFor(mod);
  return path.join(OUT_DIR, grp, `${mod}.md`);
}

// ---- read & split ----------------------------------------------------------

let txt = fs.readFileSync(SRC, 'utf8');

// keep any frontmatter at top; rewrite the index later
let fm = '';
if (txt.startsWith('---\n')) {
  const i = txt.indexOf('\n---\n', 4);
  if (i !== -1) {
    fm = txt.slice(0, i + 5);
    txt = txt.slice(i + 5);
  }
}

// sections := [{name: 'smdt.xxx', body: '...'}]
const lines = txt.split('\n');
const sections = [];
let cur = null;

for (const line of lines) {
  const m = line.match(/^#\s+([A-Za-z0-9_.]+)\s*$/);
  if (m) {
    if (cur) sections.push(cur);
    cur = { name: m[1], body: [line] };
  } else if (cur) {
    cur.body.push(line);
  }
}
if (cur) sections.push(cur);

// strip anchor-only lines and add page frontmatter + H1
for (const s of sections) {
  let body = s.body.join('\n').replace(/^\s*<a id="[^"]+"><\/a>\s*\n?/gm, '');
  // Ensure H1 stays first line (already there), but add frontmatter with a nice title
  const pageTitle = toTitle(s.name.replace(/^smdt\./, ''));
  const front = `---\ntitle: ${pageTitle}\noutline: deep\n---\n\n`;
  s.text = pageTitle;
  s.body = front + body;
}

// ---- write files by group --------------------------------------------------

const written = [];
for (const s of sections) {
  const dest = destPathFor(s.name);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.writeFileSync(dest, s.body, 'utf8');
  written.push({ mod: s.name, text: s.text, link: `/api/${groupFor(s.name)}/${s.name}/` });
}

// ---- rewrite /api/ index ---------------------------------------------------

const grouped = written.reduce((acc, w) => {
  (acc[groupFor(w.mod)] ||= []).push(w);
  return acc;
}, {});

const groupsOrder = ['enrichers', 'standardizers', 'store', 'ingest', 'inspector', 'io', 'misc'];

let indexMd =
  (fm || '---\noutline: deep\n---\n') +
  '\n# API Reference\n\n';

for (const g of groupsOrder) {
  if (!grouped[g]?.length) continue;
  indexMd += `## ${toTitle(g)}\n\n`;
  for (const w of grouped[g]) indexMd += `- [${w.text}](${w.link})\n`;
  indexMd += '\n';
}

fs.writeFileSync(SRC, indexMd.trim() + '\n', 'utf8');

// ---- emit sidebar helper ---------------------------------------------------

const sidebar = groupsOrder
  .filter((g) => grouped[g]?.length)
  .map((g) => ({
    text: toTitle(g),
    collapsed: true,
    items: grouped[g]
      .map((w) => ({ text: w.text, link: w.link }))
      .sort((a, b) => a.text.localeCompare(b.text)),
  }));

fs.writeFileSync(
  'site/.vitepress/apiSidebar.mjs',
  `export const apiSidebar = ${JSON.stringify(
    [{ text: 'API Reference', link: '/api/' }, ...sidebar],
    null,
    2
  )};\n`,
  'utf8'
);

console.log(`Split ${written.length} modules into ${Object.keys(grouped).length} groups.`);