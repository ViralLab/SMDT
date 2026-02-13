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

function cleanTitle(modName) {
  let parts = modName.split('.');

  // Remove 'smdt' prefix if present
  if (parts[0] === 'smdt') parts.shift();

  // Remove the group name if it's the first remaining part and there are more parts
  if (parts.length > 1) {
    parts.shift();
  }

  // Smart Deduplication
  const finalParts = [];
  for (const part of parts) {
    const last = finalParts[finalParts.length - 1];
    if (!last) {
      finalParts.push(part);
      continue;
    }
    
    // Case 1: Exact duplicate (gab -> gab)
    if (part === last) {
      continue;
    }

    // Case 2: Prefix duplicate (bluesky -> bluesky_api)
    // If the new part starts with the last part + '_', replace the last part
    if (part.startsWith(last + '_')) {
      finalParts.pop();
      finalParts.push(part);
      continue;
    }

    finalParts.push(part);
  }
  
  // Format to Title Case
  return finalParts
    .map(p => {
      return p
        .replace(/[_\-]+/g, ' ')
        .replace(/\b[a-z]/g, c => c.toUpperCase());
    })
    .join(' ');
}

const toTitle = (s) =>
  s
    .replace(/[_.-]+/g, ' ')
    .replace(/\b([a-z])/g, (m, c) => c.toUpperCase());

function groupFor(mod) {
  // mod like: smdt.enrichers.server.textgen.textgen
  const parts = mod.split('.');
  // Dynamic grouping based on second part of the module name (e.g. smdt.enrichers -> enrichers)
  if (parts.length > 1) {
    return parts[1];
  }
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
  // Match headers, allowing for escaped characters (like \_)
  const m = line.match(/^#\s+([A-Za-z0-9_.\\\\]+)\s*$/);
  if (m) {
    if (cur) sections.push(cur);
    // Unescape the captured name
    const rawName = m[1].replace(/\\/g, '');
    cur = { name: rawName, body: [line] };
  } else if (cur) {
    cur.body.push(line);
  }
}
if (cur) sections.push(cur);

// strip anchor-only lines and add page frontmatter + H1
for (const s of sections) {
  let body = s.body.join('\n').replace(/^\s*<a id="[^"]+"><\/a>\s*\n?/gm, '');
  // Ensure H1 stays first line (already there), but add frontmatter with a nice title
  const pageTitle = cleanTitle(s.name);
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

// Dynamically determine group order
const groupsOrder = Object.keys(grouped).sort();

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