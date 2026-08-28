import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const docsDirectory = dirname(dirname(fileURLToPath(import.meta.url)));
const outputDirectory = join(docsDirectory, 'dist');
const siteOrigin = 'https://streamfusion.invalid';
const siteBase = '/StreamFusion/';
const failures = [];

for (const entry of readdirSync(outputDirectory, { recursive: true, withFileTypes: true })) {
  if (!entry.isFile() || !entry.name.endsWith('.html')) continue;

  const file = join(entry.parentPath, entry.name);
  const outputPath = relative(outputDirectory, file).replaceAll('\\', '/');
  const pagePath = outputPath.endsWith('/index.html')
    ? outputPath.slice(0, -'index.html'.length)
    : outputPath === 'index.html'
      ? ''
      : outputPath;
  const pageUrl = new URL(siteBase + pagePath, siteOrigin);
  const html = readFileSync(file, 'utf8');

  for (const [, href] of html.matchAll(/href="([^"]+)"/g)) {
    if (href.startsWith('#') || /^(?:https?:|mailto:|data:)/.test(href)) continue;

    const target = new URL(href, pageUrl);
    if (target.origin !== siteOrigin) continue;
    if (!target.pathname.startsWith(siteBase)) {
      failures.push(`${outputPath}: ${href} escapes ${siteBase}`);
      continue;
    }

    const targetPath = decodeURIComponent(target.pathname.slice(siteBase.length));
    if (!existsSync(join(outputDirectory, targetPath))
        && !existsSync(join(outputDirectory, targetPath, 'index.html'))) {
      failures.push(`${outputPath}: ${href} -> ${target.pathname}`);
    }
  }
}

if (failures.length > 0) {
  throw new Error(`Broken internal links:\n${failures.join('\n')}`);
}

console.log('All generated internal links resolve.');
