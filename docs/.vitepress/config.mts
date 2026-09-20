import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

const repo = 'https://github.com/conveyorq/conveyor'

// The site is served from the repository's GitHub Pages project URL.
const site = 'https://conveyorq.github.io/conveyor/'

// Where a repository-relative link resolves on GitHub. GitHub serves a
// directory path through the same route by redirecting to its tree view.
const repoFileURL = `${repo}/blob/main/`

// Links in the guides that leave docs/ (the README, the SDK READMEs, source
// directories) have no page on this site. They are rewritten to GitHub at
// build time so the Markdown stays readable both in the repository and here.
const parentDirPrefix = '../'

const config = defineConfig({
  title: 'Conveyor',
  description:
    'A distributed, push-based task queue with Go, TypeScript, and Python SDKs: persistent tasks with at-least-once execution, retries with backoff, scheduling, and priorities, backed by Postgres or an in-memory broker, with no Redis and no polling.',
  base: '/conveyor/',
  cleanUrls: true,
  lastUpdated: true,
  sitemap: { hostname: site },
  // The dependency license inventory stays in the repository but is not a page here.
  srcExclude: ['licenses.md'],
  head: [
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap',
      },
    ],
    // Point VitePress's font variables at Geist. `!important` on the custom
    // property definitions makes them win over the theme defaults regardless
    // of stylesheet order.
    [
      'style',
      {},
      ':root{--vp-font-family-base:"Geist",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif!important;--vp-font-family-mono:"Geist Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace!important}',
    ],
  ],

  markdown: {
    config(md) {
      const renderLinkOpen =
        md.renderer.rules.link_open ??
        ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options))

      md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
        const href = tokens[idx].attrGet('href')

        if (href?.startsWith(parentDirPrefix)) {
          tokens[idx].attrSet('href', repoFileURL + href.slice(parentDirPrefix.length))
        }

        return renderLinkOpen(tokens, idx, options, env, self)
      }
    },
  },

  themeConfig: {
    // The nav bar carries the one entry point; GitHub is the social link.
    nav: [{ text: 'Guide', link: '/install' }],

    sidebar: [
      {
        text: '🚀 Guide',
        items: [
          { text: 'Installation', link: '/install' },
          { text: 'Concepts', link: '/concepts' },
          { text: 'Use cases', link: '/use-cases' },
          { text: 'Usage guide', link: '/usage' },
        ],
      },
      {
        text: '🔌 Interfaces',
        items: [
          { text: 'CLI reference', link: '/cli' },
          { text: 'Dashboard', link: '/dashboard' },
          { text: 'HTTP API', link: '/http-api' },
          { text: 'Webhook workers', link: '/webhook-workers' },
          { text: 'Embedded mode', link: '/embedded' },
        ],
      },
      {
        text: '⚙️ Features',
        items: [
          { text: 'Task dependencies', link: '/workflows' },
          { text: 'Group aggregation', link: '/grouping' },
          { text: 'Rate limiting', link: '/rate-limiting' },
          { text: 'Concurrency limits', link: '/concurrency' },
          { text: 'End-to-end encryption', link: '/encryption' },
          { text: 'Expiring tasks', link: '/expiring-jobs' },
          { text: 'Lifecycle events', link: '/events' },
        ],
      },
      {
        text: '🛠️ Operations',
        items: [
          { text: 'Operations guide', link: '/operations' },
          { text: 'High availability', link: '/high-availability' },
          { text: 'Architecture', link: '/architecture' },
        ],
      },
      {
        text: '📦 SDKs',
        items: [
          { text: 'Go', link: `${repoFileURL}sdks/go/README.md` },
          { text: 'TypeScript', link: `${repoFileURL}sdks/typescript/README.md` },
          { text: 'Python', link: `${repoFileURL}sdks/python/README.md` },
        ],
      },
      {
        text: '🔄 Migrate',
        items: [
          { text: 'How it compares', link: '/comparison' },
          { text: 'From asynq', link: '/migrate-from-asynq' },
          { text: 'From River', link: '/migrate-from-river' },
        ],
      },
      {
        text: '📖 Reference',
        items: [{ text: 'Wire protocol', link: '/protocol' }],
      },
    ],

    socialLinks: [{ icon: 'github', link: repo }],
    search: { provider: 'local' },
    outline: { level: [2, 3] },
    editLink: {
      pattern: `${repo}/edit/main/docs/:path`,
      text: 'Edit this page on GitHub',
    },
    footer: { message: 'Released under the Apache-2.0 License.' },
  },
})

// The guides draw their diagrams in ```mermaid fences, which GitHub renders
// natively; the plugin renders them in the browser here.
const siteConfig = withMermaid({
  ...config,
  mermaidPlugin: { class: 'mermaid' },
})

// Under pnpm's nested node_modules, Vite's dev server would serve Mermaid's
// CommonJS dependencies (dayjs and friends) unbundled and the browser fails on
// their missing default export, leaving a blank page. Pre-bundling Mermaid
// itself resolves the interop the way the production build already does. The
// plugin lists those deep dependencies by name instead, which the docs root
// cannot resolve under pnpm, so the list is replaced rather than extended.
siteConfig.vite = {
  ...siteConfig.vite,
  optimizeDeps: { ...siteConfig.vite?.optimizeDeps, include: ['mermaid'] },
}

export default siteConfig
