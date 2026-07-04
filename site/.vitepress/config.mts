import { defineConfig } from 'vitepress'
import { apiSidebar } from './apiSidebar.mjs'

export default defineConfig({
  title: 'SMDT',
  description: 'Docs generated from source',
  base: '/SMDT/',                  // keep for GitHub Pages
  appearance: 'dark',
  lastUpdated: true,
  cleanUrls: true,
  ignoreDeadLinks: true,
  head: [
    [
      'script',
      { async: '', src: 'https://www.googletagmanager.com/gtag/js?id=G-YB7W3XFS3Q' }
    ],
    [
      'script',
      {},
      `window.dataLayer = window.dataLayer || [];
      function gtag(){dataLayer.push(arguments);}
      gtag('js', new Date());
      gtag('config', 'G-YB7W3XFS3Q');`
    ]
  ],
  markdown: {
    html: true,
    headers: { level: [2, 3, 4] },
  },
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Install', link: '/installation/' },
      { text: 'Recipes', link: '/recipes/' },
      { text: 'About Us', link: '/about/' }
    ],

    sidebar: {
      '/api/': apiSidebar,
      '/recipes/': [
        {
          text: 'Start Here',
          items: [
            { text: 'Overview', link: '/recipes/' },
            { text: 'Getting Started', link: '/recipes/getting-started' },
          ]
        },
        {
          text: '1. Ingest & Verify Your Data',
          items: [
            { text: 'Using Ingestion Pipelines', link: '/recipes/using-pipelines' },
            { text: 'Standardizing Twitter API v2 Data', link: '/recipes/standardizing-twitter-v2' },
            { text: 'Using the Database Inspector', link: '/recipes/analysis/inspector' },
          ]
        },
        {
          text: '2. Enrich Your Data',
          items: [
            { text: 'NLP Enrichment with LLMs', link: '/recipes/enrichment/nlp' },
          ]
        },
        {
          text: '3. Protect & Share Your Data',
          items: [
            { text: 'Pseudonymization', link: '/recipes/pseudonymization' },
          ]
        },
        {
          text: '4. Analyze Your Data',
          items: [
            { text: 'Network Construction', link: '/recipes/networks/construction' },
            { text: 'Temporal Networks', link: '/recipes/networks/temporal' },
            { text: 'Cross-Platform Analysis (MultiStore)', link: '/recipes/analysis/multistore' },
          ]
        },
        {
          text: 'Advanced & Reference',
          items: [
            { text: 'Building a Custom Standardizer', link: '/recipes/building-custom-standardizer' },
            { text: 'Building a Custom Enricher', link: '/recipes/enrichment/building-custom-enricher' },
          ]
        },
      ],
      '/installation/': [
        {
          text: 'Installation',
          items: [
            { text: 'Guide', link: '/installation/' },
            { text: '1. Prerequisites & Database', link: '/installation/#_1-prerequisites-system-dependencies' },
            { text: '2. Project Installation', link: '/installation/#_2-project-installation' },
            { text: '3. Configuration', link: '/installation/#_3-configuration' },
            { text: '4. Verify Installation', link: '/installation/#_4-verify-installation' },
          ]
        }
      ]
    },

    search: { provider: 'local' },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/ViralLab/SMDT' }
    ]
  }
})




