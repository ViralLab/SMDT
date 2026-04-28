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
          text: 'Essentials',
          items: [
            { text: 'Overview', link: '/recipes/' },
            { text: 'Getting Started', link: '/recipes/getting-started' },
          ]
        },
        {
          text: 'Ingestion',
          items: [
            { text: 'Using Pipelines', link: '/recipes/using-pipelines' },
            { text: 'Standardizing Twitter v2', link: '/recipes/standardizing-twitter-v2' },
            { text: 'Custom Standardizers', link: '/recipes/building-custom-standardizer' },
          ]
        },
        {
          text: 'Enrichment',
          items: [
            { text: 'NLP Enrichment', link: '/recipes/enrichment/nlp' },
          ]
        },
        {
          text: 'Data Privacy',
          items: [
            { text: 'Pseudonymization', link: '/recipes/pseudonymization' },
          ]
        },
        {
          text: 'Analysis',
          items: [
            { text: 'Database Inspector', link: '/recipes/analysis/inspector' },
          ]
        },
        {
          text: 'Network Analysis',
          items: [
            { text: 'Network Construction', link: '/recipes/networks/construction' },
            { text: 'Temporal Networks', link: '/recipes/networks/temporal' },
          ]
        }
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




