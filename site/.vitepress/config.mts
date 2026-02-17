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
      { text: 'About Us', link: '/about' }
    ],

    sidebar: {
      '/api/': apiSidebar,
      '/recipes/': [
        {
          text: 'Recipes',
          items: [
            { text: 'Overview', link: '/recipes/' },
            { text: 'Getting Started', link: '/recipes/getting-started' },
            { text: 'Standardizing Twitter v2', link: '/recipes/standardizing-twitter-v2' },
            { text: 'Temporal Networks', link: '/recipes/temporal-networks' },
            { text: 'Building a Custom Standardizer', link: '/recipes/building-custom-standardizer' },
            { text: 'NLP Enrichment with LLMs', link: '/recipes/nlp-enrichment' }
          ]
        }
      ],
      '/installation/': [
        {
          text: 'Installation',
          items: [
            { text: 'Guide', link: '/installation/' },
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
 



