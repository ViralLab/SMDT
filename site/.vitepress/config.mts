import { defineConfig } from 'vitepress'
import { apiSidebar } from './apiSidebar.mjs'

export default defineConfig({
  title: 'SMDT',
  description: 'Docs generated from source',
  base: '/SMDT/',                  // keep for GitHub Pages
  lastUpdated: true,
  cleanUrls: true,
  markdown: {
    html: true,
    headers: { level: [2, 3, 4] }, 
  },
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Examples', link: '/examples/' },
      { text: 'About Us', link: '/about' }
    ],

    sidebar: {
      '/api/': apiSidebar,
      '/examples/': [
        {
          text: 'Examples',
          items: [
            { text: 'Overview', link: '/examples/' },
            { text: 'Getting Started', link: '/examples/getting-started' }
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
 



