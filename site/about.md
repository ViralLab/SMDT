---
layout: doc
---

<script setup>
import { VPTeamMembers } from 'vitepress/theme'
import { withBase } from 'vitepress'

const googleScholar = {
  svg: '<svg fill="#000000" width="800px" height="800px" viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg"><path d="M14.573 2.729c-0.729 0.484-4.292 2.849-7.917 5.255s-6.589 4.396-6.589 4.422c0 0.026 0.182 0.146 0.406 0.266 0.224 0.13 3.797 2.109 7.953 4.411l7.542 4.193 0.193-0.099c0.109-0.052 2.891-1.641 6.188-3.521l5.99-3.427 0.036 10.599h3.557v-12.401l-4.615-3.094c-6.219-4.167-11.188-7.448-11.307-7.474-0.063-0.010-0.703 0.38-1.438 0.87zM7.141 22.177l0.016 2.672 8.828 5.292 8.891-5.339v-2.641c0-1.458-0.016-2.646-0.031-2.646-0.021 0-1.76 1.042-3.87 2.323l-4.406 2.661-0.578 0.339-1.755-1.052c-1.464-0.875-2.927-1.755-4.385-2.641l-2.672-1.615c-0.031-0.010-0.042 1.177-0.036 2.646z"/></svg>'
}

const website = {
  svg: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-globe" viewBox="0 0 16 16"> <path d="M0 8a8 8 0 1 1 16 0A8 8 0 0 1 0 8m7.5-6.923c-.67.204-1.335.82-1.887 1.855A8 8 0 0 0 5.145 4H7.5zM4.09 4a9.3 9.3 0 0 1 .64-1.539 7 7 0 0 1 .597-.933A7.03 7.03 0 0 0 2.255 4zm-.582 3.5c.03-.877.138-1.718.312-2.5H1.674a7 7 0 0 0-.656 2.5zM4.847 5a12.5 12.5 0 0 0-.338 2.5H7.5V5zM8.5 5v2.5h2.99a12.5 12.5 0 0 0-.337-2.5zM4.51 8.5a12.5 12.5 0 0 0 .337 2.5H7.5V8.5zm3.99 0V11h2.653c.187-.765.306-1.608.338-2.5zM5.145 12q.208.58.468 1.068c.552 1.035 1.218 1.65 1.887 1.855V12zm.182 2.472a7 7 0 0 1-.597-.933A9.3 9.3 0 0 1 4.09 12H2.255a7 7 0 0 0 3.072 2.472M3.82 11a13.7 13.7 0 0 1-.312-2.5h-2.49c.062.89.291 1.733.656 2.5zm6.853 3.472A7 7 0 0 0 13.745 12H11.91a9.3 9.3 0 0 1-.64 1.539 7 7 0 0 1-.597.933M8.5 12v2.923c.67-.204 1.335-.82 1.887-1.855q.26-.487.468-1.068zm3.68-1h2.146c.365-.767.594-1.61.656-2.5h-2.49a13.7 13.7 0 0 1-.312 2.5m2.802-3.5a7 7 0 0 0-.656-2.5H12.18c.174.782.282 1.623.312 2.5zM11.27 2.461c.247.464.462.98.64 1.539h1.835a7 7 0 0 0-3.072-2.472c.218.284.418.598.597.933M10.855 4a8 8 0 0 0-.468-1.068C9.835 1.897 9.17 1.282 8.5 1.077V4z"/></svg>',
}

const members = [
  {
    avatar: withBase('/team/ali_najafi.jpg'),
    name: 'Ali Najafi',
    title: 'PhD Student',
    org: 'Sabanci University',
    links: [
      { icon: googleScholar, link: 'https://scholar.google.com/citations?user=c9QdS-sAAAAJ&hl=en&oi=ao' },
      { icon: website, link: 'https://najafi-ali.com' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/najafi-ali1998' },
      { icon: 'github', link: 'https://github.com/AliNajafi1998' },
    ]
  },
  {
    avatar: withBase('/team/Iannucci.jpeg'),
    name: 'Letizia Iannucci',
    title: 'PhD Student',
    org: 'Aalto University',
    links: [
      { icon: googleScholar, link: 'https://scholar.google.com/citations?user=GtzlOnEAAAAJ&hl=en&oi=ao' },
      { icon: 'github', link: 'https://github.com/letiziaia' },
    ]
  },
  {
    avatar: withBase('/team/onur_varol.jpg'),
    name: 'Onur Varol',
    title: 'Assistant Professor',
    org: ' Sabanci University',
    links: [
      { icon: googleScholar, link: 'https://scholar.google.com/citations?user=t8YAefAAAAAJ' },
      { icon: website, link: 'https://www.onurvarol.com/' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/onurvarol' },
      { icon: 'github', link: 'https://github.com/onurvarol'},
    ]
  },
  {
    avatar: withBase('/team/kivela.jpeg'),
    name: 'Mikko Kivelä',
    title: 'Associate Professor',
    org: 'Aalto University',

    links: [
      { icon: googleScholar, link: 'https://scholar.google.com/citations?user=Z3913I0AAAAJ&hl=en&oi=ao' },
      { icon: website, link: 'http://www.mkivela.com/' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/mikko-kivel%C3%A4-3809551' },
    ]
  }
]
</script>

# About Us

SMDT (Social Media Data Toolkit) is developed by the Viral Lab.

## Our Mission

To provide researchers and developers with robust, unified tools for ingesting, enriching, and analyzing data from diverse social media platforms.

## The Team

<VPTeamMembers size="medium" :members="members" />

## Contact

For questions or support, please open an issue on our [GitHub repository](https://github.com/ViralLab/SMDT).
