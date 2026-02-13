---
layout: doc
---

<script setup>
import { VPTeamMembers } from 'vitepress/theme'
import { withBase } from 'vitepress'

const googleScholar = {
  svg: '<svg role="img" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><title>Google Scholar</title><path d="M12 24a7 7 0 1 1 0-14 7 7 0 0 1 0 14zm0-24L0 9.5l4.838 3.94A8 8 0 0 1 12 9a8 8 0 0 1 7.162 4.44L24 9.5z"/></svg>'
}

const members = [
  {
    avatar: withBase('/team/ali_najafi.jpg'),
    name: 'Research Lead',
    title: 'Principal Investigator',
    links: [
        { icon: googleScholar, link: 'https://scholar.google.com/citations?user=c9QdS-sAAAAJ&hl=en&oi=ao' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/najafi-ali1998' },
      { icon: 'github', link: 'https://github.com/AliNajafi1998' },
    ]
  },
  {
    avatar: withBase('/team/Iannucci.jpeg'),
    name: 'Lead Developer',
    title: 'Core Maintainer',
    links: [
        { icon: googleScholar, link: 'https://scholar.google.com/citations?user=GtzlOnEAAAAJ&hl=en&oi=ao' },
      { icon: 'github', link: 'https://github.com/letiziaia' },

    ]
  },
  {
    avatar: withBase('/team/onur_varol.jpg'),
    name: 'Data Scientist',
    title: 'Algorithm Specialist',
    links: [
        { icon: googleScholar, link: 'https://scholar.google.com/citations?user=t8YAefAAAAAJ' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/onurvarol' },
      { icon: 'github', link: 'https://github.com/onurvarol'},
    ]
  },
  {
    avatar: withBase('/team/kivela.jpeg'),
    name: 'Contributor',
    title: 'Open Source Fellow',
    links: [
        { icon: googleScholar, link: 'https://scholar.google.com/citations?user=Z3913I0AAAAJ&hl=en&oi=ao' },
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
