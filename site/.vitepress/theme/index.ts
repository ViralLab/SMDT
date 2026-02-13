// https://vitepress.dev/guide/custom-theme
import { h, onMounted, watch, nextTick } from 'vue'
import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'
import { useRoute } from 'vitepress'
import './style.css'

export default {
  extends: DefaultTheme,
  Layout: () => {
    return h(DefaultTheme.Layout, null, {
      // https://vitepress.dev/guide/extending-default-theme#layout-slots
    })
  },
  setup() {
    const route = useRoute()
    
    onMounted(() => {
      initTypewriter()
    })

    watch(
      () => route.path,
      () => nextTick(() => initTypewriter())
    )

    function initTypewriter() {
      const element = document.getElementById('hero-typewriter')
      if (!element) return

      // If already typed (from navigating back), reset or keep? 
      // Resetting is better for "landing page experience"
      const text = element.innerText || element.textContent // It has "SocialMedia..." inside but hidden
      if (!text) return
      
      // Store original text if not already stored, to handle multiple re-runs cleanly
      if (!element.dataset.originalText) {
        element.dataset.originalText = text
      }
      const fullText = element.dataset.originalText

      element.innerText = ''
      element.style.opacity = '1'
      
      let i = 0
      const typeSpeed = 50
      const waitTime = 5000

      function loop() {
        if (!document.body.contains(element)) return

        if (i <= fullText.length) {
          element.innerText = fullText.substring(0, i)
          i++
          setTimeout(loop, typeSpeed)
        } else {
          // Finished typing, wait then restart from scratch
          setTimeout(() => {
            i = 0
            element.innerText = ''
            loop()
          }, waitTime)
        }
      }
      
      loop()
    }
  },
  enhanceApp({ app, router, siteData }) {
    // ...
  }
} satisfies Theme
