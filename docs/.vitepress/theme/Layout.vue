<script setup lang="ts">
import DefaultTheme from 'vitepress/theme'
import { useData } from 'vitepress'
import FeatureMark from './FeatureMark.vue'
import HomeSections from './HomeSections.vue'
import diagram from '../../architecture.svg'

const { frontmatter } = useData()
</script>

<template>
  <DefaultTheme.Layout>
    <template #home-hero-after>
      <section
        v-if="frontmatter.intro"
        id="how-it-works"
        class="cv-intro"
        aria-labelledby="cv-intro-title"
      >
        <div class="cv-wrap">
          <header class="cv-head">
            <p class="cv-kicker">{{ frontmatter.intro.kicker }}</p>
            <h2 id="cv-intro-title">{{ frontmatter.intro.title }}</h2>
            <p class="cv-lede">{{ frontmatter.intro.details }}</p>
          </header>
          <img class="cv-intro__diagram" :src="diagram" :alt="frontmatter.intro.alt" />
          <ul v-if="frontmatter.intro.parts" class="cv-parts">
            <li v-for="part in frontmatter.intro.parts" :key="part.term" class="cv-part">
              <span class="cv-part__icon">
                <FeatureMark :name="part.icon ?? 'task'" />
              </span>
              <span class="cv-part__body">
                <span class="cv-part__term">{{ part.term }}</span>
                <span class="cv-part__details">{{ part.details }}</span>
              </span>
            </li>
          </ul>
        </div>
      </section>
    </template>
    <template #home-features-after>
      <HomeSections />
    </template>
  </DefaultTheme.Layout>
</template>

<style scoped>
/* Gutters live on the section, not the wrap, the same pattern as VitePress
   VPHero, so the diagram shares the hero's 1152px content edge. */
.cv-intro {
  padding: 0 24px;
}

@media (min-width: 640px) {
  .cv-intro {
    padding: 0 48px;
  }
}

@media (min-width: 960px) {
  .cv-intro {
    padding: 0 64px;
  }
}

.cv-wrap {
  margin: 0 auto;
  max-width: 1152px;
}

.cv-head {
  margin-bottom: 24px;
}

.cv-kicker {
  display: flex;
  align-items: center;
  gap: 16px;
  margin: 0 0 12px;
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--vp-c-brand-1);
}

.cv-kicker::after {
  content: '';
  flex: 1;
  height: 1px;
  background: var(--vp-c-divider);
}

.cv-head h2 {
  margin: 0 0 8px;
  font-size: 28px;
  font-weight: 600;
  letter-spacing: -0.03em;
  line-height: 1.2;
}

.cv-lede {
  margin: 0;
  max-width: 48em;
  font-size: 15px;
  line-height: 1.6;
  color: var(--vp-c-text-2);
}

/* The diagram carries its own white background; rounded corners and a hairline
   border keep it looking like a card in dark mode. */
.cv-intro__diagram {
  display: block;
  width: 100%;
  height: auto;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
}

/* The moving parts, one per column, in the same tile language as the bands. */
.cv-parts {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
  margin: 12px 0 0;
  padding: 0;
  list-style: none;
}

@media (min-width: 640px) {
  .cv-parts {
    grid-template-columns: 1fr 1fr;
  }
}

@media (min-width: 960px) {
  .cv-parts {
    grid-template-columns: repeat(4, 1fr);
  }
}

.cv-part {
  display: flex;
  gap: 12px;
  padding: 16px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg);
}

.cv-part__icon {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 36px;
  height: 36px;
  border-radius: 8px;
  color: var(--vp-c-brand-1);
  background: var(--vp-c-brand-soft);
}

.cv-part__body {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}

.cv-part__term {
  font-size: 15px;
  font-weight: 600;
  line-height: 1.35;
  letter-spacing: -0.02em;
}

.cv-part__details {
  font-size: 14px;
  line-height: 1.5;
  color: var(--vp-c-text-2);
}
</style>
