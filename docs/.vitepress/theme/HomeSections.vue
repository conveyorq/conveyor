<script setup lang="ts">
import { computed } from 'vue'
import { useData, withBase } from 'vitepress'
import FeatureMark from './FeatureMark.vue'

interface Item {
  title: string
  details: string
  link: string
  icon?: string
}

interface Band {
  kicker: string
  title: string
  details: string
  items: Item[]
}

interface Note {
  title: string
  details: string
  icon?: string
}

const { frontmatter } = useData()
const bands = computed(() => (frontmatter.value.bands ?? []) as Band[])
const note = computed(() => frontmatter.value.note as Note | undefined)

// A tile links to a page of this site or, for the SDKs, to the repository.
function href(link: string): string {
  return /^https?:\/\//.test(link) ? link : withBase(link)
}

// Each band is a nav target on the landing page, addressed by its kicker.
function anchor(kicker: string): string {
  return kicker.toLowerCase().replace(/\s+/g, '-')
}
</script>

<template>
  <div v-if="bands.length || note" class="cv-home">
    <section
      v-for="(band, b) in bands"
      :id="anchor(band.kicker)"
      :key="band.kicker"
      :class="['cv-band', b % 2 === 1 && 'cv-band--alt']"
      :aria-labelledby="`cv-band-${b}`"
    >
      <div class="cv-wrap">
        <header class="cv-head">
          <p class="cv-kicker">{{ band.kicker }}</p>
          <h2 :id="`cv-band-${b}`">{{ band.title }}</h2>
          <p class="cv-lede">{{ band.details }}</p>
        </header>
        <ul class="cv-bento">
          <li v-for="item in band.items" :key="item.title" class="cv-cell">
            <a class="cv-tile" :href="href(item.link)">
              <span class="cv-tile__head">
                <span class="cv-tile__icon">
                  <FeatureMark :name="item.icon ?? 'task'" />
                </span>
                <span class="cv-tile__title">{{ item.title }}</span>
              </span>
              <span class="cv-tile__details">{{ item.details }}</span>
              <span class="cv-go">Read <span aria-hidden="true">→</span></span>
            </a>
          </li>
        </ul>
      </div>
    </section>

    <section v-if="note" id="push-not-poll" class="cv-band" aria-labelledby="cv-note-title">
      <div class="cv-wrap">
        <article class="cv-tile cv-tile--static">
          <span class="cv-tile__head">
            <span class="cv-tile__icon">
              <FeatureMark :name="note.icon ?? 'push'" />
            </span>
            <h2 id="cv-note-title" class="cv-tile__title">{{ note.title }}</h2>
          </span>
          <p class="cv-tile__details">{{ note.details }}</p>
        </article>
      </div>
    </section>
  </div>
</template>

<style scoped>
.cv-home {
  padding-bottom: 8px;
}

/* Gutters live on the band, not the wrap, the same pattern as VitePress
   VPHero / VPFeatures, so cards share the hero's 1152px content edge. */
.cv-band {
  margin-top: 64px;
  padding: 0 24px;
}

@media (min-width: 640px) {
  .cv-band {
    padding-left: 48px;
    padding-right: 48px;
  }
}

@media (min-width: 960px) {
  .cv-band {
    padding-left: 64px;
    padding-right: 64px;
  }
}

.cv-band--alt {
  padding-top: 56px;
  padding-bottom: 56px;
  background: var(--vp-c-bg-alt);
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

.cv-bento {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
  margin: 0;
  padding: 0;
  list-style: none;
}

@media (min-width: 640px) {
  .cv-bento {
    grid-template-columns: 1fr 1fr;
  }
}

@media (min-width: 960px) {
  .cv-bento {
    grid-template-columns: repeat(3, 1fr);
  }
}

.cv-tile {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: hidden;
  /* Every tile is the same size: the grid stretches rows, and the fixed
     minimum keeps rows equal across bands whatever the copy length. */
  height: 100%;
  min-height: 176px;
  padding: 20px;
  text-decoration: none;
  color: inherit;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg);
  transition:
    border-color 0.2s ease,
    background-color 0.2s ease;
}

.cv-tile:hover {
  border-color: var(--vp-c-brand-1);
}

.cv-tile:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 2px;
}

.cv-tile__icon {
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

.cv-tile__head {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.cv-tile__title {
  font-size: 16px;
  font-weight: 600;
  line-height: 1.35;
  letter-spacing: -0.02em;
}

.cv-tile__details {
  font-size: 14px;
  font-weight: 400;
  line-height: 1.55;
  color: var(--vp-c-text-2);
}

.cv-tile--static {
  min-height: auto;
  pointer-events: none;
}

.cv-tile--static .cv-tile__title,
.cv-tile--static .cv-tile__details {
  margin: 0;
}

.cv-go {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-top: auto;
  padding-top: 8px;
  font-size: 13px;
  font-weight: 500;
  line-height: 1;
  color: var(--vp-c-brand-1);
}

.cv-go span {
  transition: transform 0.2s ease;
}

.cv-tile:hover .cv-go span {
  transform: translateX(4px);
}

@media (prefers-reduced-motion: reduce) {
  .cv-tile,
  .cv-go span {
    transition: none;
  }

  .cv-tile:hover .cv-go span {
    transform: none;
  }
}
</style>
