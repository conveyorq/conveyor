/// <reference path="../env.d.ts" />
import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'
import './custom.css'
import Layout from './Layout.vue'

// The custom Layout adds the home page's diagram and feature bands; every
// other page is the default theme.
export default {
  extends: DefaultTheme,
  Layout,
} satisfies Theme
