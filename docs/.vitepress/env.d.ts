declare module '*.css' {}

declare module '*.svg' {
  const src: string
  export default src
}

declare module '*.vue' {
  const component: import('vitepress/theme').default['Layout']
  export default component
}
