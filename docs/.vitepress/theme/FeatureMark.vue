<script setup lang="ts">
defineProps<{ name: string }>()
</script>

<template>
  <svg
    class="cv-mark"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="1.6"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
  >
    <!-- Task: a payload with a type line. -->
    <template v-if="name === 'task'">
      <rect x="5" y="5" width="14" height="14" rx="2" />
      <path d="M8 10h8M8 14h5" />
    </template>

    <!-- Worker: a handler with its concurrency ring. -->
    <template v-else-if="name === 'worker'">
      <circle cx="12" cy="12" r="4" />
      <circle cx="12" cy="12" r="8.5" opacity="0.35" />
    </template>

    <!-- CLI: a terminal prompt. -->
    <template v-else-if="name === 'cli'">
      <rect x="3.5" y="5" width="17" height="14" rx="2" />
      <path d="M7.5 10l2.5 2-2.5 2M12.5 14h4" />
    </template>

    <!-- HTTP: two endpoints, a dashed hop. -->
    <template v-else-if="name === 'http'">
      <circle cx="6" cy="12" r="2.4" />
      <circle cx="18" cy="12" r="2.4" />
      <path d="M9 12h6" stroke-dasharray="2.2 2.2" />
    </template>

    <!-- Webhook: a task pushed out. -->
    <template v-else-if="name === 'webhook'">
      <circle cx="6.5" cy="12" r="2.2" />
      <path d="M9.2 12h8.3M15.2 8.5 19.5 12l-4.3 3.5" />
    </template>

    <!-- SDK: one package. -->
    <template v-else-if="name === 'sdk'">
      <rect x="6" y="6" width="12" height="12" rx="2" />
      <path d="M9 12h6" />
    </template>

    <!-- Task dependencies: a parent over two children. -->
    <template v-else-if="name === 'workflow'">
      <circle cx="12" cy="6.5" r="2.2" />
      <circle cx="6.5" cy="17" r="2.2" />
      <circle cx="17.5" cy="17" r="2.2" />
      <path d="M12 8.7v2.3L7.8 15M12 11l4.2 4" />
    </template>

    <!-- Group aggregation: stacked tasks. -->
    <template v-else-if="name === 'group'">
      <rect x="6" y="8" width="12" height="10" rx="2" />
      <path d="M9 8V6.5A1.5 1.5 0 0 1 10.5 5h3A1.5 1.5 0 0 1 15 6.5V8" />
    </template>

    <!-- Rate limiting: a gauge. -->
    <template v-else-if="name === 'ratelimit'">
      <path d="M5 15.5a7 7 0 0 1 14 0" />
      <path d="M12 15.5l3.5-4" />
      <circle cx="12" cy="15.5" r="1.2" fill="currentColor" stroke="none" />
    </template>

    <!-- Concurrency: a node with its slots. -->
    <template v-else-if="name === 'concurrency'">
      <path d="M12 3.5 19 7.5v9L12 20.5 5 16.5v-9L12 3.5Z" />
      <circle cx="12" cy="12" r="2" />
    </template>

    <!-- Encryption: a lock. -->
    <template v-else-if="name === 'encryption'">
      <rect x="5" y="7" width="14" height="11" rx="2" />
      <path d="M8.5 7V6a3.5 3.5 0 0 1 7 0v1" />
    </template>

    <!-- Expiring tasks: a clock. -->
    <template v-else-if="name === 'expiring'">
      <circle cx="12" cy="12" r="8" />
      <path d="M12 7.5V12l3 2" />
    </template>

    <!-- Events: a broadcast. -->
    <template v-else-if="name === 'events'">
      <circle cx="12" cy="12" r="2.2" />
      <path d="M8.5 8.5a5 5 0 0 0 0 7M15.5 8.5a5 5 0 0 1 0 7" opacity="0.6" />
      <path d="M6 6a8.5 8.5 0 0 0 0 12M18 6a8.5 8.5 0 0 1 0 12" opacity="0.35" />
    </template>

    <!-- Operations: a grid with a chosen cell. -->
    <template v-else-if="name === 'operations'">
      <rect x="4.5" y="4.5" width="15" height="15" rx="2" />
      <path d="M4.5 12h15M12 4.5v15" opacity="0.45" />
      <circle cx="15.75" cy="15.75" r="1.6" fill="currentColor" stroke="none" />
    </template>

    <!-- High availability: three connected nodes. -->
    <template v-else-if="name === 'ha'">
      <circle cx="6" cy="7.5" r="2.1" />
      <circle cx="18" cy="7.5" r="2.1" />
      <circle cx="12" cy="17" r="2.1" />
      <path d="M8.1 7.5h7.8M7.7 9.3 10.6 15M16.3 9.3 13.4 15" />
    </template>

    <!-- Architecture: layers. -->
    <template v-else-if="name === 'architecture'">
      <path d="M12 4.5 19.5 8.5 12 12.5 4.5 8.5 12 4.5Z" />
      <path d="M4.5 12.5 12 16.5l7.5-4M4.5 16 12 20l7.5-4" opacity="0.45" />
    </template>

    <!-- Protocol: a document. -->
    <template v-else-if="name === 'protocol'">
      <path d="M7 4.5h7l4 4V19.5H7z" />
      <path d="M14 4.5v4h4M9.5 13h5M9.5 16h5" />
    </template>

    <!-- Migrate: a task moving between systems. -->
    <template v-else-if="name === 'migrate'">
      <circle cx="6" cy="12" r="2.2" opacity="0.4" />
      <circle cx="18" cy="12" r="2.2" />
      <path d="M8.4 12h5.4M12 9l3 3-3 3" />
    </template>

    <!-- Broker: a database. -->
    <template v-else-if="name === 'broker'">
      <ellipse cx="12" cy="6.5" rx="7" ry="2.5" />
      <path d="M5 6.5v11c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5v-11" />
      <path d="M5 12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5" opacity="0.5" />
    </template>

    <!-- Push: a bolt. -->
    <template v-else-if="name === 'push'">
      <path d="M13 3.5 6 13.5h5l-1 7 7-10h-5l1-7Z" />
    </template>

    <template v-else>
      <rect x="5" y="5" width="14" height="14" rx="2" />
      <path d="M8 10h8M8 14h5" />
    </template>
  </svg>
</template>

<style scoped>
.cv-mark {
  display: block;
  width: 22px;
  height: 22px;
}
</style>
