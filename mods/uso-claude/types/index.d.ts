export type Totals = {
  turns: number
  input: number
  output: number
  cacheRead: number
  cacheWrite: number
}

declare module 'claude-code' {
  interface PluginState {
    'uso-claude': { totals: Shaped<Totals>; tick: number }
  }
}
