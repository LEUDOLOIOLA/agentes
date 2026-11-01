import { atom, read, update } from 'claude-code'
import type { Register, SessionRateLimit } from 'claude-code'

import type { Totals } from '../types'

const PANE = 'uso-claude'
const EMPTY: Totals = { turns: 0, input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }
// Change the tag when Totals' fields change: it declines what an earlier version stored.
const totals = atom({ plugin: 'uso-claude', key: 'totals' } as const, EMPTY, {
  shape: 'totals-1',
})
// Bumped on each measurement so the pane redraws with fresh figures.
const tick = atom({ plugin: 'uso-claude', key: 'tick' } as const, 0)

const LIMIT_NAMES: Record<string, string> = {
  five_hour: 'Limite 5h',
  seven_day: 'Limite 7 dias',
  spend_limit: 'Limite de gasto',
}

const fmt = (n: number) =>
  n >= 1_000_000 ? `${(n / 1_000_000).toFixed(1)}M` : n >= 1000 ? `${(n / 1000).toFixed(1)}k` : `${n}`

const bar = (percent: number, width = 20) => {
  const full = Math.round((Math.min(100, Math.max(0, percent)) / 100) * width)
  return '█'.repeat(full) + '░'.repeat(width - full)
}

const color = (percent: number) => (percent >= 90 ? 'red' : percent >= 70 ? 'yellow' : 'green')

const resetIn = (limit: SessionRateLimit, now: number) => {
  if (!limit.resetsAt) return ''
  const ms = Date.parse(limit.resetsAt) - now
  if (!(ms > 0)) return ' · renova agora'
  const h = Math.floor(ms / 3_600_000)
  const m = Math.floor((ms % 3_600_000) / 60_000)
  return h >= 24 ? ` · renova em ${Math.floor(h / 24)}d ${h % 24}h` : ` · renova em ${h}h ${m}min`
}

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    await $.command.register({
      name: 'uso',
      description: 'Mostra o uso do Claude (contexto, custo, tokens e limites) num painel',
    })
    void $.ui.open({ id: PANE, title: 'Uso do Claude' })

    return next(e)
  })

  on('command.run', { command: 'uso' }, async $ => {
    const opened = await $.ui.open({ id: PANE, title: 'Uso do Claude' })
    const { rateLimits, cost, context } = await $.session.usage()
    const t = await read($, totals)
    const now = await $.clock.now()
    const ctxPercent = context.percent ?? 0
    const lines = [
      'Uso do Claude',
      `Contexto       ${bar(ctxPercent)} ${ctxPercent}%  (${fmt(context.tokens ?? 0)} de ${fmt(context.window)} tokens)`,
      ...(rateLimits.length === 0
        ? ['Limites        sem leitura ainda (aparece após a próxima resposta)']
        : rateLimits.map(
            l =>
              `${(LIMIT_NAMES[l.kind] ?? l.kind).padEnd(15)}${bar(l.percentUsed)} ${l.percentUsed}%${resetIn(l, now)}`,
          )),
      `Custo          ${cost ? `US$ ${cost.usd.toFixed(4)}` : 'indisponível'}`,
      `Tokens         entrada ${fmt(t.input)} · saída ${fmt(t.output)} · cache lido ${fmt(t.cacheRead)} · cache escrito ${fmt(t.cacheWrite)} (${t.turns} turnos)`,
    ]
    if (!opened.isPlaced) lines.push('(o painel lateral não aparece neste app; use /uso para ver o uso)')

    return { text: lines.join('\n') }
  })

  on('turn.complete', async ($, e, next) => {
    const u = e.usage
    if (u) {
      await update($, totals, t => ({
        turns: t.turns + 1,
        input: t.input + u.input_tokens,
        output: t.output + u.output_tokens,
        cacheRead: t.cacheRead + u.cache_read_input_tokens,
        cacheWrite: t.cacheWrite + u.cache_creation_input_tokens,
      }))
    }

    return next(e)
  })

  on('session.measure', async ($, e, next) => {
    await update($, tick, n => n + 1)
    const high = e.rateLimits.find(l => l.percentUsed >= 90)
    if (high && e.changed.includes('rateLimits')) {
      await $.ui.toast(`${LIMIT_NAMES[high.kind] ?? high.kind}: ${high.percentUsed}% usado`)
    }
    const fiveHour = e.rateLimits.find(l => l.kind === 'five_hour')
    await $.ui.status(fiveHour ? `5h ${fiveHour.percentUsed}%` : undefined)

    return next(e)
  })

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const { Box, Text } = $.ui.resolve(e)
    await read($, tick)
    const t = await read($, totals)
    const { context, rateLimits, cost } = await $.session.usage()
    const now = await $.clock.now()
    const ctxPercent = context.percent ?? 0

    return (
      <Box flexDirection="column">
        <Text bold>Contexto</Text>
        <Text color={color(ctxPercent)}>
          {bar(ctxPercent)} {ctxPercent}%
        </Text>
        <Text dimColor>
          {fmt(context.tokens ?? 0)} de {fmt(context.window)} tokens
        </Text>

        <Text bold>Limites da assinatura</Text>
        {rateLimits.length === 0 && <Text dimColor>Sem leitura ainda (aparece após a primeira resposta).</Text>}
        {rateLimits.map(l => (
          <Box flexDirection="column">
            <Text>{LIMIT_NAMES[l.kind] ?? l.kind}</Text>
            <Text color={color(l.percentUsed)}>
              {bar(l.percentUsed)} {l.percentUsed}%
            </Text>
            <Text dimColor>{resetIn(l, now).replace(' · ', '')}</Text>
          </Box>
        ))}

        <Text bold>Custo da sessão</Text>
        <Text>{cost ? `US$ ${cost.usd.toFixed(4)}` : 'indisponível'}</Text>

        <Text bold>Tokens ({t.turns} turnos)</Text>
        <Text>Entrada: {fmt(t.input)} · Saída: {fmt(t.output)}</Text>
        <Text dimColor>
          Cache lido: {fmt(t.cacheRead)} · Cache escrito: {fmt(t.cacheWrite)}
        </Text>
      </Box>
    )
  })
}
