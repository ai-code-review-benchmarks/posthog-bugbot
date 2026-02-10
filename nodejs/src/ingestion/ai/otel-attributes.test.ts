import { PluginEvent } from '@posthog/plugin-scaffold'

import { mapOtelAttributes } from './otel-attributes'

const createEvent = (properties: Record<string, unknown>): PluginEvent => ({
    event: '$ai_generation',
    distinct_id: 'user-123',
    team_id: 1,
    properties,
    uuid: 'test-uuid',
    timestamp: new Date().toISOString(),
    ip: '127.0.0.1',
    site_url: 'https://app.posthog.com',
    now: new Date().toISOString(),
})

describe('mapOtelAttributes', () => {
    it.each([
        ['non-otel event (no source)', {}],
        ['non-otel event (different source)', { $ai_ingestion_source: 'sdk' }],
    ])('passes through %s unchanged', (_label, properties) => {
        const event = createEvent(properties)
        const original = structuredClone(event)
        mapOtelAttributes(event)
        expect(event).toEqual(original)
    })

    it.each([
        ['gen_ai.input.messages', '$ai_input'],
        ['gen_ai.output.messages', '$ai_output_choices'],
        ['gen_ai.usage.input_tokens', '$ai_input_tokens'],
        ['gen_ai.usage.output_tokens', '$ai_output_tokens'],
        ['gen_ai.request.model', '$ai_model'],
        ['gen_ai.provider.name', '$ai_provider'],
    ])('renames %s to %s', (otelKey, phKey) => {
        const event = createEvent({ $ai_ingestion_source: 'otel', [otelKey]: 'test-value' })
        mapOtelAttributes(event)
        expect(event.properties![phKey]).toBe('test-value')
        expect(event.properties![otelKey]).toBeUndefined()
    })

    it('JSON-parses string values for $ai_input and $ai_output_choices', () => {
        const event = createEvent({
            $ai_ingestion_source: 'otel',
            'gen_ai.input.messages': '[{"role": "user", "content": "Hello"}]',
            'gen_ai.output.messages': '[{"role": "assistant", "content": "Hi"}]',
        })
        mapOtelAttributes(event)
        expect(event.properties!.$ai_input).toEqual([{ role: 'user', content: 'Hello' }])
        expect(event.properties!.$ai_output_choices).toEqual([{ role: 'assistant', content: 'Hi' }])
    })

    it('keeps original string when JSON parsing fails', () => {
        const event = createEvent({
            $ai_ingestion_source: 'otel',
            'gen_ai.input.messages': 'not valid json',
        })
        mapOtelAttributes(event)
        expect(event.properties!.$ai_input).toBe('not valid json')
    })

    it('does not JSON-parse already-parsed objects', () => {
        const parsed = [{ role: 'user', content: 'Hello' }]
        const event = createEvent({
            $ai_ingestion_source: 'otel',
            'gen_ai.input.messages': parsed,
        })
        mapOtelAttributes(event)
        expect(event.properties!.$ai_input).toEqual(parsed)
    })

    it('preserves unknown attributes', () => {
        const event = createEvent({
            $ai_ingestion_source: 'otel',
            'custom.attribute': 'custom-value',
            'gen_ai.request.model': 'gpt-4',
        })
        mapOtelAttributes(event)
        expect(event.properties!['custom.attribute']).toBe('custom-value')
        expect(event.properties!.$ai_model).toBe('gpt-4')
    })

    it('handles event with no properties', () => {
        const event: PluginEvent = {
            event: '$ai_generation',
            distinct_id: 'user-123',
            team_id: 1,
            uuid: 'test-uuid',
            timestamp: new Date().toISOString(),
            ip: '127.0.0.1',
            site_url: 'https://app.posthog.com',
            now: new Date().toISOString(),
        }
        const result = mapOtelAttributes(event)
        expect(result).toBe(event)
    })
})
