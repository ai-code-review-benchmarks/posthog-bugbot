import { PluginEvent } from '@posthog/plugin-scaffold'

import { parseJSON } from '../../utils/json-parse'

const ATTRIBUTE_MAP: Record<string, string> = {
    'gen_ai.input.messages': '$ai_input',
    'gen_ai.output.messages': '$ai_output_choices',
    'gen_ai.usage.input_tokens': '$ai_input_tokens',
    'gen_ai.usage.output_tokens': '$ai_output_tokens',
    'gen_ai.request.model': '$ai_model',
    'gen_ai.provider.name': '$ai_provider',
}

const JSON_PARSE_PROPERTIES = new Set(['$ai_input', '$ai_output_choices'])

export function mapOtelAttributes(event: PluginEvent): PluginEvent {
    if (!event.properties || event.properties.$ai_ingestion_source !== 'otel') {
        return event
    }

    for (const [otelKey, phKey] of Object.entries(ATTRIBUTE_MAP)) {
        if (event.properties[otelKey] !== undefined) {
            let value = event.properties[otelKey]
            if (JSON_PARSE_PROPERTIES.has(phKey) && typeof value === 'string') {
                try {
                    value = parseJSON(value)
                } catch {
                    // Keep original string value if parsing fails
                }
            }
            event.properties[phKey] = value
            delete event.properties[otelKey]
        }
    }

    return event
}
