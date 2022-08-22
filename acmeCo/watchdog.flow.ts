import { IDerivation, Document, Register, DelayedStatsSource, LiveStatsSource } from 'flow/acmeCo/watchdog/messages';

// Implementation for derivation flow.yaml#/collections/acmeCo~1watchdog~1messages/derivation.
export class Derivation implements IDerivation {
    delayedStatsPublish(source: DelayedStatsSource, register: Register, _previous: Register): Document[] {
        const uniqueValue = `ts=${source.ts}, open=${source.openSecondsTotal}`;
        if (uniqueValue == register) {
            return [
                {
                    ts: new Date().toISOString(),
                    message: `No stats from ${source.shard.name} since ${source.ts}`,
                },
            ];
        }
        return [];
    }
    liveStatsUpdate(source: LiveStatsSource): Register[] {
        const uniqueValue = `ts=${source.ts}, open=${source.openSecondsTotal}`;
        return [uniqueValue];
    }
}
