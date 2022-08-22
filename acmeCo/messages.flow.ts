import { IDerivation, Document, Register, PeriodicSource, StatsSource } from 'flow/acmeCo/messages';

// Implementation for derivation flow.yaml#/collections/acmeCo~1messages/derivation.
export class Derivation implements IDerivation {
    periodicPublish(source: PeriodicSource, register: Register, _previous: Register): Document[] {
        const out: Document = {
            message: `The time is ${source.ts} and register value is ${register}`,
            ...source,
        };
        return [out];
    }
    statsUpdate(source: StatsSource): Register[] {
        return [source.ts];
    }
}
