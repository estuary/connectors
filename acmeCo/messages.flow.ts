import { IDerivation, Document, Register, AddMessageSource } from 'flow/acmeCo/messages';

// Implementation for derivation flow.yaml#/collections/acmeCo~1messages/derivation.
export class Derivation implements IDerivation {
    addMessagePublish(source: AddMessageSource, _register: Register, _previous: Register): Document[] {
        const out: Document = {
            message: `The time is ${source.ts}`,
            ...source,
        };
        return [out];
    }
}
