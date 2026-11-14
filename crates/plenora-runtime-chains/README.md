# Coordinatore sequenziale sperimentale

Coordinatore Rust con journal opaco, CAS, ordine topologico, prenotazioni globali,
eventi e riconciliazione esplicita. I test pubblici usano adapter e store scriptati;
non attestano librerie reali. PostgreSQL implementa la stessa interfaccia in chain-store.

Il profilo corrente ammette un tentativo per step; politiche con piu tentativi
sono rifiutate. Non e il completamento di P1.4. Il protocollo Request/Response
centralizza le chiamate; ChainClient non contiene algoritmi di esecuzione.

Limiti: [qualification](../../docs/qualification.md#gate-p14p15-sperimentale).
Prova locale: `cargo test -p plenora-runtime-chains`.
