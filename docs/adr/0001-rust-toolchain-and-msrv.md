# ADR 0001: toolchain Rust di sviluppo e MSRV

- Stato: Sostituito da [ADR 0007](0007-toolchain-1-98-1-e-msrv-1-98.md)
- Data: 2026-08-15

## Contesto

L'host oggi non dispone di Cargo né di rustc. Le build devono comunque essere riproducibili. Il
grafo delle dipendenze scelto per la v0.1 è ora qualificato sulla toolchain fissata, quindi i
rilasci hanno bisogno di una versione minima di Rust supportata esplicita e verificabile.

## Decisione

Usare Rust 1.97.1 sia come toolchain fissata di sviluppo e di CI sia come MSRV conservativa della
v0.1. La validazione locale può girare nel container ufficiale `rust:1.97.1-slim-bookworm`. Ogni
pacchetto del workspace eredita `rust-version = "1.97.1"`, e la CI qualifica esattamente quella
toolchain.

Abbassare la MSRV richiede una qualificazione separata, sullo stesso SHA, di tutti i gate di
workspace, documentazione, policy delle dipendenze e NATS reale prima di modificare questo ADR e i
metadati dei pacchetti.

## Conseguenze

Le build di sviluppo e la soglia minima di compilatore supportata sono deterministiche. Di
proposito non si afferma che compilatori più vecchi funzionino; abbassare la soglia è un lavoro
futuro di compatibilità.
