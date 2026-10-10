# ADR 0007: toolchain Rust 1.98.1 e MSRV 1.98

- Stato: accettato
- Data: 2026-10-10
- Sostituisce: [ADR 0001](0001-rust-toolchain-and-msrv.md)

## Contesto

L'ADR 0001 fissava Rust 1.97.1 sia come toolchain di sviluppo e di CI sia
come MSRV. Le altre librerie Plenora sono passate a Rust 1.98: `database-tools`,
`IO-tools`, `storage-tools` e `rest-tools` fissano la toolchain 1.98.1, e
l'obiettivo di famiglia per la MSRV è 1.98 (oggi `storage-tools` e
`rest-tools` dichiarano `1.98`, `database-tools` e `IO-tools` `1.98.1`). Un consumer che compone runtime-tools
con quelle librerie compila comunque con almeno 1.98, quindi una MSRV più bassa
qui non apre alcun uso reale e costa una seconda toolchain da qualificare.

La CI installava la toolchain a mano con `rustup`, con action pinnate per tag e
senza cache.

## Decisione

- La toolchain fissata è **Rust 1.98.1** (`rust-toolchain.toml`), la stessa
  di `database-tools`, `IO-tools`, `storage-tools` e `rest-tools`.
- La MSRV dichiarata è **`rust-version = "1.98"`**, ereditata da ogni pacchetto
  del workspace. Si dichiara la serie 1.98, non la patch: è la forma usata da
  `storage-tools` e `rest-tools`.
- La CI qualifica la toolchain fissata, non ogni patch della serie 1.98: la
  MSRV è una dichiarazione di compatibilità, e la prova vale per 1.98.1.
- Ogni job della CI che esegue `cargo` installa la toolchain con
  `dtolnay/rust-toolchain` pinnata per SHA e `toolchain: '1.98.1'`: lo SHA è
  un commit dell'action, non la versione di Rust, che sceglie solo `toolchain:`. Ogni
  altra action è pinnata per SHA completo, con il tag in commento. Il test
  d'architettura `workspace_members_and_ci_share_msrv_and_same_sha_qualification`
  verifica che la toolchain fissata appartenga alla serie della MSRV e che ogni
  job `cargo` la usi; `workflow_actions_are_pinned_by_full_commit_sha` verifica
  i pin delle action.
- La validazione locale in Docker usa `rust:1.98.1-slim-bookworm` (e
  `rust:1.98.1-bookworm` per la prova di memoria), pinnate per digest
  dell'indice multi-architettura.
- La nightly del fuzz resta `nightly-2026-08-01`: è una scelta separata, legata
  a `cargo-fuzz` e a libFuzzer, non alla MSRV.

## Conseguenze

La toolchain coincide con quella delle librerie con cui runtime-tools verrà
composto. Un compilatore 1.97 non è più supportato. Abbassare la MSRV, o
dichiarare la compatibilità con una patch diversa da quella fissata, richiede
una corsa same-SHA di tutti i gate su quella toolchain prima di cambiare
questo ADR e i metadati dei pacchetti.
