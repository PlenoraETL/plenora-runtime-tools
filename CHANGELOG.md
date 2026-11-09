# Changelog

## 0.1.1 — non rilasciata

### Dipendenze

- Allineati serde_json da 1.0.151 a 1.0.154 (feature conservate), thiserror e
  thiserror-impl da 2.0.20 a 2.0.21, tokio-rustls da 0.26.4 a 0.26.5 nei due
  lockfile; Hyper da 1.11.0 a 1.11.1 nel lock del workspace. Tokio resta 1.53.3
  e la toolchain resta Rust 1.98.1. Nessuna modifica alle versioni dei crate.

### Effetti osservabili delle dipendenze

- Hyper HTTP/1 completa il parsing del terminatore misto LF/CRLF anche quando
  arriva frammentato; riconosce `trailers` nelle liste e nelle righe multiple
  dell'header `TE`, consentendo l'invio dei trailer dichiarati dalla risposta;
  esegue il flush del terminatore chunked quando la fine del body diventa
  disponibile fra due poll di scrittura, senza attendere altri dati dal peer.
- tokio-rustls può restituire in una lettura più record TLS già disponibili,
  sia lato client sia lato server. I byte dello stream restano identici; la
  segmentazione delle letture non è un confine di messaggio.
- La diagnostica interna di serde_json per UTF-8 invalido indica il primo byte
  invalido anziché la fine della stringa. La categoria resta `Syntax`; il
  decoder Discovery continua a esporre la categoria propria `InvalidJson`.

Prove differenziali riproducibili e limiti della qualifica sono nel
[rapporto di allineamento](docs/dependency-alignment-0.1.1.md).
