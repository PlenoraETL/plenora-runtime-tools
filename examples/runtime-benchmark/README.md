# Microbenchmark del runtime

Questo benchmark in modalità release misura l'esecuzione dei worker indipendente dal motore e il
dispatch dello scheduler. Il driver non ha mai più task Tokio di `--max-in-flight`, accetta al
massimo 1.000.000 di iterazioni e stampa un solo oggetto JSON, così le corse si possono archiviare e
confrontare sullo stesso host.

```text
cargo run --release -p plenora-runtime-example-benchmark -- \
  --iterations 20000 --max-in-flight 32
```

Eseguire almeno cinque campioni dopo un riscaldamento, registrare modello di CPU, sistema
operativo, versione di Rust, profilo energetico e SHA del commit, poi confrontare le mediane.
Questi numeri misurano il costo di coordinamento del runtime; non predicono il throughput di NATS,
dei database, di Python o delle librerie reali.
