# Pressione sulle risorse

`plenora-runtime-resources` sorveglia la memoria residente del processo senza dipendere da un
fornitore di metriche, da un motore di worker, da un broker o da un database. Classifica il
processo come `Initializing`, `Normal`, `Pressured`, `Critical` o `Unavailable` e proietta quello
stato nel registro condiviso della salute.

Il monitor è fail-closed. Prima del primo campione riuscito, dopo un fallimento del campionamento e
finché una delle due soglie è attiva, il `WorkerAdmissionControl` collegato rifiuta il lavoro
nuovo. Gli handler attivi non vengono annullati. L'ammissione si riapre dopo il numero configurato
di campioni consecutivi sotto la soglia di ripresa. Le soglie distinte di ripresa e morbida danno
isteresi ed evitano rapide oscillazioni di stato.

`ProcessMemorySampler` legge `/proc/self/status` su Linux senza codice unsafe. Sulle altre
piattaforme si deve iniettare un campionatore fornito dall'applicazione oppure omettere il
monitor; un campionamento non supportato non riporta mai in silenzio un uso pari a zero.

Le soglie si scelgono a partire dal limite del container, non dalla RAM dell'host:

```text
resume threshold < soft admission threshold < hard unhealthy threshold < container limit
```

La soglia rigida non uccide né ricicla il processo. Segna la salute come non sana e tiene chiusa
l'ammissione, così che un supervisore esterno possa applicare la propria politica di deployment.
Il riciclo del processo è una decisione di isolamento separata, descritta in
[`subprocess-execution.md`](subprocess-execution.md).

L'osservatore riceve solo contatori, stato e valori in byte. Deve restare non bloccante e non deve
eseguire scritture su database in linea; la persistenza spetta al futuro adapter `database-tools`.
