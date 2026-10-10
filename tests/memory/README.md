# Sonda di ritenzione della memoria

Questo pacchetto verifica se l'esecuzione prolungata dei worker rilascia le allocazioni possedute
dai task. Usa buffer sintetici effettivamente allocati, quindi valida i meccanismi del runtime e
l'harness di misura anziché predire il comportamento futuro della memoria di `data-tools`,
`database-tools` o `IO-tools`.

La suite normale del workspace esegue piccoli contratti deterministici per successo, fallimento,
timeout, concorrenza e il classificatore della crescita. Il soak RSS è opzionale e va eseguito
tramite il wrapper Docker limitato dalla radice del workspace:

```powershell
./scripts/memory-soak.ps1
```

Il wrapper compila una volta, esegue ogni scenario in un container senza rete con un limite di
memoria rigido e scrive un file CSV per scenario sotto `target/memory-soak/`. Lo scenario
intenzionale `leak-control` deve essere classificato come in crescita; prova che l'harness rileva
le allocazioni trattenute.

Un RSS alto dopo la prima iterazione non è automaticamente un leak. Un plateau stabile
dell'allocatore passa. La crescita si misura fra la mediana del primo e quella dell'ultimo quartile
dei campioni dopo il riscaldamento.
