# Binario sperimentale

`plenora-runtime scripted-call CONFIG` legge una Request JSON da stdin e scrive
una Response JSON su stdout. CONFIG e un documento ScriptedConfig: puo contenere
credenziali e va protetto dall'host. `plenora-runtime remote-call SERVER SUBJECT`
invia la stessa richiesta al listener NATS scriptato. Non e la sintassi finale P1.5.

Il timeout remoto non prova cancellazione. Limiti:
[ADR 0010](../../docs/adr/0010-scripted-chain-facades.md).
