Code

- zur Implementierung des Allokators eine Instanz der Klasse MemoryManager erzeugen

Benchmark

- Aufruf des Benchmarks: java Benchmarks Modus Größe Blockgröße Objektgröße Wiederholungen

    Modus: 1 = Initialisierung
           2 = Allokation
           3 = Lesen
           4 = 50% Lesen 50% Schreiben
           5 = 75% Lesen 25% Schreiben
           4 = 90% Lesen 10% Schreiben

    Größe: Größe des Speicherbereichs, Parameter wird mit 100MB multipliziert

    Blockgröße: maximale Blockgröße, parameter wird mit 1MB multipliziert, maximal 16MB

    Objektgröße: Größe der allozierten Objekte

    Wiederholungen: Anzahl der Wiederholungen, deren Durchschnitt berechnet wird
