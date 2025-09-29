# Documentation
Streams DSL  
https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html

# Scenariusze Testów dla Kafka Streams

## I. Operacje Bezstanowe (Stateless)
**Cel:** Weryfikacja, czy pojedyncza wiadomość jest poprawnie transformowana, filtrowana lub przekierowywana, bez zależności od poprzednich wiadomości.

### `filter / filterNot` ✅ DONE
- **Test Pozytywny:** Wiadomość spełniająca warunek przechodzi dalej.
- **Test Negatywny:** Wiadomość niespełniająca warunku jest odrzucana.
- **Test Graniczny:** Dla warunku `value > 100`, przetestuj dla wartości `99`, `100` i `101`.

### `map / mapValues` ✅ DONE
- **Transformacja Wartości:** Wartość wiadomości jest poprawnie zmieniana.
- **Transformacja Klucza (`map`):** Klucz wiadomości jest poprawnie zmieniany (i weryfikacja konsekwencji, np. dla późniejszej agregacji).
- **Transformacja Typu:** Typ obiektu w wiadomości jest poprawnie konwertowany.

### `selectKey` ✅ DONE
- **Zmiana Klucza:** Klucz jest poprawnie zmieniany na podstawie wartości rekordu.
- **Repartitioning:** Zmiana klucza powoduje repartitioning dla downstream operacji.
- **Null Safety:** Obsługa przypadków gdy wartość jest null lub pusta.

### `flatMap / flatMapValues` ✅ DONE
- **Jeden-do-Wielu:** Jedna wiadomość wejściowa generuje poprawną liczbę (więcej niż jedną) wiadomości wyjściowych.
- **Jeden-do-Zera:** Jedna wiadomość wejściowa nie generuje żadnej wiadomości wyjściowej.

### `branch` ✅ DONE
- **Poprawne Rozgałęzienie:** Wiadomości trafiają do właściwych strumieni na podstawie zdefiniowanych predykatów.
- **Brak Dopasowania:** Wiadomość, która nie pasuje do żadnego predykatu, jest poprawnie odrzucana.

### `merge` ✅ DONE
- **Łączenie Streamów:** Multiple streams są poprawnie łączone w jeden.
- **Zachowanie Kolejności:** Rekordy zachowują czasową kolejność po merge.

### `TopicNameExtractor` ✅ DONE
- **Dynamic Routing:** Rekordy są kierowane do różnych topicó w zależności od content.
- **Topic Auto-Creation:** Kafka Streams tworzy topici jeśli nie istnieją.

---

## II. Operacje Stanowe (Stateful)
**Cel:** Weryfikacja, czy agregacje i aktualizacje stanu działają poprawnie w miarę napływania kolejnych wiadomości.

### Grupowanie danych
### `groupByKey` ✅ DONE
- **Zachowanie Klucza:** Grupowanie zachowuje istniejący klucz rekordu.
- **Przygotowanie do Agregacji:** Umożliwia operacje jak count(), reduce(), aggregate().

### `groupBy` ❌ TODO
- **Nowy Klucz:** Grupowanie na podstawie nowego klucza z wartości.
- **Repartitioning:** Automatyczne repartitioning gdy klucz się zmienia.

### Agregacje
### `count` ✅ DONE
- **Inicjalizacja:** Pierwsza wiadomość dla danego klucza daje wynik `1`.
- **Inkrementacja:** Kolejne wiadomości dla tego samego klucza poprawnie zwiększają licznik.
- **Niezależność Kluczy:** Liczniki dla różnych kluczy są od siebie niezależne.

### `reduce` ❌ TODO
- **Inicjalizacja:** Pierwsza wiadomość poprawnie inicjuje stan agregacji.
- **Redukcja:** Kolejne wiadomości poprawnie modyfikują stan zgodnie z logiką Reducer.

### `aggregate` ❌ TODO
- **Initializer:** Pierwsza wiadomość dla klucza poprawnie wywołuje Initializer.
- **Aggregator:** Kolejne wiadomości poprawnie aktualizują agregat za pomocą Aggregator.

---

## III. Operacje Okienne (Windowed)
**Cel:** Weryfikacja, czy operacje stanowe działają poprawnie w granicach czasowych.  
Kluczowe jest użycie `TopologyTestDriver.advanceWallClockTime()` do symulowania upływu czasu.

### Testy Ogólne (dla każdego typu okna: Tumbling, Hopping, Session)
- **Wiadomości w Jednym Oknie:** Grupa wiadomości z bliskimi timestampami jest agregowana w jednym oknie.
- **Wiadomości na Granicy Okien:** Dwie wiadomości po przeciwnych stronach granicy okna trafiają do dwóch różnych okien.
- **Wiadomości Nieuporządkowane (Out-of-Order):** Wiadomość ze starszym timestampem, ale wciąż w ramach otwartego okna, jest poprawnie uwzględniana.
- **Wiadomości Spóźnione (Late-Arriving):** Wiadomość z timestampem z okna, które już dawno się zamknęło (poza grace period), jest odrzucana.

### Testy Specyficzne dla Typu Okna
- **Hopping (skaczące):** Jedna wiadomość jest uwzględniana w wielu nakładających się na siebie oknach.
- **Session (sesyjne):**
    - **Tworzenie Sesji:** Wiadomości w bliskim odstępie czasu tworzą jedną sesję; wiadomość po dłuższej przerwie (inactivity gap) tworzy nową sesję.
    - **Łączenie Sesji:** Dwie oddzielne sesje są poprawnie łączone w jedną, gdy pojawi się wiadomość „pomostowa” między nimi.

### Operator `suppress`
- **Emisja po Zamknięciu Okna:** Wynik agregacji jest emitowany z KTable dopiero po przesunięciu czasu strumienia (`advanceWallClockTime`) za koniec okna + grace period.
- **Brak Emisji Pośrednich:** Żadne pośrednie wyniki nie są emitowane przed zamknięciem okna.

---

## IV. KTable i GlobalKTable
**Cel:** Zrozumienie abstrakcji tabeli jako strumienia zmian i globalnego stanu.

### KTable ❌ TODO
- **Upsert Semantics:** Nowy rekord z tym samym kluczem nadpisuje poprzedni.
- **Tombstone Handling:** null value usuwa klucz z tabeli.
- **toStream():** Konwersja KTable → KStream emituje changelog events.
- **State Query:** Odczyt aktualnego stanu przez getStateStore().

### GlobalKTable ❌ TODO  
- **Full Replication:** Każda instancja aplikacji ma pełną kopię danych.
- **No Partitioning:** Nie ma ograniczeń co-partitioning dla joinów.
- **Bootstrap:** Tabela jest w pełni załadowana przed rozpoczęciem przetwarzania.
- **Join with Any Key:** Join KStream z GlobalKTable na dowolnym kluczu.

### KTable Operations ❌ TODO
- **filter():** Filtrowanie rekordów w tabeli.
- **mapValues():** Transformacja wartości w tabeli.
- **join():** KTable-KTable join (inner/left/outer).
- **aggregate():** Agregacja wartości grupowanych kluczy.

---

## V. Złączenia (Joins)
**Cel:** Weryfikacja logiki łączenia strumieni i wzbogacania danych, zarówno w oknach czasowych, jak i z tabelami.

### KStream–KStream Join (Windowed)
- **Idealne Dopasowanie:** Wiadomości z obu strumieni z bliskimi timestampami poprawnie tworzą parę.
- **Odwrócona Kolejność:** Join działa poprawnie niezależnie od tego, która wiadomość (lewa czy prawa) przyszła pierwsza.
- **Brak Dopasowania (poza oknem):** Wiadomości, których timestampy różnią się o więcej niż czas okna, nie są łączone (`inner join`).
- **Kardynalność (Jeden-do-Wielu):** Jedna wiadomość z lewego strumienia jest poprawnie łączona z wieloma pasującymi wiadomościami z prawego.
- **Left Join:** Wiadomość z lewego strumienia, dla której nie znaleziono dopasowania w oknie, po upływie czasu generuje wynik z wartością `null` po prawej stronie.
- **Okres Karencji (grace period):** Spóźniona wiadomość jest poprawnie łączona, jeśli dotrze w ramach grace period dla swojego okna.

### KStream–KTable Join
- **Wzbogacanie Danych:** Wiadomość ze strumienia jest poprawnie łączona z aktualnym stanem z tabeli.
- **Brak Klucza w Tabeli:** Wiadomość ze strumienia, dla której nie ma klucza w tabeli, nie generuje wyniku (`inner join`).
- **Aktualizacja Tabeli:** Najpierw zaktualizuj tabelę, a potem wyślij wiadomość do strumienia, aby sprawdzić, czy join używa nowej wartości.

### KTable–KTable Join
- **Reaktywna Aktualizacja:** Zmiana w jednej z tabel (lewej lub prawej) powoduje poprawną re-ewaluację i emisję zaktualizowanego wyniku złączenia.

---

## V. Aspekty Techniczne i Konfiguracja
**Cel:** Weryfikacja odporności aplikacji na błędy i poprawnej obsługi metadanych.

### Serializacja/Deserializacja (SerDes)
- **Błędne Dane („Poison Pill”):** Wiadomość w niepoprawnym formacie (np. zły JSON) nie powoduje awarii aplikacji i jest obsługiwana (np. przez `DeserializationExceptionHandler`).
- **Brakujące Pola:** Wiadomość z brakującymi polami (wartości `null`) jest poprawnie przetwarzana przez logikę biznesową.

### TimestampExtractor
- **Poprawność:** Timestamp zdarzenia jest poprawnie wyciągany z treści wiadomości, a operacje okienne bazują na nim, a nie na czasie brokera.

### Wiadomości „Tombstone” (`key, null`)
- **Usunięcie z KTable:** Wysłanie „tombstone” poprawnie usuwa klucz z KTable, co jest widoczne w kolejnych złączeniach.

---

## VI. Zaawansowane Koncepcje i Mechanizmy Wewnętrzne
**Cel:** Zrozumienie, jak Kafka Streams działa „pod maską”.  
Te testy uczą interakcji z niższymi warstwami API i semantyki platformy.

### Interakcja z Magazynem Stanu (State Store)
- **Bezpośrednie Zapytanie do Stanu:**  
  `driver.getStateStore("nazwa-sklepu").get(klucz)`  
  → sprawdzenie, czy stan wewnątrz State Store jest poprawny.
- **Weryfikacja Stanu Okiennego:**  
  `driver.getWindowStore("nazwa-sklepu").fetch(klucz, od, do)`  
  → sprawdzenie stanu dla konkretnego okna czasowego.


### Semantyka KTable (Strumień Zmian)
- **Test Aktualizacji:** Dwie wiadomości z tym samym kluczem → strumień emituje najpierw „nowy” wpis, potem „aktualizację”.
- **Test „Tombstone” na Strumieniu Zmian:** Wysłanie `tombstone` powoduje, że `table.toStream()` emituje wiadomość z `null` jako wartością.

### Testowanie Repartrycjonowania
- **Scenariusz:** `selectKey()` lub `map()` → zmiana klucza → `groupByKey()` + `count()`.
- **Test:** Agregacja odbywa się na podstawie **nowego** klucza, co potwierdza, że dane przeszły przez wewnętrzny temat repartrycjonujący.

### Obsługa Błędów w Trakcie Przetwarzania
- **StreamUncaughtExceptionHandler:** własny handler błędów.
- **Test:** Operator (np. `mapValues`) celowo rzuca wyjątek dla danej wiadomości.
    - Handler jest wywołany.
    - Aplikacja albo kontynuuje (`REPLACE_THREAD` / `CONTINUE`), albo zatrzymuje się (`SHUTDOWN_CLIENT`).  
