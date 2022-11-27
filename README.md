# FenySoft.Qdb

Eredeti forrás: https://github.com/STSSoft/STSdb4
Liszensz frissítve GPL-2.0-ról GPL-3.0-ra


# A változások naplózása

A változások egyszerűbb áttekintéséhez a CHANGELOG.md fájlban vezetjük a módosításokat.
A naplófájl vezetésében a következő kis szabályrendszert követjük.

## Kulcsszavak
Számos kulcsszó van, amelyet a végrehajtott változtatás típusának azonosítására használunk:

* ADDED - valami hozzá lett adva (egy funkció, egy új adatbázis implementáció stb.). Ezt a kulcsszót nem csak arra használjuk, hogy jelezzük, hogy egy új fájl került hozzáadásra.
* CHANGED - valami fontos dolog változott (egy implementáció, egy interfész stb.).
* FIXED - egy probléma javításra került.
* IMPROVED - valami javult (egy implementáció, egy osztály architektúra stb.).
* REMOVED - valami eltávolításra került.
* UPDATED - valami frissült (általában egy adatbázis bináris állománya).

A kulcsszavak ábécé sorrendben vannak, és a CHANGELOG-ban is ugyanúgy használjuk őket.

## Fájlszerkezet

A changelog egy adott verzióhoz a következő fejléccel kezdődik:

`ver. X.X.X.X (kiadatlan/kiadva)`

Ezután következnek a változásokat jelző kulcsszavak (ábécé sorrendben).
```
ADDED
CHANGED 
FIXED
JAVÍTOTT
ELTÁVOLÍTVA 
UPDATED 
```

Nem szükséges, hogy minden kulcsszó megmaradjon. Előfordul, hogy a változások nem tartalmazzák az összes megadott típust.

Translated with www.DeepL.com/Translator (free version)