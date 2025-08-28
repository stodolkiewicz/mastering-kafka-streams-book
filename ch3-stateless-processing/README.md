### Example Tweet:
```json
{
    "CreatedAt": 1602545767000,
    "Id": 1206079394583924736,
    "Text": "Anyone else buying the Bitcoin dip?",
    "Source": "",
    "User": {
        "Id": "123",
        "Name": "Mitch",
        "Description": "",
        "ScreenName": "timeflown",
        "URL": "https://twitter.com/timeflown",
        "FollowersCount": "1128",
        "FriendsCount": "1128"
    }
}
```

### Very nice! < Borat voice >
https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html

### Split branches

#### with defaultBranch
```java
Predicate<String, String> isEnglish = (k, v) -> isEnglish(v);
Predicate<String, String> isSpanish = (k, v) -> isSpanish(v);

Map<String, KStream<String, String>> branches = stream
    .split(Named.as("language-split-"))
    .branch(isEnglish, Branched.as("english"))
    .branch(isSpanish, Branched.as("spanish"))
    .defaultBranch(Branched.as("other-languages")); // Można ją też nazwać!

KStream<String, String> englishStream = branches.get("language-split-english");
KStream<String, String> spanishStream = branches.get("language-split-spanish");
KStream<String, String> otherStream = branches.get("language-split-other-languages"); // Dostęp przez nazwę
```

#### without defaultBranch
```java
Predicate<String, String> isEnglish = (k, v) -> isEnglish(v);
Predicate<String, String> nonEnglishTweets = (k, v) -> !isEnglish(v);

// Chcemy tylko przetwarzać tweety po angielsku i nie-angielsku,
// a puste lub błędne (które nie spełniają żadnego warunku) odrzucić.
Map<String, KStream<String, String>> branches = stream
    .split(Named.as("language-split-"))
    .branch(isEnglish, Branched.as("english"))
    .branch(nonEnglishTweets, Branched.as("non-english"))
    .noDefaultBranch();

KStream<String, String> englishStream = branches.get("language-split-english");
```