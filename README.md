# rechInfo
Projet du Recherche d'Information - 5ème année INSA Toulouse - Assoualma, Ciruzzi

## Libraries
All the utilised libraries are under the _/lib_ directory. Their source code have been downloaded and then compiled to generate a _.jar_ file or, in somecase, just downloaded as _.jar_ file.

### Stemmer
The stemmer used is [Tartarus Snowball](http://snowball.tartarus.org/). The stemming algorithm utilised by the library can be found [here](http://snowball.tartarus.org/algorithms/french/stemmer.html), where there's also a list of stemming examples.

### Parser
The library used for parsing HTML files is [JSoup](https://jsoup.org/).

### Database
The databased utilised is [SQLite](https://sqlite.org/). It's used for storing the parsed HTML documents, the parsed requests and their results and also for making some queries in order to evaluate the system performance.
