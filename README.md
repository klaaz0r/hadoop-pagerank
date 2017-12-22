#Hadoop pagerank and hits

Werkende implemntaties van pagrank en hits, helaas geen random jump en normalisatie omdat er geen tijd meer is voor een node count job. Als deze er wel was kon deze gebruikt worden als tussen stap en gebruikt worden voor de absolute waarden  G aka aantal nodes.

de Hits bestaat uit een extract stap, een link stap waar alle nodes waar zij naar wijzen gemapped worden en dan vervolgens een link stap waar alle nodes alle inkomende en uitkgaande links hebben staan plus de auth en hub waardes. De calculate stap verdeeld steeds de ingaange en uitgaande waardes en berekende de nieuwe auth en hub.

Pagerank heeft een extraxt stap, een link stap waar alle nodes waar zij naar wijzen gemapped worden met een default rank. De Mapper verdeelt de waardes en de Reducer geeft de nieuwe waarde aan de nodes. 
