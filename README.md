# Etudes des bases de données NoSQL à partie de critères

Le travail ici demandé fait suite au cours « Les Bases de Données du Big Data ». Ces bases de
données sont aussi souvent appelées Bases de Données NoSql. L’objectif est, à partir de
critères et d’un schéma de données prédéfinis, d’évaluer une base de données Nosql..  

La base de données Nosql que nous évaluons est:  
- Cassandra  

Notre groupe est composé de:
- Saad el din AHMED
- Yessine BEN EL BEY
- Hugo NORTIER
- Raoua Messai
- Elias TAZI  

## Commandes

```sh
docker run -p 9042:9042 -t -d cassandra:latest
```

Pour effectuer les insertions sous linux: 
```sh
export PATH_TO_DATA=le/chemin/vers/2DATA/
./script_insert.sh
```
Pour effectuer les insertions sous windows: 
```sh
Set PATH_TO_DATA=le/chemin/vers/2DATA/
script_insert.bat
```