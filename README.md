# Productor
Ce plugin permet de générer le SQL pour les passages en production de couches Postgis au sein du SIG de l'Eurométropole de Strasbourg.
Il permet également de passer du SQL depuis la Production vers le Développement et de générer une partie de SQL permmetant de modifier les énumérations.

## Passages en production
L'usage principal de ce plugin est de générer les fichiers SQL nécessaires au passage en production.
Dans l'exemple ci-dessous on peut voir un export de la table al_dt_gestion_proximite dans le schéma alert.

![image](https://github.com/cazitouni/Productor/assets/92778930/61b00ad4-26d4-48bc-a01e-c19aee670dbe)

Il est également possible de générer le SQL pour plusieurs tables en les selectionnant dans le menu associé si ces dernières se trouvent dans le même schéma. 
La démarche reste la même pour exporter des couches depuis la Production vers le Développement(indiquer sigli en base de données).

lors de l'export de vues et vues matérialisées, les tables associées seront récupérées de manière récursive.

Si tout s'est déroulé comme prévu, le plugin devrait avoir généré un dossier export_sql à l'endroit indiqué.

![image](https://github.com/cazitouni/Productor/assets/92778930/47d9534a-5216-4e20-b7ac-11e0ef27b082)

Dans ce dossier se trouvent les différents fichiers SQL nécessaires au fonctionnement de la table. 
Leur numéro correspond à l'ordre dans lequel le SQL doit être exécuté :

<ol>
  <li>Les énumérations</li>
  <li>Les fonctions</li>
  <li>Les structures de table</li>
  <li>Les vues</li>
  <li>Les grants/droits</li>
</ol>

## Imports en Développement
Si l'export effectué précédemment concernait une table en production, il est possible d'utiliser le plugin pour réimporter les structures en Développement.
Pour cela il suffit d'indiquer le dossier d'export et de rentrer les informations de connexion de votre base.

**Attention : Il est tout de même recommandé de vérifier les fichiers SQL avant tout import en base.**

![image](https://github.com/cazitouni/Productor/assets/92778930/d3f09b6f-e2be-4244-8f6f-c24a0ec3c5b9)

Le plugin va réimporter toutes les structures et leurs éléments associés, les données quant à elles doivent être réimportée manuellement pour le moment.

## Modification des énumérations
**Attention : Cette fonctionalité ne concerne pas les énumérations utilisées dans des vues.**
Pour modifier une énumération il suffit simplement de la sélection à l'aide du menu.

![image](https://github.com/cazitouni/Productor/assets/92778930/0b7e7f24-3134-421e-b9cf-73e3142b41d9)

Le menu "Valeurs" permet de modifier ces dernières de les supprimer ou de les ajouter.
Une fois la modification terminée va générer un fichier SQL dont les deux parties sont à passer séparément afin de pouvoir modifier les valeurs de la table en amont. 

Cette fonctionnalité concerne principalement des énumérations qui requièrent une modificiation importante.
S'il s'agit seulement d'ajouter ou modifier une valeur du SQL brut suffit amplement.

