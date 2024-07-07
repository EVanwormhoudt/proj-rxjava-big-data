# Projet Big Data: Traitement de flux infinis

Ce projet a été réalisé dans le module Big Data et traite du problème de traitement de flux infinis. 
Il est basé sur de la programmation réactive orienté flux de données et utilise le framework RxJava et dans une autre version le framework Kafka. 
Le but de projet était de réaliser à la fois côté serveur et le côté client. 
Le serveur produit un flux infini de messages qui sont des citations et des proverbes provenant de la base utilisée par la commande “fortune” sous Linux. 
Le client reçoit le flux de messages sous la forme d'observables et implémente différents traitements en utilisant les opérateurs du framework RxJava (filter, map, merge, ...)