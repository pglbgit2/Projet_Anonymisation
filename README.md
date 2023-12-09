# PSEUDO CODE de la défense kouign amann  
### Métrique à prendre en compte :  
- Date Utility (Pas besoin de modifié dans un premier temps, mais faire attention en cas de volonté de pollution)  
- Hour Utility (On vérifier sur le calcule mais l'arrondi à l'heure semble correct)  
- Point of Interest (On va se basé sur cette métrique pour l'anonymisation)  
- Distance Utility (pour un 50% d'utilité il faut rester dans la même ville (0.1))  
- Meet Utility (ce fera naturellement)  
- Tuile Utility (celui qu'on abandonne à 99%)  

### Idée de base :  
Quel défense ont pu mettre en place les ennemis ?  
- Bruit basique + k-anonymat sur les heures  
- Défense sur les POI comme kouign amann  
Il faudrait pouvoir identifier sur quels défenses ils se sont orientés  
	Il faut donc essayer de chercher à savoir si il y a un choix dans leur suppression de lignes  
Si ils ne se sont pas orienté vers les POI :  
	On peut essayer de simplement faire des calcules de moyennes tout les jours et comparer avec le fichier de base  
Si ils se sont orienté vers les POI :  
	Faire une attaque en calculant par semaine les moyennes de chaque POI et comparer avec le fichier de base  
Si il y a moyen de désanonymiser les lignes DEL on peut compter les lignes manquantes théorique et comparer  

#### Idées d'Algorithmes :  
Analyse des DEL:  
	lire les DEL et chercher à savoir si il y a un grand nombre de DEL qui se ressemble sur les heures ou coordonnées  

Identification des DEL:  
	Jointure de base et si possible voir si la jointure à fonctionné  

Attaque sur les moyennes:  
	Comparer les moyennes pour chaque jour avec le fichier de base  
	L'ID qui correspond le plus est estimé comme étant le bon  
	Prendre en compte une mémoire pour traiter les ID déjà identifié et qu'ils brouillent pas les autres identification  
	Si possible afficher un pourcentage de réussite et de confiance  

Attaque sur les POI:  
	Refaire les calcules de début de kouign amann sur les moyennes de chaque POI pour le dataset attaqué et le default  
	Comparer les moyennes de chaque POI  
	Si possible afficher un pourcentage de réussite et de confiance  

#### Amélioration :

