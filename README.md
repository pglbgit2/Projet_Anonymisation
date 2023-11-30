# PSEUDO CODE de la défense kouign amann
### Métrique à prendre en compte :
- Date Utility (Pas besoin de modifié dans un premier temps, mais faire attention en cas de volonté de pollution)
- Hour Utility (On vérifier sur le calcule mais l'arrondi à l'heure semble correct)
- Point of Interest (On va se basé sur cette métrique pour l'anonymisation)
- Distance Utility (pour un 50% d'utilité il faut rester dans la même ville (0.1))
- Meet Utility (ce fera naturellement)
- Tuile Utility (celui qu'on abandonne à 99%)

### Idée de base : 
Utiliser la méthode de k-anonymisation sur les points of interest et l'abusée au point de rassembler un très grand nombre d'id dessus

#### Entrée : 
BDD avec les id déjà anonymisé
		(permet de différencié dès le début pour les métriques)

#### Sortie : 
BDD anonymisé "kouign_amann.csv"

#### Début :
	ChargerLaBDD(default.csv)
	pour tous les ids:
		calculer des différents points of interest
	tant que tout les ids ne sont pas traité: // l'idée est de s'arrêter lorsque tout les couples sont soit dans groupe soit gérer autrement
		pour tous les "places" identifié:
			rassembler les couples (id;POI) qui sont proches (cf: distance utility)
		pour tous les couples (id;POI) seuls:
			//A comparer
			soit:
				aggrandir la marge de distance utility pour rattaché les autres MAIS il faut marqué les couples éloigné pour plus tard
			soit:
				les abandonner à leurs sorts (ils seront probablement réidentifié sans difficulté mais on sauve de l'efficacité)
			soit:
				rassembler une partie/tous les seuls ça nique l'utilité mais pollue l'identification (probablement à calculé le (bénéfice potentiel/pertes d'utilité)
			// Si idée supplémentaire proposé ici:

	//Modification minim à faire selon choix des solos
	pour tous les groupes:
		calculer une moyenne de tout les POI SAUF LES MARQUÉS(cf couples seuls)
		assigner à l'ensemble des lignes concerné par les couples, dans les valeurs de localisation, la moyenne calculé
		
	//Finition
	arrondir les heures

	//Encore une fois à comparer mais je pense que l'option A est mieux car plus utilisé par les autres groupes
	pour toutes les lignes qui ne sont pas prise en compte dans le calcule de POI:
		//Option A
		assigner DELL à l'id
		//Option B
		random(HEURE,JOUR,LOCALISATION)
		//Option C
		//trouver un moyen de monter le score d'utilité en ne détruisant pas l'anonymisation faites avant car les id sont en commun
#### Fin
