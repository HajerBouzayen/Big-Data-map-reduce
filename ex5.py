from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
    
    # Mapper : Extrait l'âge et le nombre d'amis, puis émet une paire (âge, nombre d'amis)
    def mapper(self, _, line):
        # Sépare la ligne en champs en utilisant la virgule comme délimiteur
        (ID, name, age, numfriends) = line.split(',')
        # Émet une paire clé-valeur avec l'âge comme clé et le nombre d'amis comme valeur
        yield age, int(numfriends)
        
    # Reducer : Agrège le nombre d'amis par âge et émet la moyenne du nombre d'amis pour chaque âge
    def reducer(self, age, numfriends):
        # Convertit l'itérateur numfriends en une liste
        n = list(numfriends)
        # Calcule la somme et la longueur de la liste pour calculer la moyenne
        s = sum(n)
        l = len(n)
        # Émet une paire clé-valeur avec l'âge comme clé et la moyenne du nombre d'amis comme valeur
        yield age, s / l

# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MRFriendsByAge.run()
