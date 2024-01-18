from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopulaireMovie(MRJob):

    # Définit les étapes du job MapReduce
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]

    # Étape 1 : Mapper pour extraire les évaluations par film
    def mapper_get_ratings(self, _, line):
        # Sépare la ligne en champs en utilisant la tabulation comme délimiteur
        (userID, movieID, rating, timestamp) = line.split('\t')
        # Émet une paire clé-valeur avec le film comme clé et la valeur 1
        yield movieID, 1

    # Étape 1 : Reducer pour compter le nombre d'évaluations par film
    def reducer_count_ratings(self, key, values):
        # Émet une paire clé-valeur avec le film et la somme des évaluations
        yield key, sum(values)

    # Étape 2 : Reducer pour trouver le film le plus populaire (celui avec le plus grand nombre d'évaluations)
    def reducer_find_max(self, key, values):
        # Émet une paire clé-valeur avec le film et le nombre maximum d'évaluations
        yield key, max(values)

# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MostPopulaireMovie.run()
