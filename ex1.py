from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    
    # Mapper: Divise les lignes d'entrée en champs et émet une paire (rating, 1)
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1  # Émet la paire (rating, 1)
        
    # Reducer: Agrège les occurrences de chaque rating et émet la somme
    def reducer(self, rating, occurrences):
        yield rating, sum(occurrences)  # Émet la paire (rating, somme des occurrences)
        
# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MRRatingCounter.run()
