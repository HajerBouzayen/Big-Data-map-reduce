from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        # Séparer chaque ligne en mots
        words = line.split()

        # Émettre chaque mot en minuscules avec une occurrence de 1
        for word in words:
            yield word.lower(), 1

    def combiner(self, key, values):
        # Combiner localement les occurrences partielles des mots
        yield key, sum(values)

    def reducer(self, key, values):
        # Agréger les occurrences finales des mots
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()