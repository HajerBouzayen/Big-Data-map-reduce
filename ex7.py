from mrjob.job import MRJob
import re
from mrjob.step import MRStep

# Expression régulière pour extraire les mots d'une ligne
WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    # Définit les étapes du job MapReduce
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words, reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_count_words, reducer=self.reducer_output_words)
        ]

    # Étape 1 : Mapper pour extraire les mots
    def mapper_get_words(self, _, line):
        # Utilise l'expression régulière pour extraire les mots de la ligne
        words = WORD_REGEXP.findall(line)
        # Émet une paire clé-valeur pour chaque mot avec le mot en minuscules et la valeur 1
        for word in words:
            yield word.lower(), 1

    # Étape 1 : Reducer pour compter le nombre d'occurrences de chaque mot
    def reducer_count_words(self, word, values):
        # Émet une paire clé-valeur avec le mot et la somme des occurrences du mot
        yield word, sum(values)

    # Étape 2 : Mapper pour inverser la clé et la valeur et formater le compteur
    def mapper_make_count_words(self, word, count):
        # Émet une paire clé-valeur avec le compteur formaté comme clé et le mot comme valeur
        yield '%04d' % int(count), word

    # Étape 2 : Reducer pour émettre les résultats finaux triés
    def reducer_output_words(self, count, words):
        # Émet une paire clé-valeur avec le compteur et le mot pour chaque mot
        for word in words:
            yield count, word

# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MRWordFrequencyCount.run()
