from mrjob.job import MRJob
import re

# Expression régulière pour extraire les mots d'une ligne
WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequency(MRJob):
    
    # Mapper : Tokenise la ligne en mots et émet une paire (mot, 1) pour chaque mot
    def mapper(self, key, line):
        # Utilise l'expression régulière pour extraire les mots de la ligne
        words = WORD_REGEXP.findall(line)
        # Émet une paire clé-valeur pour chaque mot avec le mot en minuscules et la valeur 1
        for word in words:
            yield word.lower(), 1
    
    # Reducer : Agrège le nombre d'occurrences de chaque mot et émet une paire (mot, nombre d'occurrences)
    def reducer(self, word, values):
        # Émet une paire clé-valeur avec le mot et la somme des occurrences du mot
        yield word, sum(values)

# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MRWordFrequency.run()
