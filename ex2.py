from mrjob.job import MRJob

class MTempCounter(MRJob):
    
    # Mapper: Extrait les températures maximales (TMAX) par emplacement
    def mapper(self, key, line):
        # Sépare la ligne en champs en utilisant la virgule comme délimiteur
        (location, date, type, data, x, y, z, w) = line.split(',')
        
        # Vérifie si le type de données est 'TMAX'
        if type == 'TMAX':
            # Émet une paire clé-valeur avec l'emplacement comme clé et la température maximale en tant que valeur
            yield location, float(data)
        
    # Reducer: Agrège les températures maximales par emplacement et émet la température maximale pour chaque emplacement
    def reducer(self, location, temps):
        # Émet une paire clé-valeur avec l'emplacement comme clé et la température maximale parmi celles reçues
        yield location, max(temps)
        
# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MTempCounter.run()

        