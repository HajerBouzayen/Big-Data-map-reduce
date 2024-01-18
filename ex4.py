from mrjob.job import MRJob

class MRTempCounter(MRJob):
    
    # Méthode pour convertir de dixièmes de degrés Celsius à Fahrenheit
    def MakeFahrenheit(self, tenthsofcelsius):
        # Convertit les dixièmes de degrés Celsius en degrés Celsius
        celsius = float(tenthsofcelsius) / 10.0
        # Convertit les degrés Celsius en Fahrenheit
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit
    
    # Mapper: Extrait les températures minimales (TMIN) par emplacement et les convertit en Fahrenheit
    def mapper(self, _, line):
        # Sépare la ligne en champs en utilisant la virgule comme délimiteur
        (location, date, type, data, x, y, z, w) = line.split(',')
        
        # Vérifie si le type de données est 'TMIN'
        if type == 'TMIN':
            # Convertit la température de dixièmes de degrés Celsius à Fahrenheit
            temperature_fahrenheit = self.MakeFahrenheit(data)
            # Émet une paire clé-valeur avec l'emplacement comme clé et la température minimale en Fahrenheit comme valeur
            yield location, temperature_fahrenheit
        
    # Reducer: Agrège les températures minimales par emplacement et émet la température minimale pour chaque emplacement
    def reducer(self, location, temps):
        # Émet une paire clé-valeur avec l'emplacement comme clé et la température minimale en Fahrenheit parmi celles reçues
        yield location, min(temps)

# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    MRTempCounter.run()
