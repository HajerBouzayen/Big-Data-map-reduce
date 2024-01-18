from mrjob.job import MRJob
from mrjob.step import MRStep

class OrderAmounts(MRJob):

    # Définit les étapes du job MapReduce
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_order, reducer=self.reducer_count_order),
            MRStep(mapper=self.mapper_make_order, reducer=self.reducer_output_order)
        ]

    # Étape 1 : Mapper pour extraire les montants de commande par client
    def mapper_get_order(self, _, line):
        # Sépare la ligne en champs en utilisant la virgule comme délimiteur
        (customer, item, order) = line.split(',')
        # Émet une paire clé-valeur avec le client comme clé et le montant de la commande comme valeur
        yield customer, float(order)

    # Étape 1 : Reducer pour agréger les montants de commande par client
    def reducer_count_order(self, customerID, order):
        # Émet une paire clé-valeur avec le client et la somme des montants de commande
        yield customerID, sum(order)

    # Étape 2 : Mapper pour inverser la clé et la valeur et formater le montant de commande
    def mapper_make_order(self, customerID, values):
        # Émet une paire clé-valeur avec le montant de commande formaté comme clé et le client comme valeur
        yield '%04.02f' % float(values), customerID

    # Étape 2 : Reducer pour émettre les résultats finaux triés
    def reducer_output_order(self, count, customers):
        # Émet une paire clé-valeur avec le montant de commande et le client pour chaque client
        for customer in customers:
            yield count, customer

# Point d'entrée pour exécuter le job MapReduce
if __name__ == '__main__':
    OrderAmounts.run()
