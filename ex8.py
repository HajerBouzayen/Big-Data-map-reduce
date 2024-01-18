from mrjob.job import MRJob

class MROrderAmounts(MRJob):

    def mapper(self, key, line):
        # Séparer chaque ligne en colonnes en utilisant la virgule comme délimiteur
        (customer, item, order) = line.split(',')

        # Émettre le client comme clé et le montant de la commande (converti en float) comme valeur
        yield customer, float(order)

    def reducer(self, customer, orders):
        # Agréger le montant total des commandes pour chaque client
        yield customer, sum(orders)

if __name__ == '__main__':
    MROrderAmounts.run()