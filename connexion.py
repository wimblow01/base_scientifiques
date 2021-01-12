import pymongo
from pprint import pprint
try: 
    from mdp import mdp, user
except:
    user = "stephane"
    mdp = "isenbrest"

class Connexion:

    @classmethod
    def connect(cls):
        cls.user = user
        cls.password = mdp
        cls.database = "DBLP"
        return pymongo.MongoClient(f"mongodb+srv://{cls.user}:{cls.password}@clusterwim.bhpor.mongodb.net/{cls.database}?retryWrites=true&w=majority")


    @classmethod
    def open_db(cls):
        cls.client = cls.connect()
        cls.publis = cls.client.DBLP.publis


    @classmethod
    def close_db(cls):
        cls.client.close()
    
# Compter le nombre de documents de la collection publis
    @classmethod
    def get_number(cls):
        cls.open_db()
        count = len(list(cls.publis.find({})))
        cls.close_db()
        return count

 # Lister tous les livres (type “Book”) 
    @classmethod
    def get_book(cls):
        cls.open_db()
        show = list(cls.publis.find({"type":"Book"}))
        cls.close_db()
        return show

# Lister les livres depuis 2014 
    @classmethod
    def get_books_2014(cls):
        cls.open_db()
        show = list(cls.publis.find({"type":"Book", "year":{"$gte":2014}}, {'title': 1, 'year':1, '_id':0}))
        cls.close_db()
        return show

 # Lister les publications de l’auteur “Toru Ishida” 
    @classmethod
    def get_publi_ishida(cls):
        cls.open_db()
        # show = list(cls.publis.find({"authors":"Toru Ishida"}, {'title': 1, '_id':0}))
        show = list(cls.publis.find({"authors":{'$regex':'Toru Ishida'}}, {'title': 1, '_id':0}))
        cls.close_db()
        return show

# Lister tous les auteurs distincts
    @classmethod
    def get_authors(cls):
        cls.open_db()
        show = list(cls.publis.distinct("authors"))
        cls.close_db()
        return show

 # Trier les publications de “Toru Ishida” par titre de livre
    @classmethod
    def get_publi_ishida_croiss(cls):
        cls.open_db()
        show = list(cls.publis.find({"authors":"Toru Ishida"}, {'title': 1, '_id':0}).sort('title',1))
        cls.close_db()
        return show

# Compter le nombre de ses publications 
    @classmethod
    def get_number_ishida(cls):
        cls.open_db()
        count = len(list(cls.publis.find({"authors":"Toru Ishida"})))
        cls.close_db()
        return count

# Compter le nombre de publications depuis 2011 et par type
    @classmethod
    def get_books_2011_type(cls):
        cls.open_db()
        types = list(cls.publis.distinct("type"))
        liste = {}
        for _type in types:
            liste[_type] = len(list(cls.publis.find({"type":_type, "year":{"$gte":2011}})))
        cls.close_db()
        return liste

# Compter le nombre de publications par auteur et trier le résultat par ordre croissant
    @classmethod
    def get_count_publi_author(cls):
        cls.open_db()

        count = list(cls.publis.aggregate([
            {"$unwind": "$authors" },
            {"$group": {"_id": {"authors":"$authors"},"count": { "$sum": 1 }}},
            {"$sort": {"count": 1}}
            ]))
        cls.close_db()
        return count





        # Compter le nombre de documents de la collection publis
# print(f"Il y a {Connexion.get_number()} publications dans la bdd")

        # Lister tous les livres (type “Book”) 
# pprint(Connexion.get_book())

        # Lister les livres depuis 2014 
# pprint(Connexion.get_books_2014())

        # Lister les publications de l’auteur “Toru Ishida” 
# pprint(Connexion.get_publi_ishida())

        # Lister tous les auteurs distincts 
# pprint(Connexion.get_authors())

        # Trier les publications de “Toru Ishida” par titre de livre 
# pprint(Connexion.get_publi_ishida_croiss())

        # Compter le nombre de ses publications 
# print(f"Toru Ishida a publié {Connexion.get_number_ishida()} ouvrages")

        # Compter le nombre de publications depuis 2011 et par type 
# pprint(Connexion.get_books_2011_type())

        # Compter le nombre de publications par auteur et trier le résultat par ordre croissant
# pprint(Connexion.get_count_publi_author())