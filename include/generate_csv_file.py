import pandas as pd
from faker import Faker
from datetime import datetime


def create_file():
    # Initialiser Faker
    fake = Faker()

    # Générer des données fictives
    data = {
        "nom": [fake.name() for _ in range(10)],
        "email": [fake.email() for _ in range(10)],
        "adresse": [fake.address() for _ in range(10)],
        "telephone": [fake.phone_number() for _ in range(10)],
        "date_naissance": [fake.date_of_birth(minimum_age=18, maximum_age=65) for _ in range(10)]
    }

    # Créer un DataFrame pandas
    df = pd.DataFrame(data)

    # Sauvegarder le DataFrame dans un fichier CSV
    df.to_csv(f"include/data/fake-data.csv", index=False)    

if __name__ == "__main__":
    create_file()