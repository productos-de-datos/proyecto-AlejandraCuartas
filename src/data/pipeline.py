"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


"""
import luigi
from create_data_lake import create_data_lake
from ingest_data import ingest_data
from transform_data import transform_data
from clean_data import clean_data
from compute_daily_prices import compute_daily_prices
from compute_monthly_prices import compute_monthly_prices


class create_structure(luigi.Task):
    """
    Crea la estructura
    """
    def output(self):
        return []

    def run(self):
        create_data_lake()

class ingest_data(luigi.Task):
    """
    Recupera los datos desde un archivo externo
    """
    def output(self):
        return []

    def run(self):
        ingest_data()

class transform_data(luigi.Task):
    """
    Transforma los datos y los
    consolida en un unico archivo
    """
    def output(self):
        return []

    def run(self):
        transform_data()

class clean_data(luigi.Task):
    """
    La función limpia y estructura los datos
    """
    def output(self):
        return []

    def run(self):
        clean_data()

class compute_daily_prices(luigi.Task):
    """
    Computa los precios por dia
    """
    def output(self):
        return []

    def run(self):
        compute_daily_prices()

class compute_monthly_prices(luigi.Task):
    """
    Computa los precios por mes
    """
    def output(self):
        return []

    def run(self):
        compute_monthly_prices()

def pipeline():
    """
    Orquesta y ejecuta el pipeline
    """
    luigi.build([create_structure(), ingest_data(), transform_data(), clean_data(),
    compute_daily_prices(), compute_monthly_prices() ],  local_scheduler=True)

if __name__ == "__main__":
    import doctest
    pipeline()
    doctest.testmod()
