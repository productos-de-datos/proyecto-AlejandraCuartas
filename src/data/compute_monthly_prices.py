"""Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
import pandas as pd
def compute_monthly_prices():

    routeTry = True
    try:
        df= pd.read_csv("./data_lake/cleansed/precios-horarios.csv")
    except:
        routeTry = False
        df= pd.read_csv("../../data_lake/cleansed/precios-horarios.csv")
    df["year-month"] = df["fecha"].map(lambda x: str(x)[0:7])

    df = df.groupby('year-month', as_index=False).mean()
    df = df[['year-month','precio']]
    df = df.rename(columns= {'year-month': 'fecha'})
    df["fecha"] =  df["fecha"].map(lambda x: x + str("-01"))
    route = "./data_lake/business/precios-mensuales.csv" if routeTry else "../../data_lake/business/precios-mensuales.csv"
    df.to_csv(route, index=False)

if __name__ == "__main__":
    import doctest
    compute_monthly_prices()
    doctest.testmod()