
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
import pandas as pd
def compute_daily_prices():

    routeTry = True
    try:
        df = pd.read_csv("./data_lake/cleansed/precios-horarios.csv")
    except:
        routeTry = False
        df= pd.read_csv("../../data_lake/cleansed/precios-horarios.csv")
    df= df.groupby('fecha', as_index=False).mean()
    df= df[['fecha','precio']]
    route = "./data_lake/business/precios-diarios.csv" if routeTry else "../../data_lake/business/precios-diarios.csv"
    df.to_csv(route, index=False)
    print(df.head())

if __name__ == "__main__":
    import doctest
    compute_daily_prices()
    doctest.testmod()