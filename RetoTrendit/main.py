import os
import pandas as pd
from datetime import datetime


def c_archivos(c_entrada, c_salida, p_salida='combinado'):
    if not os.path.exists(c_salida):
        os.makedirs(c_salida)

    dataframes = []
     # Lectura de archivos
    for archivo in os.listdir(c_entrada):
        r_archivo = os.path.join(c_entrada, archivo)

        try:
            if archivo.endswith('.csv'):
                df = pd.read_csv(r_archivo)
            elif archivo.endswith('.json'):
                df = pd.read_json(r_archivo)
            elif archivo.endswith('.xml'):
                df = pd.read_xml(r_archivo)
            else:
                continue

            dataframes.append(df)
        except:
            print(f"No se pudo procesar el archivo: {archivo}")

    if not dataframes:
        print("No se encontraron archivos para combinar.")
        return
         # Creacion de archivo
    combinado = pd.concat(dataframes, ignore_index=True)
    f_actual = datetime.now().strftime("%Y-%m-%d")
    n_salida = f"{p_salida}_{f_actual}.csv"
    r_salida = os.path.join(c_salida, n_salida)
    combinado.to_csv(r_salida, index=False)

    print(f"Archivo guardado en: {r_salida}")


    # Ventas por mes
    try:
        combinado['fecha'] = pd.to_datetime(combinado['fecha'])
        combinado['mes'] = combinado['fecha'].dt.to_period('M')
        ventas_por_mes = (
        combinado
        .groupby(['producto_id', 'mes'], as_index=False)
        .agg(t_cantidad=('cantidad', 'sum'))
    )
        ventas_por_mes.to_csv(os.path.join(c_salida, f"VENTAS_MES_{f_actual}.csv"), index=False)
        print(f"Archivo guardado: {os.path.join(c_salida, f'ventas_por_mes_{f_actual}.csv')}")
    except:
        print("Error")

    # Produtos top
    try:
        t_productos = (
            combinado
            .groupby('producto_id', as_index=False)      
            .agg(total_cantidad=('cantidad', 'sum'))
            .sort_values(by='total_cantidad', ascending=False)
            .head(10)
        )

        t_productos.to_csv(os.path.join(c_salida, f"PRODUCTOS_TOP_{f_actual}.csv"), index=False)
        print(f"Archivo guardado: {os.path.join(c_salida, f'PRODUCTOS_TOP_{f_actual}.csv')}")
    except:
        print("Error al generar el archivo de top 10 productos.")




if __name__ == "__main__":
    c_archivos("./raw", "./transform_data", "CONSOLIDACION")


