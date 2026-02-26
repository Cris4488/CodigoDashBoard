import datetime,requests,time,pandas as pd, os    
from general.generales import Get_Path_Documents, Get_Token_Azure, Get_Site_Id, Get_Drive_Id, Get_File_SH

tenant_id = os.getenv('TENANT_ID')
client_id = os.getenv('CLIENT_ID_SHAREPOINT')
client_secret = os.getenv('CLIENT_SECRET_SHAREPOINT')

print("Inició correctamente")
print("Compra Etiquetas")
directorio = Get_Path_Documents() + "\\Dashboard"
print(directorio)

def Request_Bases(directorio, tenant_id, client_id, client_secret):
    tiempo = time.time()
    headers = Get_Token_Azure(tenant_id, client_id, client_secret)
    site_id = Get_Site_Id(headers, "AbastecimientoPlaneacin")
    data, fincas, reglas_etiquetas, ShipLoc, aux_fincas = Get_Data(headers, site_id)
    print(fincas)

    # dias = Get_Arguments()
    # print("Dias " + str(dias))
    # user_graph = data.iat[6,1]
    # password_graph = data.iat[6,2]
    # token = Get_Token_Graph(user_graph, password_graph)
    # inv_etiq = Get_List_Inv_Etiquetas(headers, site_id, fincas)
    # consumo_etiq = Request_Consumo_Etiquetas(token, aux_fincas, reglas_etiquetas, dias, ShipLoc)
    # running, date_e = Request_Running(data, fincas, dias)
    # Combinar_Running(directorio, running, inv_etiq, consumo_etiq, date_e)
    # Actualizar_Plantilla()
    # print('Tiempo: ' + str(time.time() - tiempo))

def Get_Data(headers, site_id):
    drive_id_Documentos = Get_Drive_Id(headers, site_id, 'Documentos')
    drive_id_Proyectos = Get_Drive_Id(headers, site_id, 'Proyectos')
    data = pd.read_excel(Get_File_SH(headers, drive_id_Proyectos, "Bases", "Lista Productos.xlsx"), sheet_name="Excel2017", index_col=False)
    fincas = pd.read_excel(Get_File_SH(headers, drive_id_Documentos, "Bases", "Fincas.xlsx"), sheet_name="Fincas", index_col=False)
    aux_fincas = pd.read_excel(Get_File_SH(headers, drive_id_Documentos, "Bases", "Fincas.xlsx"), sheet_name="Auxiliar", index_col=False)
    reglas_etiquetas = pd.read_excel(Get_File_SH(headers, drive_id_Documentos, "Bases", "Color Etiquetas 2.xlsx"), sheet_name="Etiquetas", index_col=False)
    shipLoc = pd.read_excel(Get_File_SH(headers, drive_id_Documentos, "Bases", "Ship Location.xlsx"), sheet_name="ShipLoc", index_col=False)
    fincas["Poscosecha"] = fincas["Poscosecha"].str.upper()
    aux_fincas = aux_fincas[aux_fincas['Id Finca']!=0]
    print("Bases obtenidas correctamente...")
    return data, fincas, reglas_etiquetas, shipLoc, aux_fincas

# def Get_Arguments():
#     argumentos = sys.argv 
#     for arg in argumentos:
#         if "Dias-" in arg:
#             dias = int(arg.replace("Dias-", ""))
#     return dias

# def Get_List_Inv_Etiquetas(headers, site_id, fincas: pd.DataFrame):
#     inv_etiq = pd.DataFrame(Get_Items_Lista_sH(headers, site_id, "Inventario Etiquetas"))
#     inv_etiq = inv_etiq.rename(columns={'ItemInvEtiqueta': 'Item', 'FincaInvEtiqueta': 'Finca', 'CantidadInvEtiqueta': 'Closing'})
#     inv_etiq = inv_etiq[['Item', 'Finca', 'Closing']]
#     inv_etiq = inv_etiq.fillna("")
#     inv_etiq["Finca"] = inv_etiq["Finca"].str.upper().map(fincas.set_index("Poscosecha")["Finca IN"])
#     inv_etiq = inv_etiq.groupby(["Item", "Finca"], as_index=False).agg({"Closing": "sum"})
#     print("Inventario de Etiquetas de CO's obtenido correctamente...")
#     return inv_etiq

def Execute_Query(token, query: str, type_query: str):
    url_graph = 'https://apigrq-integrationhub.azurewebsites.net/graphql/'
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "PostmanRuntime/7.43.4",
        "Authorization": f"Bearer {token}"
    }
    response = requests.post(url_graph, json={"query": query}, headers=headers)
    response.raise_for_status()
    return pd.DataFrame(response.json().get("data", {}).get(type_query, []))

def Request_Consumo_Etiquetas(token, fincas, reglas_etiquetas, dias, ShipLoc,directorio):
    consumo_total = pd.DataFrame()
    i = 0
    while True:
        start_date = (datetime.datetime.today() + datetime.timedelta(i)).strftime("%m-%d-%Y")
        end_date = (datetime.datetime.today() + datetime.timedelta(i+15)).strftime("%m-%d-%Y")
        query = f"""
            query {{
                labelsConsumption(
                    where: {{start_date: "{start_date}", end_date: "{end_date}", countryIds: "0"}}
                ) {{
                    customer_number
                    department
                    farm_id
                    has_ethyblock
                    market_type_id
                    master_location_id
                    product_type
                    quantity
                    quantity_wo_pieces
                    sa_production_date
                    ship_location_id
                    ship_to_city
                }}
            }}
        """
        consumo = Execute_Query(token, query, 'labelsConsumption').fillna('')
        consumo_total = pd.concat([consumo_total, consumo], ignore_index=True)
        i += 15
        if i >= dias:
            break
    # consumo_total = Edit_Consumo_Etiquetas(consumo_total, reglas_etiquetas, fincas, ShipLoc)
    consumo_total.to_csv(directorio + '\\Bases\\Running\\Consumo Etiquetas.csv', index=False)
    print("Consumo de etiquetas obtenido correctamente...")
    return consumo_total

# def Edit_Consumo_Etiquetas(consumo: pd.DataFrame, reglas_etiquetas: pd.DataFrame, fincas: pd.DataFrame, ShipLoc: pd.DataFrame):
#     consumo = consumo.rename(columns={'customer_number': 'Cliente', 'ship_location_id': 'Destino', 'ship_to_city': 'Ciudad', 'sa_production_date': 'Fecha', 'product_type': 'Dry/Wet', 'quantity_wo_pieces': 'usage', 'department': 'Dept', 'master_location_id': 'Master Location', 'has_ethyblock': 'Ethyblock'})
#     consumo = consumo.astype({'Master Location': str, 'Destino': str})
#     consumo.loc[consumo['Master Location'] == "1", 'Master Location'] = 'Sunshine Bouquet'
#     consumo['Finca'] = consumo['farm_id'].astype(int).map(fincas.set_index('Id Finca')['Finca IN'])
#     consumo['Tipo'] = consumo['farm_id'].astype(int).map(fincas.set_index('Id Finca')['Tipo'])
#     consumo['Destino'] = consumo['Destino'].astype(int).map(ShipLoc.set_index('id')['Destino'])
#     consumo['Dry/Wet'] = consumo['Dry/Wet'].str.upper()
#     consumo['Fecha'] = pd.to_datetime(consumo["Fecha"])
#     consumo["Fecha"] = consumo["Fecha"].dt.strftime("%m/%d/%Y")
#     consumo = Reglas_Etiquetas(consumo, reglas_etiquetas)
#     consumo = consumo.groupby(['Item', 'Finca'], as_index=False).agg({'usage': 'sum'})
#     return consumo

# def Request_Running(auth, fincas, dias):
#     user2 = auth.iat[0,1]
#     password2 = auth.iat[0,2]
#     date_b = datetime.datetime.strptime(Date_Begin(0, 2), "%d/%m/%Y").strftime("%m-%d-%Y")
#     date_e = datetime.datetime.strptime(Date_End(dias, 2), "%d/%m/%Y").strftime("%m-%d-%Y")
#     print("Extrayendo Running de Material...")
#     url = f"/api/hardgood-running-inventory?dateFrom={date_b}&dateTo={date_e}&typeMaterial=1&itemNumber=0&department=,~SAM&materials=&type=5&country=1&farms=&requestDays=0&isSameDateReq=false&isDaysReqF=true&isSupplyDate=false&closingDay=0&calcClosingInv=true"
#     running = Extract_Base_Sunshine(user2, password2, url)
#     running1 = Edit_Running(running, fincas, False)
#     print("Extrayendo Running de Empaque...")
#     url = f"/api/hardgood-running-inventory?dateFrom={date_b}&dateTo={date_e}&typeMaterial=2&itemNumber=0&department=,~SAM&materials=&type=5&country=1&farms=&requestDays=0&isSameDateReq=false&isDaysReqF=true&isSupplyDate=false&closingDay=0&calcClosingInv=true"
#     running = Extract_Base_Sunshine(user2, password2, url)
#     running2 = Edit_Running(running, fincas, True)
#     running = pd.concat([running1, running2])
#     return running, date_e

# def Edit_Running(running, fincas, etiq):
#     fincas = pd.DataFrame(fincas)
#     running = pd.DataFrame(running)
#     running = running.rename(columns={"item_number": "Item", "hardgood_type": "Categoria", "farm_name": "Finca", "closing_inv": "Closing"})
#     running = running.astype({"Item": str, "Categoria": str, "Finca": str, "Closing": int, "request_purchase": int, "request": int, "transferred": int, "usage": int, "live_inv": int})
#     running["Finca"] = running["Finca"].str.upper().map(fincas.set_index("Poscosecha")["Finca IN"])
#     running["Ubicacion"] = running["Finca"].str.upper().map(fincas.set_index("Poscosecha")["Ubicación"])
#     running = running.groupby(["Item", "Finca"], as_index=False).agg({"Closing": "sum", "request_purchase": "sum", "live_inv": "sum", "usage": "sum"})
#     if etiq == True:
#         running["usage"] = 0
#     print("Running Descargado Correctamente...")
#     return running

# def Combinar_Running(directorio, running, inv_etiq, consumo_etiq, date_e):
#     ids = running.drop_duplicates(subset="Item").iloc[:, :4]
#     running = pd.merge(running, inv_etiq, how="outer", on=["Finca", "Item"])
#     running['Closing'] = running['Closing_x'].fillna(0) + running['Closing_y'].fillna(0)
#     running.drop(['Closing_x', 'Closing_y'], axis=1, inplace=True)
#     running = pd.merge(running, consumo_etiq, how="outer", on=["Finca", "Item"])
#     running['usage'] = running['usage_x'].fillna(0) + running['usage_y'].fillna(0)
#     running.drop(['usage_x', 'usage_y'], axis=1, inplace=True)
#     running["Modificado"] = datetime.datetime.now().strftime('%m-%d-%Y %H:%M:%S')
#     running["Fecha Maxima"] = date_e
#     running.to_csv(directorio + "\\Bases\\Running\\Running.csv", index=False)
#     ids.to_csv(directorio + "\\Bases\\Running\\Id's.csv", index=False)
#     print("Bases combinadas correctamente...")

# def Actualizar_Plantilla():
#     xlApp = w3c.Dispatch("Excel.Application")
#     xlApp.Run("Compra_Etiquetas.xlsb!Python.Actualizar_Running")
#     xlApp.Run("Compra_Etiquetas.xlsb!Python.Msg_Fin")

# Request_Bases(directorio, tenant_id, client_id, client_secret)