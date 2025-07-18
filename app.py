######################################### LIBRERIAS ############################################

import re
import os
import json
import boto3
import unicodedata
import pandas as pd
from dotenv import load_dotenv
from datetime import timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from flask import Flask, render_template, request 

########################################## AMBIENTE ############################################

load_dotenv()

######################################## CARGA VARIABLES #######################################

S3_SA_KEY              = os.getenv("S3_SA_KEY")
SHEET_ID               = os.getenv("SHEET_ID")
SCOPES                 = json.loads(os.getenv("SCOPES"))
S3_BUCKET              = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY         = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY         = os.getenv("AWS_SECRET_KEY")

################################# ACCESO A GOOGLE SHEETS #######################################

s3 = boto3.client("s3",aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)

def get_service_account_credentials():
    obj       = s3.get_object(Bucket=S3_BUCKET, Key=S3_SA_KEY)
    sa_info   = json.loads(obj["Body"].read().decode())
    creds_sa  = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return creds_sa

def build_sheets_service():
    creds = get_service_account_credentials()
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def fetch_sheet_data(service, sheet_name):
    rng  = f"{sheet_name}!A1:Z"
    vals = (service.spreadsheets()
                  .values()
                  .get(spreadsheetId=SHEET_ID, range=rng)
                  .execute()
                  .get("values", []))

    if not vals:
        return pd.DataFrame()

    headers, *data = vals                                                                 
    max_cols = max(len(headers), *(len(r) for r in data)) if data else len(headers)
    headers += [f"Col_{i}" for i in range(len(headers), max_cols)]                        
    padded  = [row + [None]*(max_cols - len(row)) for row in data]                        
    return pd.DataFrame(padded, columns=[h.strip() for h in headers])

############################################# DASHBOARD ##############################################

app = Flask(__name__)

DISPLAY_PAISES = {
    "mexico"          : "México",
    "espana"          : "España",          
    "estados unidos"  : "Estados Unidos",
    "chile"           : "Chile",
    "china"           : "China",
    "colombia"        : "Colombia",
    "ecuador"         : "Ecuador",
}

@app.template_filter("pais_display")
def pais_display(valor_normalizado: str) -> str:
    key = (valor_normalizado or "").strip().lower()
    return DISPLAY_PAISES.get(key, valor_normalizado.title())

@app.route("/", methods=["GET"])
def index():
    service   = build_sheets_service()
    df_LEADs  = fetch_sheet_data(service, "Respuestas de formulario 1")

    paises = (df_LEADs["Pais de origen."]
              .dropna()
              .unique()
              .tolist())
    paises.sort(key=lambda p: pais_display(p).lower())

    pais_sel_list = request.args.getlist("pais")          
    if not pais_sel_list:                                
        pais_sel_list = ["todos"]

    if "todos" in pais_sel_list:
        df_filt = df_LEADs.copy()
    else:
        df_filt = df_LEADs[df_LEADs["Pais de origen."].isin(pais_sel_list)].copy()

    ############################################# KPIs ##############################################
    
    # TOTAL DE LEADs #
    total_leads = len(df_filt)

    # PORCENTAJE DE VARIACION DE LEADS ACTUAL VS MES ANTERIOR #
    df_filt['Cual es la fecha del primer contacto?'] = pd.to_datetime(df_filt['Cual es la fecha del primer contacto?'],dayfirst=True,errors='coerce')
    df_filt['anio_mes'] = df_filt['Cual es la fecha del primer contacto?'].dt.to_period('M')
    resumen = (df_filt.groupby('anio_mes').size().to_frame('total_leads').sort_index())
    pct_camb = None
    if len(resumen) >= 2:                           
        total_actual = resumen.iloc[-1]['total_leads']
        total_pasado = resumen.iloc[-2]['total_leads']
        if total_pasado:                            
            pct_camb = (total_actual - total_pasado) / total_pasado * 100
    if pct_camb is None:
        arrow_icon   = ""
        color_class  = "text-gray-400"
        pct_display  = "—"
    else:
        arrow_icon  = "fa-arrow-up" if pct_camb >= 0 else "fa-arrow-down"
        color_class = "text-green-500" if pct_camb >= 0 else "text-red-500"
        pct_display = f"{abs(pct_camb):.1f}%"
        
    # NUEVOS LEADs EN LAS ULTIMAS 2 SEMANAS #
    hoy = pd.Timestamp.now().normalize()
    inicio_ult2 = hoy - timedelta(weeks=2)
    nuevos_ult2sem = (df_filt["Cual es la fecha del primer contacto?"] >= inicio_ult2).sum()
    
    # CALIDAD DE LOS LEADs #
    df_filt['estatus_clean'] = (df_filt['Estatus de la ultima cita'].astype(str).str.strip().str.lower())
    calificados = ['interes inicial', 'interes','seguimiento', 'reunion', 'coordinar','deriva', "a futuro", "interesado propuesta $"]
    tasa_calif = df_filt['estatus_clean'].isin(calificados).mean()
    tasa_calif = tasa_calif * 100
    tasa_calif = round(tasa_calif, 2)
    if tasa_calif >= 60:
        ring_color_1 = "#16a34a"       
    elif tasa_calif >= 50:
        ring_color_1 = "#eab308"       
    else:
        ring_color_1 = "#dc2626" 
    leads_calif_cnt = df_filt['estatus_clean'] \
                    .isin(calificados) \
                    .sum()
    
    # INTERESADOS PROPUESTA #
    df_filt['interesado_clean'] = (df_filt['Estatus de la ultima cita'].astype(str).str.strip().str.lower())
    propuesta_ok = ['interesado propuesta $']
    mask_prop = df_filt['interesado_clean'].isin(propuesta_ok)
    kpi_prop_pct   = round(mask_prop.mean() * 100, 2)  
    kpi_prop_total = int(mask_prop.sum())                                  
    if kpi_prop_pct >= 30:
        ring_color_2 = "#16a34a"       
    elif kpi_prop_pct >= 20:
        ring_color_2 = "#eab308"       
    else:
        ring_color_2 = "#dc2626"
    
    ############################################# GRAFICAS ##############################################
    
    # GRAFICA DE LEADs POR MES #
    fecha_col = "Cual es la fecha del primer contacto?"
    df_filt[fecha_col] = pd.to_datetime(df_filt[fecha_col], dayfirst=True, errors='coerce')
    df_valid          = df_filt.dropna(subset=[fecha_col]).copy()
    df_valid["month"] = df_valid[fecha_col].dt.to_period("M").dt.to_timestamp()
    total_counts      = df_valid.groupby("month").size()
    mask_interesado   = df_valid["Estatus de la ultima cita"].str.contains(
                            r"interesado propuesta \$", case=False, na=False)
    interesado_counts = (df_valid[mask_interesado]
                        .groupby("month").size()
                        .reindex(total_counts.index, fill_value=0))
    MESES_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    chart_labels = [f"{MESES_ES[d.month-1]} {d.year}" for d in total_counts.index]
    chart_total_data      = total_counts.tolist()
    chart_interesado_data = interesado_counts.tolist()
    
    # GRAFICA DE LEADs POR FUENTE #
    resumen_fuente = (df_filt["Este LEAD es:"].value_counts(dropna=False).rename_axis("Fuente").reset_index(name="Total"))
    resumen_fuente["Porcentaje"] = (resumen_fuente["Total"] / resumen_fuente["Total"].sum() * 100).round(0).astype(int)
    mask_propuesta = df_filt["Estatus de la ultima cita"].eq("interesado propuesta $")
    conteo_propuesta = (df_filt[mask_propuesta].groupby("Este LEAD es:").size()) 
    resumen_fuente["Propuestas"] = (resumen_fuente["Fuente"].map(conteo_propuesta).fillna(0).astype(int))
    resumen_fuente = resumen_fuente[["Fuente", "Total", "Porcentaje", "Propuestas"]]
    resumen_fuente = resumen_fuente.to_dict(orient="records")
    
    # DONUT DE ESTATUS #
    def limpiar_estatus(x: str) -> str:
        if pd.isna(x):
            return 'Sin estatus'
        x = unicodedata.normalize('NFKD', str(x).strip())\
                    .encode('ascii', errors='ignore')\
                    .decode('utf‑8')\
                    .lower()
        
        agrupaciones = {
            'no interesado': 'No interesado',
            'nointeresado':  'No interesado',
            'no_interesado': 'No interesado',
            'no ha respondido': 'No ha respondido',
            'noharespondido':   'No ha respondido',
            'sin respuesta':    'No ha respondido',
            'no responde':      'No ha respondido',
            'no respondio':     'No ha respondido',
        }
        
        return agrupaciones.get(x, x.title())

    df_filt['Estatus limpio'] = df_filt['Estatus de la ultima cita'].apply(limpiar_estatus)
    estatus_counts  = df_filt['Estatus limpio'].value_counts(dropna=False)
    estatus_labels  = estatus_counts.index.tolist()
    estatus_values  = estatus_counts.values.tolist()
    
    # TOP LEADers #
    def normalizar_responsable(valor):
        if pd.isna(valor):
            return valor
        valor = str(valor).strip()
        valor = re.sub(r"\s+", " ", valor)
        valor = re.sub(r"\s*\|\s*", " | ", valor)
        partes = [p.strip().title() for p in valor.split("|")]
        return " | ".join(partes) if len(partes) > 1 else partes[0]
    col_resp    = "Perfil o responsable de origen."
    col_estatus = "Estatus de la ultima cita"
    df_filt["responsable_norm"] = df_filt[col_resp].apply(normalizar_responsable)
    mask_propuesta = (df_filt[col_estatus].fillna("").str.lower().str.contains(r"interesado\s*propuesta\s*\$", regex=True))
    mask_a_futuro = (df_filt[col_estatus].fillna("").str.lower().str.contains(r"a\s*futuro", regex=True))
    
    top_df = (                      
        df_filt.assign(es_propuesta=mask_propuesta, es_a_futuro=mask_a_futuro)
            .groupby("responsable_norm")
            .agg(Total_LEADs=("responsable_norm", "size"),
                    Propuestas=("es_propuesta", "sum"),
                    A_futuro=("es_a_futuro", "sum"))
            .sort_values("Total_LEADs", ascending=False)
            .reset_index()
    )
    max_leads           = top_df["Total_LEADs"].max() or 1
    top_df["pct_leads"] = (top_df["Total_LEADs"] / max_leads * 100).round(1)
    top_df["pct_prop"]  = (top_df["Propuestas"]  / max_leads * 100).round(1)
    top_df["pct_futuro"] = (top_df["A_futuro"] / max_leads * 100).round(1)
    top_performers = top_df.to_dict("records") 
    
    # 4 LEADs RECIENTES #
    columnas = ["Nombre de prospecto.", "Cargo del prospecto.", "Nombre de la empresa", "Sector de la empresa", "Estatus de la ultima cita", "Ultima fecha de seguimiento", "Correo electronico", "Numero telefonico", "Comentario de seguimiento", "Perfil o responsable de origen.", "Servicios de interes actualizacion:", "Pais de origen."]
    df_contactos = df_filt[columnas].copy()
    
    def first_last(s: str) -> str:
        partes = s.strip().split()                 
        return " ".join([partes[0], partes[-1]])   

    df_contactos["Nombre de prospecto."] = (df_contactos["Nombre de prospecto."].apply(first_last))
    
    pat = r'(\d{1,2}/\d{1,2}/\d{4})'

    df_contactos["Fecha Ultimo Contacto"] = (
        df_contactos["Comentario de seguimiento"]        
        .str.findall(pat)                              
        .apply(                                         
            lambda fechas:                              
                pd.to_datetime(fechas, dayfirst=True).max() if fechas else pd.NaT
        )
    )

    df_contactos["Fecha Ultimo Contacto"] = (df_contactos["Fecha Ultimo Contacto"].dt.strftime("%d/%m/%Y"))


    df_contactos["Fecha Ultimo Contacto"] = pd.to_datetime(
        df_contactos["Fecha Ultimo Contacto"],
        format="%d/%m/%Y",   
        dayfirst=True       
    )
    df_contactos["Fecha Ultimo Contacto"] = (pd.to_datetime(df_contactos["Fecha Ultimo Contacto"]).dt.date)
    
    status_to_color = {
        'Setter'                   : '#0c376c',
        'No interesado'            : '#f43f5e',
        'No ha respondido'         : '#cbd5e1',
        'Seguimiento'              : '#1e56a0',
        'A futuro'                 : '#94a3b8',
        'Interes inicial'          : '#38bdf8',
        'interesado propuesta $'   : '#34d399',
        'Reunion'                  : '#9333ea',
        'Interes'                  : '#0ea5e9',
        'Deriva'                   : '#f97316',
        'Venta'                    : '#16a34a',
        'Coordinar'                : '#c084fc'
    }

    df_contactos['Color'] = (df_contactos['Estatus de la ultima cita'].str.strip().map(status_to_color).fillna('#d1d5db'))

    recent_leads = df_contactos.dropna(subset=["Fecha Ultimo Contacto"])\
    .sort_values("Fecha Ultimo Contacto", ascending=False)\
    .head(4)\
    .to_dict(orient='records')

    # TABLA COMPLETA DE LEADs #
    desired_order = [
        "Nombre de la empresa",                 # 0
        "Nombre de prospecto.",                 # 1
        "Cargo del prospecto.",                 # 2
        "Sector de la empresa",                 # 3
        "Estatus de la ultima cita",            # 4
        "Fecha Ultimo Contacto",                # 5
        "Correo electronico",                   # 6
        "Numero telefonico",                    # 7
        "Perfil o responsable de origen.",      # 8
        "Servicios de interes actualizacion:",  # 9
        "Pais de origen.",                      # 10
        "Comentario de seguimiento",            # 11
        "Ultima fecha de seguimiento",          # 12
        "Color"                                 # 13
    ]
    tabla_leads_full = (df_contactos.reindex(columns=desired_order).fillna(""))
    tabla_leads_columns    = desired_order
    tabla_leads_full_records = tabla_leads_full.to_dict(orient="records")

    # DISTRIBUCION DE SERVICIOS DE INTERES #
    df_filt['Servicios de interes actualizacion:'] = (df_filt['Servicios de interes actualizacion:'].str.strip().str.replace(r'do it right.*', 'Do it Right', flags=re.I, regex=True))
    df_filt['Estatus de la ultima cita'] = df_filt['Estatus de la ultima cita'].str.strip()

    def clasificar_status(txt: str) -> str:
        if re.search(r'interesado.*propuesta', str(txt), flags=re.I):
            return 'Interesado propuesta $'
        elif re.search(r'a futuro', str(txt), flags=re.I):
            return 'A futuro'
        else:
            return 'Otros'

    df_filt['Status_cat'] = df_filt['Estatus de la ultima cita'].apply(clasificar_status)
    tabla_serv_status = (pd.crosstab(df_filt['Servicios de interes actualizacion:'],df_filt['Status_cat']).reindex(columns=['Interesado propuesta $', 'A futuro', 'Otros'], fill_value=0))

    servicios_labels = tabla_serv_status.index.tolist()
    servicios_interesado = tabla_serv_status['Interesado propuesta $'].tolist()
    servicios_futuro      = tabla_serv_status['A futuro'].tolist()
    servicios_otro        = tabla_serv_status['Otros'].tolist()
        
   ############################################# VARIABLES A ENVIAR ##############################################

    return render_template(
        "dashboard-de-LEADs.html",
        paises=paises,
        pais_sel_list=pais_sel_list,
        # ---- KPIs ----
        total_leads      = total_leads,
        pct_display      = pct_display,
        arrow_icon       = arrow_icon,
        color_class      = color_class,
        nuevos_ult2sem   = nuevos_ult2sem,
        tasa_calif       = tasa_calif,
        leads_calif_cnt  = leads_calif_cnt,
        kpi_prop_pct     = kpi_prop_pct,
        kpi_prop_total   = kpi_prop_total,
        ring_color_2     = ring_color_2,
        ring_color_1     = ring_color_1,
        # ---- Gráficas ----
        chart_labels          = chart_labels,
        chart_total_data      = chart_total_data,
        chart_interesado_data = chart_interesado_data,
        resumen_fuente=resumen_fuente,
        estatus_counts=estatus_counts,
        estatus_labels=estatus_labels,
        estatus_values=estatus_values,
        top_performers=top_performers,
        servicios_labels=servicios_labels,
        servicios_interesado=servicios_interesado,
        servicios_futuro=servicios_futuro,
        servicios_otro=servicios_otro,
        # ---- Tablas ----
        recent_leads       = recent_leads,
        tabla_leads_full   = tabla_leads_full_records,
        tabla_leads_columns= tabla_leads_columns,
    )

######################################### EJECUTADOR #############################################
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
