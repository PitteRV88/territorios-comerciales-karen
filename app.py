# =============================================================
# app.py (v2) - Dashboard Territorios Comerciales Karen
# Streamlit Community Cloud edition (RSA key-pair auth)
# Incluye: KPIs, graficas, Top 10/15, detalle paginado,
#   tarjeta popup por cuenta, pitch IA EGOS BI, mailto
# Actualizado: 2026-03-25 | Proyecto: EGOS BI
# =============================================================

import urllib.parse
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(
    page_title="Territorios Comerciales - Karen",
    page_icon="\U0001f5fa\ufe0f",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB = "DB_TERRITORIOS_COMERCIALES"


# =============================================================
# CONEXION (RSA key-pair para Streamlit Cloud)
# =============================================================

def get_connection():
    """Conexion a Snowflake via RSA key-pair (st.secrets)."""
    pk_b64 = st.secrets["snowflake"]["private_key"]
    pk_der = base64.b64decode(pk_b64)
    private_key = serialization.load_der_private_key(pk_der, password=None, backend=default_backend())
    pk_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=pk_bytes,
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
    )


# =============================================================
# FUNCIONES HELPER
# =============================================================

def make_mailto(email, subject="", body=""):
    """Genera un mailto: link URL-encoded."""
    if not email or not str(email).strip():
        return None
    email = str(email).strip()
    params = {}
    if subject:
        params["subject"] = subject
    if body:
        params["body"] = body
    if params:
        return f"mailto:{email}?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    return f"mailto:{email}"


def email_link_md(email):
    """Retorna markdown con mailto link para un email."""
    if not email or not str(email).strip():
        return ""
    e = str(email).strip()
    return f"[{e}](mailto:{e})"


# =============================================================
# CARGA DE DATOS
# =============================================================

@st.cache_data(ttl=300)
def load_data():
    """Carga datos principales: cuentas + mejor contacto + industria."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        WITH best_contact AS (
            SELECT *
            FROM {DB}.CORE.DIM_CONTACTOS
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CUENTA_ID
                ORDER BY PRIORIDAD ASC, HAS_EMAIL DESC, CREATED_AT DESC
            ) = 1
        )
        SELECT c.CUENTA_ID, c.ACCT_NAME, i.INDUSTRIA_NOMBRE,
               t.NOMBRE AS TERRITORIO, e.NOMBRE AS EJECUTIVO,
               c.ESTATUS, c.UBICACION, c.SITIO_WEB,
               c.CONTACTO_EMAIL, c.CONTACTO_TELEFONO,
               c.PERSONA_INTERES, c.CARGO_PERSONA, c.LINKEDIN_EMPRESA,
               c.FUENTE_CLASIFICACION, c.NOTAS, c.CREATED_AT, c.UPDATED_AT,
               c.NUM_EMPLEADOS_ESTIMADO, c.REVENUE_ESTIMADO_USD,
               c.TAMANO_EMPRESA, c.FUENTE_TAMANO,
               c.BILLING_STATE, c.BILLING_COUNTRY, c.INDUSTRIA_DETALLE,
               bc.NOMBRE_COMPLETO AS CONTACTO_PRINCIPAL,
               bc.CARGO AS CONTACTO_CARGO,
               bc.EMAIL AS CONTACTO_EMAIL_NUEVO,
               bc.LINKEDIN_PERFIL AS CONTACTO_LINKEDIN,
               bc.HAS_EMAIL AS CONTACTO_HAS_EMAIL
        FROM {DB}.CORE.DIM_CUENTAS c
        JOIN {DB}.CORE.DIM_INDUSTRIAS i ON c.INDUSTRIA_ID = i.INDUSTRIA_ID
        LEFT JOIN {DB}.CORE.DIM_TERRITORIOS t ON c.TERRITORIO_ID = t.TERRITORIO_ID
        LEFT JOIN {DB}.CORE.DIM_EJECUTIVOS e ON t.EJECUTIVO_ID = e.EJECUTIVO_ID
        LEFT JOIN best_contact bc ON c.CUENTA_ID = bc.CUENTA_ID
        ORDER BY c.ACCT_NAME
    """)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)

    cur.execute(f"""
        SELECT cu.CASO_ID, i.INDUSTRIA_NOMBRE, cu.TENDENCIAS_INDUSTRIA,
               cu.RETOS_PRINCIPALES, cu.CASOS_USO_SNOWFLAKE, cu.PROPUESTA_VALOR
        FROM {DB}.CORE.DIM_CASOS_USO cu
        JOIN {DB}.CORE.DIM_INDUSTRIAS i ON cu.INDUSTRIA_ID = i.INDUSTRIA_ID
    """)
    cols_cu = [desc[0] for desc in cur.description]
    rows_cu = cur.fetchall()
    df_casos = pd.DataFrame(rows_cu, columns=cols_cu)

    cur.close()
    conn.close()
    return df, df_casos


def load_contactos_cuenta(cuenta_id):
    """Carga todos los contactos de una cuenta especifica."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT CONTACTO_ID, CUENTA_ID, NOMBRE_COMPLETO, PRIMER_NOMBRE, APELLIDO,
               CARGO, NIVEL_CARGO, DEPARTAMENTO, EMAIL, TELEFONO,
               LINKEDIN_PERFIL, FUENTE, PRIORIDAD, HAS_EMAIL, HAS_PHONE
        FROM {DB}.CORE.DIM_CONTACTOS
        WHERE CUENTA_ID = %s
        ORDER BY PRIORIDAD ASC, HAS_EMAIL DESC, CREATED_AT DESC
    """, (cuenta_id,))
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


# =============================================================
# DIALOG: TARJETA DE DETALLE DE CUENTA (popup)
# =============================================================

@st.dialog("Detalle de Cuenta", width="large")
def mostrar_tarjeta_cuenta(acct_name):
    """Dialog popup con detalle completo de una cuenta + contactos + pitch IA."""
    row = df[df["ACCT_NAME"] == acct_name]
    if row.empty:
        st.error("Cuenta no encontrada.")
        return
    row = row.iloc[0]
    cuenta_id = int(row["CUENTA_ID"])

    # -- Tarjeta de datos principales --
    st.markdown(f"### {acct_name}")

    dc1, dc2, dc3, dc4 = st.columns(4)
    dc1.metric("Industria", row["INDUSTRIA_NOMBRE"])
    dc2.metric("Tamano", row["TAMANO_EMPRESA"] or "N/A")
    dc3.metric("Empleados Est.", f"{int(row['NUM_EMPLEADOS_ESTIMADO']):,}" if row["NUM_EMPLEADOS_ESTIMADO"] else "N/A")
    dc4.metric("Revenue Est.", f"${float(row['REVENUE_ESTIMADO_USD'])/1e6:,.1f}M" if row["REVENUE_ESTIMADO_USD"] else "N/A")

    dc5, dc6, dc7, dc8 = st.columns(4)
    if row.get("INDUSTRIA_DETALLE") and str(row["INDUSTRIA_DETALLE"]).strip():
        dc5.markdown(f"**Sub-industria:** {row['INDUSTRIA_DETALLE']}")
    else:
        dc5.markdown("**Sub-industria:** N/A")
    ubicacion_parts = []
    if row.get("BILLING_STATE") and str(row["BILLING_STATE"]).strip():
        ubicacion_parts.append(str(row["BILLING_STATE"]))
    if row.get("BILLING_COUNTRY") and str(row["BILLING_COUNTRY"]).strip():
        ubicacion_parts.append(str(row["BILLING_COUNTRY"]))
    dc6.markdown(f"**Ubicacion:** {', '.join(ubicacion_parts) if ubicacion_parts else 'N/A'}")
    if row["SITIO_WEB"] and str(row["SITIO_WEB"]).strip():
        dc7.markdown(f"**Web:** [{row['SITIO_WEB']}]({row['SITIO_WEB']})")
    else:
        dc7.markdown("**Web:** N/A")
    if row["LINKEDIN_EMPRESA"] and str(row["LINKEDIN_EMPRESA"]).strip():
        dc8.markdown(f"**LinkedIn:** [Ver perfil]({row['LINKEDIN_EMPRESA']})")
    else:
        dc8.markdown("**LinkedIn:** N/A")

    if row.get("TERRITORIO") and str(row["TERRITORIO"]).strip():
        st.markdown(f"**Territorio:** {row['TERRITORIO']} (Ejecutivo: {row['EJECUTIVO']})")
    if row["NOTAS"] and str(row["NOTAS"]).strip():
        st.info(f"**Notas:** {row['NOTAS']}")

    # -- Contacto principal --
    st.markdown("---")
    st.markdown("**Contacto Principal**")
    if row.get("CONTACTO_PRINCIPAL") and str(row["CONTACTO_PRINCIPAL"]).strip():
        cp_parts = [f"**{row['CONTACTO_PRINCIPAL']}**"]
        if row.get("CONTACTO_CARGO") and str(row["CONTACTO_CARGO"]).strip():
            cp_parts.append(f"{row['CONTACTO_CARGO']}")
        if row.get("CONTACTO_EMAIL_NUEVO") and str(row["CONTACTO_EMAIL_NUEVO"]).strip():
            cp_parts.append(email_link_md(row["CONTACTO_EMAIL_NUEVO"]))
        if row.get("CONTACTO_LINKEDIN") and str(row["CONTACTO_LINKEDIN"]).strip():
            cp_parts.append(f"[LinkedIn]({row['CONTACTO_LINKEDIN']})")
        st.markdown(" | ".join(cp_parts))
    elif row.get("PERSONA_INTERES") and str(row["PERSONA_INTERES"]).strip():
        st.markdown(f"**{row['PERSONA_INTERES']}** | {row.get('CARGO_PERSONA') or 'N/A'} *(dato original de cuenta)*")
    else:
        st.warning("Sin contacto registrado para esta cuenta.")

    # -- Lista de contactos adicionales --
    df_contactos = load_contactos_cuenta(cuenta_id)
    if not df_contactos.empty and len(df_contactos) > 1:
        st.markdown("**Otros contactos:**")
        for idx, ct in df_contactos.iloc[1:].iterrows():
            email_md = email_link_md(ct["EMAIL"])
            has_e = " [E]" if ct["HAS_EMAIL"] else ""
            st.markdown(
                f"- {ct['NOMBRE_COMPLETO']}{has_e} | {ct['CARGO'] or 'N/A'} | {ct['NIVEL_CARGO'] or ''} "
                f"{'| ' + email_md if email_md else ''}"
            )

    # -- Generar Pitch con IA --
    st.markdown("---")
    st.markdown("**Generar Pitch con IA**")

    industria_cuenta = row["INDUSTRIA_NOMBRE"]
    caso_industria = df_casos[df_casos["INDUSTRIA_NOMBRE"] == industria_cuenta]
    insights_ctx = ""
    if not caso_industria.empty:
        ci = caso_industria.iloc[0]
        insights_ctx = (
            f"\nInsights de la industria {industria_cuenta}:"
            f"\nTendencias: {str(ci['TENDENCIAS_INDUSTRIA'])[:300]}"
            f"\nRetos: {str(ci['RETOS_PRINCIPALES'])[:300]}"
            f"\nCasos de uso Snowflake: {str(ci['CASOS_USO_SNOWFLAKE'])[:300]}"
            f"\nPropuesta de valor: {str(ci['PROPUESTA_VALOR'])[:200]}"
        )

    contexto = f"Empresa: {row['ACCT_NAME']}\nIndustria: {row['INDUSTRIA_NOMBRE']}"
    if row.get("INDUSTRIA_DETALLE") and str(row["INDUSTRIA_DETALLE"]).strip():
        contexto += f" ({row['INDUSTRIA_DETALLE']})"
    if row.get("TAMANO_EMPRESA") and str(row["TAMANO_EMPRESA"]).strip():
        contexto += f"\nTamano: {row['TAMANO_EMPRESA']}"
    if row.get("NUM_EMPLEADOS_ESTIMADO") and row["NUM_EMPLEADOS_ESTIMADO"]:
        contexto += f" (~{int(row['NUM_EMPLEADOS_ESTIMADO'])} empleados)"
    if row.get("REVENUE_ESTIMADO_USD") and row["REVENUE_ESTIMADO_USD"]:
        contexto += f"\nRevenue estimado: ${float(row['REVENUE_ESTIMADO_USD'])/1e6:,.1f}M USD"
    if row.get("BILLING_STATE") and str(row["BILLING_STATE"]).strip():
        contexto += f"\nEstado: {row['BILLING_STATE']}"
    if row.get("BILLING_COUNTRY") and str(row["BILLING_COUNTRY"]).strip():
        contexto += f"\nPais: {row['BILLING_COUNTRY']}"
    if row["SITIO_WEB"] and str(row["SITIO_WEB"]).strip():
        contexto += f"\nSitio web: {row['SITIO_WEB']}"
    if row["LINKEDIN_EMPRESA"] and str(row["LINKEDIN_EMPRESA"]).strip():
        contexto += f"\nLinkedIn empresa: {row['LINKEDIN_EMPRESA']}"
    if row.get("CONTACTO_PRINCIPAL") and str(row["CONTACTO_PRINCIPAL"]).strip():
        contexto += f"\nContacto principal: {row['CONTACTO_PRINCIPAL']}"
    if row.get("CONTACTO_CARGO") and str(row["CONTACTO_CARGO"]).strip():
        contexto += f"\nCargo contacto: {row['CONTACTO_CARGO']}"
    if row.get("CONTACTO_EMAIL_NUEVO") and str(row["CONTACTO_EMAIL_NUEVO"]).strip():
        contexto += f"\nEmail contacto: {row['CONTACTO_EMAIL_NUEVO']}"
    elif row.get("CONTACTO_EMAIL") and str(row["CONTACTO_EMAIL"]).strip():
        contexto += f"\nEmail contacto: {row['CONTACTO_EMAIL']}"
    contexto += insights_ctx

    ejecutivo_nombre = str(row["EJECUTIVO"]).strip() if row.get("EJECUTIVO") and str(row["EJECUTIVO"]).strip() else "el equipo"

    if st.button("Generar Pitch y Correo", key=f"pitch_gen_{cuenta_id}", type="primary"):
        with st.spinner("Generando pitch personalizado con Cortex AI..."):
            try:
                conn_ai = get_connection()
                cur_ai = conn_ai.cursor()

                nombre_contacto = ""
                if row.get("CONTACTO_PRINCIPAL") and str(row["CONTACTO_PRINCIPAL"]).strip():
                    nombre_contacto = str(row["CONTACTO_PRINCIPAL"]).strip().split()[0]
                elif row.get("PERSONA_INTERES") and str(row["PERSONA_INTERES"]).strip():
                    nombre_contacto = str(row["PERSONA_INTERES"]).strip().split()[0]

                saludo = f"dirigido a '{nombre_contacto}'" if nombre_contacto else "con un saludo cordial"

                prompt_pitch = (
                    f"Eres {ejecutivo_nombre} de EGOS BI, partner de Snowflake en Mexico. "
                    f"EGOS BI es una consultora especializada en arquitectura de datos moderna: "
                    f"ayudamos a las organizaciones a migrar, integrar y transformar sus datos en la nube con Snowflake, "
                    f"logrando decisiones mas rapidas, escalabilidad y reduccion de costos. "
                    f"Genera un mensaje de prospeccion personalizado en espanol (5-6 oraciones) para esta cuenta. "
                    f"Estructura: "
                    f"1) Saludo {saludo}, breve y amigable. "
                    f"2) Una o dos lineas sobre que hace EGOS BI (arquitectura de datos moderna con Snowflake). "
                    f"3) Menciona un reto o tendencia relevante de su industria ({industria_cuenta}) que enfrentan hoy. "
                    f"4) Explica brevemente como Snowflake con ayuda de EGOS BI puede ayudar a su organizacion a resolver ese reto. "
                    f"5) Cierra con un tono amigable invitando a una llamada corta, algo como "
                    f"'Que te parece si agendamos una llamada de 20 minutos para intercambiar ideas?' "
                    f"o similar. NO pidas una demo. Tono: profesional, cercano, sin ser agresivo. "
                    f"Datos:\n{contexto}"
                )
                cur_ai.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)", (prompt_pitch,))
                pitch = cur_ai.fetchone()[0]
                cur_ai.close()
                conn_ai.close()

                st.markdown(f"**Pitch para {acct_name}** ({industria_cuenta})")
                st.write(pitch)

                # Mailto link
                contact_email = ""
                if row.get("CONTACTO_EMAIL_NUEVO") and str(row["CONTACTO_EMAIL_NUEVO"]).strip():
                    contact_email = str(row["CONTACTO_EMAIL_NUEVO"]).strip()
                elif row.get("CONTACTO_EMAIL") and str(row["CONTACTO_EMAIL"]).strip():
                    contact_email = str(row["CONTACTO_EMAIL"]).strip()

                if contact_email:
                    subject = f"Arquitectura de datos moderna para {acct_name} - EGOS BI"
                    pitch_body = str(pitch).replace('"', '').replace("'", "").replace('\r\n', '\n').replace('\r', '\n')
                    if len(pitch_body) > 1500:
                        pitch_body = pitch_body[:1500] + "..."
                    mailto_url = f"mailto:{contact_email}?{urllib.parse.urlencode({'subject': subject, 'body': pitch_body}, quote_via=urllib.parse.quote)}"
                    dest_name = nombre_contacto if nombre_contacto else contact_email
                    bc1, bc2 = st.columns(2)
                    with bc1:
                        st.link_button(f"Enviar email a {dest_name}", mailto_url, type="primary")
                    with bc2:
                        st.code(pitch, language=None)
                        st.caption("Copia el texto de arriba si el email se abre vacio.")
                else:
                    st.info("No hay email disponible para este contacto. Puedes copiar el pitch manualmente.")
                    st.code(pitch, language=None)
                    st.caption("Copia el pitch manualmente.")

            except Exception as e:
                st.error(f"Error al generar pitch: {e}")


# =============================================================
# CARGAR DATOS
# =============================================================

df, df_casos = load_data()

# =============================================================
# SIDEBAR: FILTROS
# =============================================================

with st.sidebar:
    st.header("Filtros")
    industrias = sorted(df["INDUSTRIA_NOMBRE"].unique())
    sel_industrias = st.multiselect("Industria", industrias, default=industrias)
    paises = sorted(df["BILLING_COUNTRY"].dropna().unique())
    sel_paises = st.multiselect("Pais", paises, default=paises)
    estados = sorted(df["BILLING_STATE"].dropna().unique())
    sel_estados = st.multiselect("Estado", estados, default=estados)
    territorios = ["Con Territorio", "Sin Territorio"]
    sel_territorio = st.multiselect("Territorio", territorios, default=territorios)
    fuentes = sorted(df["FUENTE_CLASIFICACION"].dropna().unique())
    sel_fuentes = st.multiselect("Fuente Clasificacion", fuentes, default=fuentes)
    sel_contacto = st.selectbox("Datos de Contacto", ["Todos", "Con Contacto", "Con LinkedIn", "Con Email", "Con Sitio Web", "Sin Datos"])
    tamanos = sorted(df["TAMANO_EMPRESA"].dropna().unique())
    sel_tamanos = st.multiselect("Tamano Empresa", tamanos, default=tamanos)
    buscar = st.text_input("Buscar cuenta", placeholder="Nombre de empresa...")

# =============================================================
# APLICAR FILTROS
# =============================================================

df_filtered = df[df["INDUSTRIA_NOMBRE"].isin(sel_industrias)]
df_filtered = df_filtered[df_filtered["BILLING_COUNTRY"].isin(sel_paises) | df_filtered["BILLING_COUNTRY"].isna()]
df_filtered = df_filtered[df_filtered["BILLING_STATE"].isin(sel_estados) | df_filtered["BILLING_STATE"].isna()]

if "Con Territorio" in sel_territorio and "Sin Territorio" not in sel_territorio:
    df_filtered = df_filtered[df_filtered["TERRITORIO"].notna()]
elif "Sin Territorio" in sel_territorio and "Con Territorio" not in sel_territorio:
    df_filtered = df_filtered[df_filtered["TERRITORIO"].isna()]

df_filtered = df_filtered[df_filtered["FUENTE_CLASIFICACION"].isin(sel_fuentes) | df_filtered["FUENTE_CLASIFICACION"].isna()]

if sel_contacto == "Con Contacto":
    df_filtered = df_filtered[df_filtered["CONTACTO_PRINCIPAL"].notna() & (df_filtered["CONTACTO_PRINCIPAL"] != "")]
elif sel_contacto == "Con LinkedIn":
    df_filtered = df_filtered[df_filtered["LINKEDIN_EMPRESA"].notna() & (df_filtered["LINKEDIN_EMPRESA"] != "")]
elif sel_contacto == "Con Email":
    df_filtered = df_filtered[df_filtered["CONTACTO_EMAIL"].notna() & (df_filtered["CONTACTO_EMAIL"] != "")]
elif sel_contacto == "Con Sitio Web":
    df_filtered = df_filtered[df_filtered["SITIO_WEB"].notna() & (df_filtered["SITIO_WEB"] != "")]
elif sel_contacto == "Sin Datos":
    df_filtered = df_filtered[
        (df_filtered["LINKEDIN_EMPRESA"].isna() | (df_filtered["LINKEDIN_EMPRESA"] == "")) &
        (df_filtered["CONTACTO_EMAIL"].isna() | (df_filtered["CONTACTO_EMAIL"] == "")) &
        (df_filtered["SITIO_WEB"].isna() | (df_filtered["SITIO_WEB"] == ""))
    ]

if buscar:
    df_filtered = df_filtered[df_filtered["ACCT_NAME"].str.contains(buscar, case=False, na=False)]

df_filtered = df_filtered[df_filtered["TAMANO_EMPRESA"].isin(sel_tamanos) | df_filtered["TAMANO_EMPRESA"].isna()]

# =============================================================
# TITULO Y KPIs
# =============================================================

st.title("Territorios Comerciales - Karen")
st.caption(f"DB_TERRITORIOS_COMERCIALES | {len(df_filtered)} de {len(df)} cuentas mostradas")

total = len(df_filtered)
con_territorio = df_filtered["TERRITORIO"].notna().sum()
con_linkedin = ((df_filtered["LINKEDIN_EMPRESA"].notna()) & (df_filtered["LINKEDIN_EMPRESA"] != "")).sum()
con_sitio = ((df_filtered["SITIO_WEB"].notna()) & (df_filtered["SITIO_WEB"] != "")).sum()
con_contacto = ((df_filtered["CONTACTO_PRINCIPAL"].notna()) & (df_filtered["CONTACTO_PRINCIPAL"] != "")).sum()
total_emp = int(df_filtered["NUM_EMPLEADOS_ESTIMADO"].fillna(0).sum())
n_paises = df_filtered["BILLING_COUNTRY"].dropna().nunique()
rev_by_ind = df_filtered.groupby("INDUSTRIA_NOMBRE")["REVENUE_ESTIMADO_USD"].apply(
    lambda x: float(x.fillna(0).sum())).sort_values(ascending=False)
top_ind_name = rev_by_ind.index[0] if len(rev_by_ind) > 0 else "N/A"
top_ind_rev = rev_by_ind.iloc[0] if len(rev_by_ind) > 0 else 0

with st.container(horizontal=True):
    st.metric("Total Cuentas", total, border=True)
    st.metric(f"Top Rev: {top_ind_name}", f"${top_ind_rev/1e6:,.0f}M", border=True)
    st.metric("Empleados Est. Total", f"{int(total_emp):,}", border=True)
    st.metric("Paises", n_paises, border=True)
    st.metric("Con Sitio Web", int(con_sitio), border=True)
    st.metric("Con LinkedIn", int(con_linkedin), border=True)
    st.metric("Con Contacto", int(con_contacto), border=True)

# =============================================================
# GRAFICAS
# =============================================================

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.subheader("Distribucion por Industria")
        ind_counts = df_filtered["INDUSTRIA_NOMBRE"].value_counts().reset_index()
        ind_counts.columns = ["Industria", "Cuentas"]
        fig_ind = px.bar(ind_counts, x="Cuentas", y="Industria", orientation="h",
                         color="Industria", text="Cuentas",
                         color_discrete_sequence=px.colors.qualitative.Set2)
        fig_ind.update_layout(showlegend=False, height=400, margin=dict(l=0, r=0, t=10, b=0))
        fig_ind.update_traces(textposition="outside")
        st.plotly_chart(fig_ind, use_container_width=True)

with col2:
    with st.container(border=True):
        st.subheader("Distribucion por Estado (Top 15)")
        state_counts = df_filtered["BILLING_STATE"].dropna().value_counts().head(15).reset_index()
        state_counts.columns = ["Estado", "Cuentas"]
        fig_state = px.bar(state_counts, x="Cuentas", y="Estado", orientation="h",
                           text="Cuentas", color="Cuentas",
                           color_continuous_scale="Viridis")
        fig_state.update_layout(showlegend=False, height=400, margin=dict(l=0, r=0, t=10, b=0),
                                coloraxis_showscale=False)
        fig_state.update_traces(textposition="outside")
        st.plotly_chart(fig_state, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    with st.container(border=True):
        st.subheader("Distribucion por Pais")
        country_counts = df_filtered["BILLING_COUNTRY"].dropna().value_counts().reset_index()
        country_counts.columns = ["Pais", "Cuentas"]
        fig_country = px.pie(country_counts, values="Cuentas", names="Pais",
                             color_discrete_sequence=px.colors.qualitative.Set3, hole=0.35)
        fig_country.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_country, use_container_width=True)

with col4:
    with st.container(border=True):
        st.subheader("Tamano de Empresa")
        tamano_order = sorted(df_filtered["TAMANO_EMPRESA"].dropna().unique())
        tamano_counts = df_filtered["TAMANO_EMPRESA"].value_counts().reindex(tamano_order).dropna().reset_index()
        tamano_counts.columns = ["Tamano", "Cuentas"]
        fig_tam = px.bar(tamano_counts, x="Tamano", y="Cuentas", text="Cuentas",
                         color="Tamano", color_discrete_sequence=px.colors.sequential.Viridis)
        fig_tam.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=10, b=0))
        fig_tam.update_traces(textposition="outside")
        st.plotly_chart(fig_tam, use_container_width=True)

# Heatmap
with st.container(border=True):
    st.subheader("Distribucion: Tamano por Industria (Heatmap)")
    tamano_ord = sorted(df_filtered["TAMANO_EMPRESA"].dropna().unique())
    df_heat = df_filtered[df_filtered["TAMANO_EMPRESA"].notna()].groupby(
        ["INDUSTRIA_NOMBRE", "TAMANO_EMPRESA"]).size().reset_index(name="Cuentas")
    if not df_heat.empty:
        df_pivot = df_heat.pivot(index="INDUSTRIA_NOMBRE", columns="TAMANO_EMPRESA", values="Cuentas").fillna(0)
        df_pivot = df_pivot.reindex(columns=[t for t in tamano_ord if t in df_pivot.columns])
        fig_heat = px.imshow(
            df_pivot.values, x=df_pivot.columns.tolist(), y=df_pivot.index.tolist(),
            color_continuous_scale="Viridis", text_auto=True,
            labels=dict(x="Tamano", y="Industria", color="Cuentas"),
            aspect="auto"
        )
        fig_heat.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_heat, use_container_width=True)

# =============================================================
# TOP 10 CUENTAS (con click para abrir tarjeta)
# =============================================================

st.divider()
st.subheader("Top 10 Cuentas Principales de Interes (Global)")
st.caption("Ranking global combinado: datos obtenidos (0-5) + tamano empresa (0-5) | Click en empresa para ver detalle")

tamano_score_map_g = {"Micro": 1, "Pequena": 2, "Pequeña": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
enriq_cols_g = ["SITIO_WEB", "UBICACION", "CONTACTO_PRINCIPAL", "LINKEDIN_EMPRESA", "CONTACTO_CARGO"]
df_global = df_filtered.copy()
df_global["ENRIQ_SCORE"] = df_global[enriq_cols_g].apply(
    lambda row: sum(1 for v in row if v and str(v).strip()), axis=1)
df_global["TAMANO_SCORE"] = df_global["TAMANO_EMPRESA"].map(tamano_score_map_g).fillna(0).astype(int)
df_global["SCORE_TOTAL"] = df_global["ENRIQ_SCORE"] + df_global["TAMANO_SCORE"]
df_top10_global = df_global.nlargest(10, ["SCORE_TOTAL", "TAMANO_SCORE", "ENRIQ_SCORE"])

col_g1, col_g2 = st.columns([2, 1])
with col_g1:
    with st.container(border=True):
        t10d = df_top10_global[["ACCT_NAME", "INDUSTRIA_NOMBRE", "TAMANO_EMPRESA",
                                 "ENRIQ_SCORE", "TAMANO_SCORE", "SCORE_TOTAL",
                                 "BILLING_STATE", "CONTACTO_PRINCIPAL"]].copy()
        t10d.columns = ["Empresa", "Industria", "Tamano", "Datos (0-5)", "Tamano (0-5)",
                        "Score", "Estado", "Contacto"]
        t10d = t10d.fillna("")
        hc = st.columns([2.5, 1.5, 1, 0.7, 0.7, 0.6, 1, 1.5])
        headers = ["Empresa", "Industria", "Tamano", "Datos", "Tam.", "Score", "Estado", "Contacto"]
        for col, h in zip(hc, headers):
            col.markdown(f"**{h}**")
        for i, (_, rw) in enumerate(t10d.iterrows()):
            rc = st.columns([2.5, 1.5, 1, 0.7, 0.7, 0.6, 1, 1.5])
            with rc[0]:
                if st.button(f":link: {rw['Empresa']}", key=f"t10_{i}", use_container_width=True):
                    st.session_state["_open_cuenta"] = rw["Empresa"]
                    st.rerun()
            rc[1].write(rw["Industria"])
            rc[2].write(rw["Tamano"])
            rc[3].write(str(rw["Datos (0-5)"]))
            rc[4].write(str(rw["Tamano (0-5)"]))
            rc[5].write(str(rw["Score"]))
            rc[6].write(rw["Estado"])
            rc[7].write(rw["Contacto"])

with col_g2:
    with st.container(border=True):
        st.markdown("**Composicion del Score**")
        fig_g10 = px.bar(
            t10d,
            y="Empresa", x=["Datos (0-5)", "Tamano (0-5)"],
            orientation="h", barmode="stack",
            color_discrete_sequence=["#3498db", "#2ecc71"],
            labels={"value": "Puntos", "variable": "Componente"}
        )
        fig_g10.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0),
                              yaxis=dict(autorange="reversed"), legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_g10, use_container_width=True)

# =============================================================
# TOP 15 CUENTAS POR INDUSTRIA
# =============================================================

st.divider()
st.subheader("Top 15 Cuentas de Interes por Industria")

ind_list_top = sorted(df_filtered["INDUSTRIA_NOMBRE"].unique())
sel_ind_top15 = st.selectbox("Selecciona industria para Top 15", ind_list_top,
                              index=ind_list_top.index("Manufacturing & Industrial") if "Manufacturing & Industrial" in ind_list_top else 0)
st.caption(f"Ranking combinado: datos obtenidos + tamano | Click en empresa para ver detalle — {sel_ind_top15}")

tamano_score_map = {"Micro": 1, "Pequena": 2, "Pequeña": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
df_top = df_filtered[df_filtered["INDUSTRIA_NOMBRE"] == sel_ind_top15].copy()
enriq_cols = ["SITIO_WEB", "UBICACION", "CONTACTO_PRINCIPAL", "LINKEDIN_EMPRESA", "CONTACTO_CARGO"]
df_top["ENRIQ_SCORE"] = df_top[enriq_cols].apply(
    lambda row: sum(1 for v in row if v and str(v).strip()), axis=1)
df_top["TAMANO_SCORE"] = df_top["TAMANO_EMPRESA"].map(tamano_score_map).fillna(0).astype(int)
df_top["SCORE_TOTAL"] = df_top["ENRIQ_SCORE"] + df_top["TAMANO_SCORE"]
df_top15 = df_top.nlargest(15, ["SCORE_TOTAL", "TAMANO_SCORE", "ENRIQ_SCORE"])

col_t1, col_t2 = st.columns([2, 1])
with col_t1:
    with st.container(border=True):
        t15d = df_top15[["ACCT_NAME", "INDUSTRIA_DETALLE", "TAMANO_EMPRESA",
                          "ENRIQ_SCORE", "TAMANO_SCORE", "SCORE_TOTAL",
                          "BILLING_STATE", "CONTACTO_PRINCIPAL"]].copy()
        t15d.columns = ["Empresa", "Sub-Industria", "Tamano", "Datos (0-5)",
                        "Tamano (0-5)", "Score", "Estado", "Contacto"]
        t15d = t15d.fillna("")
        hc15 = st.columns([2.5, 1.5, 1, 0.7, 0.7, 0.6, 1, 1.5])
        headers15 = ["Empresa", "Sub-Industria", "Tamano", "Datos", "Tam.", "Score", "Estado", "Contacto"]
        for col, h in zip(hc15, headers15):
            col.markdown(f"**{h}**")
        for i, (_, rw) in enumerate(t15d.iterrows()):
            rc = st.columns([2.5, 1.5, 1, 0.7, 0.7, 0.6, 1, 1.5])
            with rc[0]:
                if st.button(f":link: {rw['Empresa']}", key=f"t15_{i}", use_container_width=True):
                    st.session_state["_open_cuenta"] = rw["Empresa"]
                    st.rerun()
            rc[1].write(rw["Sub-Industria"])
            rc[2].write(rw["Tamano"])
            rc[3].write(str(rw["Datos (0-5)"]))
            rc[4].write(str(rw["Tamano (0-5)"]))
            rc[5].write(str(rw["Score"]))
            rc[6].write(rw["Estado"])
            rc[7].write(rw["Contacto"])

with col_t2:
    with st.container(border=True):
        st.markdown("**Composicion del Score**")
        fig_score = px.bar(
            t15d.head(15),
            y="Empresa", x=["Datos (0-5)", "Tamano (0-5)"],
            orientation="h", barmode="stack",
            color_discrete_sequence=["#3498db", "#2ecc71"],
            labels={"value": "Puntos", "variable": "Componente"}
        )
        fig_score.update_layout(height=540, margin=dict(l=0, r=0, t=10, b=0),
                                yaxis=dict(autorange="reversed"), legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig_score, use_container_width=True)

# =============================================================
# DETALLE DE CUENTAS (tabla paginada con botones-link)
# =============================================================

st.divider()
st.subheader("Detalle de Cuentas")
st.caption("Click en el nombre de la empresa para ver detalle completo, contactos y generar pitch")

df_detail = df_filtered[["ACCT_NAME", "INDUSTRIA_NOMBRE", "CONTACTO_PRINCIPAL", "CONTACTO_CARGO",
                          "TAMANO_EMPRESA", "BILLING_STATE", "BILLING_COUNTRY",
                          "SITIO_WEB", "CONTACTO_EMAIL_NUEVO"]].copy()
df_detail = df_detail.fillna("")

DETAIL_PER_PAGE = 15
total_detail = len(df_detail)
total_pages = max(1, (total_detail + DETAIL_PER_PAGE - 1) // DETAIL_PER_PAGE)
if "detail_page" not in st.session_state:
    st.session_state["detail_page"] = 0
current_page = st.session_state["detail_page"]

pg1, pg2, pg3 = st.columns([1, 2, 1])
with pg1:
    if st.button("< Anterior", key="det_prev", disabled=(current_page == 0)):
        st.session_state["detail_page"] = current_page - 1
        st.rerun()
with pg2:
    st.markdown(f"<div style='text-align:center'>Pagina {current_page + 1} de {total_pages} ({total_detail} cuentas)</div>", unsafe_allow_html=True)
with pg3:
    if st.button("Siguiente >", key="det_next", disabled=(current_page >= total_pages - 1)):
        st.session_state["detail_page"] = current_page + 1
        st.rerun()

start_idx = current_page * DETAIL_PER_PAGE
end_idx = min(start_idx + DETAIL_PER_PAGE, total_detail)
df_page = df_detail.iloc[start_idx:end_idx]

with st.container(border=True):
    dhc = st.columns([2.2, 1.3, 1.5, 1.2, 0.9, 0.9, 0.9, 1.5])
    for col, h in zip(dhc, ["Empresa", "Industria", "Contacto", "Cargo", "Tamano", "Estado", "Pais", "Email"]):
        col.markdown(f"**{h}**")
    for i, (_, rw) in enumerate(df_page.iterrows()):
        rc = st.columns([2.2, 1.3, 1.5, 1.2, 0.9, 0.9, 0.9, 1.5])
        with rc[0]:
            if st.button(f":link: {rw['ACCT_NAME']}", key=f"det_{start_idx + i}", use_container_width=True):
                st.session_state["_open_cuenta"] = rw["ACCT_NAME"]
                st.rerun()
        rc[1].write(rw["INDUSTRIA_NOMBRE"])
        rc[2].write(rw["CONTACTO_PRINCIPAL"])
        rc[3].write(rw["CONTACTO_CARGO"])
        rc[4].write(rw["TAMANO_EMPRESA"])
        rc[5].write(rw["BILLING_STATE"])
        rc[6].write(rw["BILLING_COUNTRY"])
        email_val = rw["CONTACTO_EMAIL_NUEVO"]
        if email_val and str(email_val).strip():
            rc[7].markdown(email_link_md(email_val))
        else:
            rc[7].write("")

# =============================================================
# INSIGHTS POR INDUSTRIA
# =============================================================

st.divider()
st.subheader("Insights por Industria: Casos de Uso Snowflake")
st.caption("Tendencias, retos y oportunidades generadas con Cortex AI para cada industria")

industrias_con_insights = sorted(df_casos["INDUSTRIA_NOMBRE"].unique())
if industrias_con_insights:
    tabs = st.tabs(industrias_con_insights)

    for tab, industria in zip(tabs, industrias_con_insights):
        with tab:
            caso = df_casos[df_casos["INDUSTRIA_NOMBRE"] == industria].iloc[0]
            n_cuentas = len(df_filtered[df_filtered["INDUSTRIA_NOMBRE"] == industria])
            st.caption(f"{n_cuentas} cuentas en esta industria (filtradas)")

            with st.container(border=True):
                st.markdown("**Propuesta de Valor**")
                st.info(str(caso["PROPUESTA_VALOR"])[:1000])

            c1, c2, c3 = st.columns(3)
            with c1:
                with st.container(border=True, height=350):
                    st.markdown("**Tendencias 2025-2026**")
                    st.write(str(caso["TENDENCIAS_INDUSTRIA"])[:1500])
            with c2:
                with st.container(border=True, height=350):
                    st.markdown("**Retos Principales**")
                    st.write(str(caso["RETOS_PRINCIPALES"])[:1500])
            with c3:
                with st.container(border=True, height=350):
                    st.markdown("**Casos de Uso Snowflake**")
                    st.write(str(caso["CASOS_USO_SNOWFLAKE"])[:1500])

# =============================================================
# APERTURA UNICA DEL DIALOG
# =============================================================

if "_open_cuenta" in st.session_state and st.session_state["_open_cuenta"]:
    _cuenta_abrir = st.session_state.pop("_open_cuenta")
    mostrar_tarjeta_cuenta(_cuenta_abrir)

# =============================================================
# PIE DE PAGINA
# =============================================================

st.divider()
st.caption("Dashboard de Territorios Comerciales v2 | DB_TERRITORIOS_COMERCIALES | Snowflake + Cortex AI | EGOS BI")
