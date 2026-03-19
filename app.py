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
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_connection():
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

@st.cache_data(ttl=300)
def load_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH best_contact AS (
            SELECT *
            FROM DB_TERRITORIOS_COMERCIALES.CORE.DIM_CONTACTOS
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
        FROM DB_TERRITORIOS_COMERCIALES.CORE.DIM_CUENTAS c
        JOIN DB_TERRITORIOS_COMERCIALES.CORE.DIM_INDUSTRIAS i ON c.INDUSTRIA_ID = i.INDUSTRIA_ID
        LEFT JOIN DB_TERRITORIOS_COMERCIALES.CORE.DIM_TERRITORIOS t ON c.TERRITORIO_ID = t.TERRITORIO_ID
        LEFT JOIN DB_TERRITORIOS_COMERCIALES.CORE.DIM_EJECUTIVOS e ON t.EJECUTIVO_ID = e.EJECUTIVO_ID
        LEFT JOIN best_contact bc ON c.CUENTA_ID = bc.CUENTA_ID
        ORDER BY c.ACCT_NAME
    """)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)

    cur.execute("""
        SELECT cu.CASO_ID, i.INDUSTRIA_NOMBRE, cu.TENDENCIAS_INDUSTRIA,
               cu.RETOS_PRINCIPALES, cu.CASOS_USO_SNOWFLAKE, cu.PROPUESTA_VALOR
        FROM DB_TERRITORIOS_COMERCIALES.CORE.DIM_CASOS_USO cu
        JOIN DB_TERRITORIOS_COMERCIALES.CORE.DIM_INDUSTRIAS i ON cu.INDUSTRIA_ID = i.INDUSTRIA_ID
    """)
    cols_cu = [desc[0] for desc in cur.description]
    rows_cu = cur.fetchall()
    df_casos = pd.DataFrame(rows_cu, columns=cols_cu)

    cur.close()
    conn.close()
    return df, df_casos

df, df_casos = load_data()

with st.sidebar:
    st.header("Filtros")
    industrias = sorted(df["INDUSTRIA_NOMBRE"].unique())
    sel_industrias = st.multiselect("Industria", industrias, default=industrias)
    paises = sorted(df["BILLING_COUNTRY"].dropna().unique())
    sel_paises = st.multiselect("País", paises, default=paises)
    estados = sorted(df["BILLING_STATE"].dropna().unique())
    sel_estados = st.multiselect("Estado", estados, default=estados)
    territorios = ["Con Territorio", "Sin Territorio"]
    sel_territorio = st.multiselect("Territorio", territorios, default=territorios)
    fuentes = sorted(df["FUENTE_CLASIFICACION"].dropna().unique())
    sel_fuentes = st.multiselect("Fuente Clasificación", fuentes, default=fuentes)
    sel_contacto = st.selectbox("Datos de Contacto", ["Todos", "Con Contacto", "Con LinkedIn", "Con Email", "Con Sitio Web", "Sin Datos"])
    tamanos = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
    sel_tamanos = st.multiselect("Tamaño Empresa", tamanos, default=tamanos)
    buscar = st.text_input("Buscar cuenta", placeholder="Nombre de empresa...")

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
    st.metric("Países", n_paises, border=True)
    st.metric("Con Sitio Web", int(con_sitio), border=True)
    st.metric("Con LinkedIn", int(con_linkedin), border=True)
    st.metric("Con Contacto", int(con_contacto), border=True)

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.subheader("Distribución por Industria")
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
        st.subheader("Distribución por Estado (Top 15)")
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
        st.subheader("Distribución por País")
        country_counts = df_filtered["BILLING_COUNTRY"].dropna().value_counts().reset_index()
        country_counts.columns = ["País", "Cuentas"]
        fig_country = px.pie(country_counts, values="Cuentas", names="País",
                             color_discrete_sequence=px.colors.qualitative.Set3, hole=0.35)
        fig_country.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_country, use_container_width=True)

with col4:
    with st.container(border=True):
        st.subheader("Tamaño de Empresa")
        tamano_order = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
        tamano_counts = df_filtered["TAMANO_EMPRESA"].value_counts().reindex(tamano_order).dropna().reset_index()
        tamano_counts.columns = ["Tamaño", "Cuentas"]
        fig_tam = px.bar(tamano_counts, x="Tamaño", y="Cuentas", text="Cuentas",
                         color="Tamaño", color_discrete_sequence=px.colors.sequential.Viridis)
        fig_tam.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=10, b=0))
        fig_tam.update_traces(textposition="outside")
        st.plotly_chart(fig_tam, use_container_width=True)

with st.container(border=True):
    st.subheader("Distribución: Tamaño por Industria (Heatmap)")
    tamano_ord = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
    df_heat = df_filtered[df_filtered["TAMANO_EMPRESA"].notna()].groupby(
        ["INDUSTRIA_NOMBRE", "TAMANO_EMPRESA"]).size().reset_index(name="Cuentas")
    df_pivot = df_heat.pivot(index="INDUSTRIA_NOMBRE", columns="TAMANO_EMPRESA", values="Cuentas").fillna(0)
    df_pivot = df_pivot.reindex(columns=[t for t in tamano_ord if t in df_pivot.columns])
    fig_heat = px.imshow(
        df_pivot.values, x=df_pivot.columns.tolist(), y=df_pivot.index.tolist(),
        color_continuous_scale="Viridis", text_auto=True,
        labels=dict(x="Tamaño", y="Industria", color="Cuentas"),
        aspect="auto"
    )
    fig_heat.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig_heat, use_container_width=True)

st.divider()
st.subheader("Top 10 Cuentas Principales de Interés (Global)")
st.caption("Ranking global combinado: datos obtenidos (0-5) + tamaño empresa (0-5) — todas las industrias")

tamano_score_map_g = {"Micro": 1, "Pequeña": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
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
        df_g10_display = df_top10_global[["ACCT_NAME", "INDUSTRIA_NOMBRE", "INDUSTRIA_DETALLE",
                                          "TAMANO_EMPRESA", "ENRIQ_SCORE", "TAMANO_SCORE",
                                          "SCORE_TOTAL", "BILLING_STATE", "BILLING_COUNTRY",
                                          "SITIO_WEB", "LINKEDIN_EMPRESA", "CONTACTO_PRINCIPAL"]].copy()
        df_g10_display.columns = ["Empresa", "Industria", "Sub-Industria", "Tamaño",
                                   "Datos (0-5)", "Tamaño (0-5)", "Score Total",
                                   "Estado", "País", "Sitio Web", "LinkedIn", "Contacto"]
        df_g10_display = df_g10_display.fillna("")
        st.dataframe(
            df_g10_display,
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "Sitio Web": st.column_config.LinkColumn("Sitio Web", display_text="Abrir"),
                "LinkedIn": st.column_config.LinkColumn("LinkedIn", display_text="Ver"),
                "Score Total": st.column_config.ProgressColumn("Score", min_value=0, max_value=10),
            }
        )

with col_g2:
    with st.container(border=True):
        st.markdown("**Composición del Score**")
        fig_g10 = px.bar(
            df_g10_display,
            y="Empresa", x=["Datos (0-5)", "Tamaño (0-5)"],
            orientation="h", barmode="stack",
            color_discrete_sequence=["#3498db", "#2ecc71"],
            labels={"value": "Puntos", "variable": "Componente"}
        )
        fig_g10.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0),
                              yaxis=dict(autorange="reversed"), legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_g10, use_container_width=True)

st.divider()
st.subheader("Top 15 Cuentas de Interés por Industria")

ind_list_top = sorted(df_filtered["INDUSTRIA_NOMBRE"].unique())
sel_ind_top15 = st.selectbox("Selecciona industria para Top 15", ind_list_top,
                              index=ind_list_top.index("Manufacturing & Industrial") if "Manufacturing & Industrial" in ind_list_top else 0)
st.caption(f"Ranking combinado: datos obtenidos + tamaño — {sel_ind_top15}")

tamano_score_map = {"Micro": 1, "Pequeña": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
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
        df_t15_display = df_top15[["ACCT_NAME", "INDUSTRIA_DETALLE", "TAMANO_EMPRESA",
                                    "ENRIQ_SCORE", "TAMANO_SCORE", "SCORE_TOTAL",
                                    "BILLING_STATE", "BILLING_COUNTRY",
                                    "SITIO_WEB", "LINKEDIN_EMPRESA", "CONTACTO_PRINCIPAL"]].copy()
        df_t15_display.columns = ["Empresa", "Sub-Industria", "Tamaño", "Datos (0-5)",
                                   "Tamaño (0-5)", "Score Total", "Estado", "País",
                                   "Sitio Web", "LinkedIn", "Contacto"]
        df_t15_display = df_t15_display.fillna("")
        st.dataframe(
            df_t15_display,
            use_container_width=True,
            hide_index=True,
            height=560,
            column_config={
                "Sitio Web": st.column_config.LinkColumn("Sitio Web", display_text="Abrir"),
                "LinkedIn": st.column_config.LinkColumn("LinkedIn", display_text="Ver"),
                "Score Total": st.column_config.ProgressColumn("Score", min_value=0, max_value=10),
            }
        )

with col_t2:
    with st.container(border=True):
        st.markdown("**Composición del Score**")
        fig_score = px.bar(
            df_t15_display.head(15),
            y="Empresa", x=["Datos (0-5)", "Tamaño (0-5)"],
            orientation="h", barmode="stack",
            color_discrete_sequence=["#3498db", "#2ecc71"],
            labels={"value": "Puntos", "variable": "Componente"}
        )
        fig_score.update_layout(height=540, margin=dict(l=0, r=0, t=10, b=0),
                                yaxis=dict(autorange="reversed"), legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig_score, use_container_width=True)

st.divider()
st.subheader("Detalle de Cuentas")

display_cols = ["ACCT_NAME", "INDUSTRIA_NOMBRE", "CONTACTO_PRINCIPAL", "CONTACTO_CARGO",
                "TAMANO_EMPRESA", "NUM_EMPLEADOS_ESTIMADO", "REVENUE_ESTIMADO_USD",
                "BILLING_STATE", "BILLING_COUNTRY",
                "SITIO_WEB", "CONTACTO_LINKEDIN", "CONTACTO_EMAIL_NUEVO"]
df_display = df_filtered[display_cols].copy()
df_display.columns = ["Empresa", "Industria", "Contacto", "Cargo Contacto",
                       "Tamaño", "Empleados Est.", "Revenue Est. USD", "Estado", "País",
                       "Sitio Web", "LinkedIn Contacto", "Email Contacto"]
df_display = df_display.fillna("")
df_display["Revenue Est. USD"] = df_display["Revenue Est. USD"].apply(
    lambda x: f"${float(x)/1e6:,.1f}M" if x and x != "" and x != 0 else "")

st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True,
    height=400,
    column_config={
        "Sitio Web": st.column_config.LinkColumn("Sitio Web", display_text="Abrir"),
        "LinkedIn Empresa": st.column_config.LinkColumn("LinkedIn Empresa", display_text="Ver Perfil"),
    }
)

st.divider()
st.subheader("Insights por Industria: Casos de Uso Snowflake")
st.caption("Tendencias, retos y oportunidades generadas con Cortex AI para cada industria")

industrias_con_insights = sorted(df_casos["INDUSTRIA_NOMBRE"].unique())
tabs = st.tabs(industrias_con_insights)

for tab, industria in zip(tabs, industrias_con_insights):
    with tab:
        caso = df_casos[df_casos["INDUSTRIA_NOMBRE"] == industria].iloc[0]
        n_cuentas = len(df_filtered[df_filtered["INDUSTRIA_NOMBRE"] == industria])
        st.caption(f"{n_cuentas} cuentas en esta industria (filtradas)")

        with st.container(border=True):
            st.markdown(f"**Propuesta de Valor**")
            st.info(caso["PROPUESTA_VALOR"])

        c1, c2, c3 = st.columns(3)
        with c1:
            with st.container(border=True):
                st.markdown("**Tendencias 2025-2026**")
                st.write(caso["TENDENCIAS_INDUSTRIA"])
        with c2:
            with st.container(border=True):
                st.markdown("**Retos Principales**")
                st.write(caso["RETOS_PRINCIPALES"])
        with c3:
            with st.container(border=True):
                st.markdown("**Casos de Uso Snowflake**")
                st.write(caso["CASOS_USO_SNOWFLAKE"])

st.divider()
st.subheader("Pitch Personalizado por Cuenta")
st.caption("Selecciona una cuenta para generar un pitch de ventas con IA combinando los insights de industria + datos de la cuenta")

cuentas_list = df_filtered["ACCT_NAME"].sort_values().tolist()
sel_cuenta = st.selectbox("Selecciona una cuenta", [""] + cuentas_list)

if sel_cuenta:
    row = df_filtered[df_filtered["ACCT_NAME"] == sel_cuenta].iloc[0]
    industria_cuenta = row["INDUSTRIA_NOMBRE"]

    caso_industria = df_casos[df_casos["INDUSTRIA_NOMBRE"] == industria_cuenta]
    insights_ctx = ""
    if not caso_industria.empty:
        ci = caso_industria.iloc[0]
        insights_ctx = (
            f"\nInsights de la industria {industria_cuenta}:"
            f"\nTendencias: {ci['TENDENCIAS_INDUSTRIA'][:300]}"
            f"\nRetos: {ci['RETOS_PRINCIPALES'][:300]}"
            f"\nCasos de uso Snowflake: {ci['CASOS_USO_SNOWFLAKE'][:300]}"
            f"\nPropuesta de valor: {ci['PROPUESTA_VALOR'][:200]}"
        )

    contexto = f"Empresa: {row['ACCT_NAME']}"
    if row["INDUSTRIA_NOMBRE"]:
        contexto += f"\nIndustria: {row['INDUSTRIA_NOMBRE']}"
    if row.get("INDUSTRIA_DETALLE") and str(row["INDUSTRIA_DETALLE"]).strip():
        contexto += f" ({row['INDUSTRIA_DETALLE']})"
    if row.get("TAMANO_EMPRESA") and str(row["TAMANO_EMPRESA"]).strip():
        contexto += f"\nTamaño: {row['TAMANO_EMPRESA']}"
    if row.get("NUM_EMPLEADOS_ESTIMADO") and row["NUM_EMPLEADOS_ESTIMADO"]:
        contexto += f" (~{int(row['NUM_EMPLEADOS_ESTIMADO'])} empleados)"
    if row.get("REVENUE_ESTIMADO_USD") and row["REVENUE_ESTIMADO_USD"]:
        contexto += f"\nRevenue estimado: ${float(row['REVENUE_ESTIMADO_USD'])/1e6:,.1f}M USD"
    if row["TERRITORIO"] and str(row["TERRITORIO"]).strip():
        contexto += f"\nTerritorio: {row['TERRITORIO']} (Ejecutivo: {row['EJECUTIVO']})"
    if row.get("BILLING_STATE") and str(row["BILLING_STATE"]).strip():
        contexto += f"\nEstado: {row['BILLING_STATE']}"
    if row.get("BILLING_COUNTRY") and str(row["BILLING_COUNTRY"]).strip():
        contexto += f"\nPaís: {row['BILLING_COUNTRY']}"
    if row["UBICACION"] and str(row["UBICACION"]).strip():
        contexto += f"\nUbicación: {row['UBICACION']}"
    if row["SITIO_WEB"] and str(row["SITIO_WEB"]).strip():
        contexto += f"\nSitio web: {row['SITIO_WEB']}"
    if row["LINKEDIN_EMPRESA"] and str(row["LINKEDIN_EMPRESA"]).strip():
        contexto += f"\nLinkedIn empresa: {row['LINKEDIN_EMPRESA']}"
    if row.get("CONTACTO_PRINCIPAL") and str(row["CONTACTO_PRINCIPAL"]).strip():
        contexto += f"\nPersona de interés: {row['CONTACTO_PRINCIPAL']}"
    if row.get("CONTACTO_CARGO") and str(row["CONTACTO_CARGO"]).strip():
        contexto += f"\nCargo: {row['CONTACTO_CARGO']}"
    if row.get("CONTACTO_EMAIL_NUEVO") and str(row["CONTACTO_EMAIL_NUEVO"]).strip():
        contexto += f"\nEmail contacto: {row['CONTACTO_EMAIL_NUEVO']}"
    elif row.get("CONTACTO_EMAIL") and str(row["CONTACTO_EMAIL"]).strip():
        contexto += f"\nEmail contacto: {row['CONTACTO_EMAIL']}"
    contexto += f"\nFuente de clasificación: {row['FUENTE_CLASIFICACION'] or 'CSV original'}"
    contexto += insights_ctx

    with st.spinner("Generando pitch personalizado con Cortex AI..."):
        try:
            conn_nl = get_connection()
            cur_nl = conn_nl.cursor()
            nombre_contacto = "XXXXX"
            if row.get("CONTACTO_PRINCIPAL") and str(row["CONTACTO_PRINCIPAL"]).strip():
                nombre_contacto = str(row["CONTACTO_PRINCIPAL"]).strip().split()[0]
            elif row.get("PERSONA_INTERES") and str(row["PERSONA_INTERES"]).strip():
                nombre_contacto = str(row["PERSONA_INTERES"]).strip().split()[0]

            prompt = (
                f"Eres un ejecutivo de ventas de Snowflake en México. Genera un pitch de ventas personalizado "
                f"en español (5-6 oraciones) para esta cuenta. Incluye: 1) saludo dirigido a '{nombre_contacto}' "
                f"mencionando la empresa, "
                f"2) un reto específico de su industria que puedes resolver, 3) un caso de uso concreto de "
                f"Snowflake relevante para ellos, 4) siguiente paso recomendado (demo, reunión, etc). "
                f"Usa un tono profesional pero cercano. Datos:\n" + contexto
            )
            cur_nl.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s) AS DESCRIPCION",
                (prompt,)
            )
            descripcion = cur_nl.fetchone()[0]
            cur_nl.close()
            conn_nl.close()

            with st.container(border=True):
                st.markdown(f"**Pitch para {sel_cuenta}** ({industria_cuenta})")
                st.write(descripcion)
        except Exception as e:
            st.error(f"Error al generar pitch: {e}")

st.divider()
st.caption("Dashboard de Territorios Comerciales | DB_TERRITORIOS_COMERCIALES | Snowflake + Cortex AI")
