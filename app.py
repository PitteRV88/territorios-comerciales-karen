# =============================================================
# app.py (v3) - Dashboard Territorios Comerciales Karen
# Streamlit Community Cloud edition (RSA key-pair auth)
# Incluye: KPIs, graficas, Top 10/15, detalle de cuentas,
#   tarjeta popup por cuenta, pitch IA, envio de correo mailto,
#   lead management (contactos, interacciones, vendedor externo,
#   activacion de leads, pipeline de leads, lead manual)
# v2: popup @st.dialog, botones :link:, pitch dentro del popup,
#     paginacion en detalle, mailto links
# v3: lead management completo — contactos por cuenta con principal,
#     contactado, comentarios, interacciones, vendedor externo,
#     marcar como lead activo, seccion pipeline de leads, lead manual,
#     barra de busqueda en detalle de cuentas
# Actualizado: 2026-03-27 | Proyecto: EGOS BI
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
    """Carga datos principales: cuentas + mejor contacto + industria + vendedor externo."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        WITH best_contact AS (
            SELECT *
            FROM {DB}.CORE.DIM_CONTACTOS
            WHERE ES_PRINCIPAL = TRUE
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
               c.ES_LEAD, c.LEAD_DESCRIPCION, c.LEAD_FECHA,
               c.VENDEDOR_EXTERNO_ID,
               ve.NOMBRE AS VENDEDOR_EXTERNO,
               bc.NOMBRE_COMPLETO AS CONTACTO_PRINCIPAL,
               bc.CARGO AS CONTACTO_CARGO,
               bc.EMAIL AS CONTACTO_EMAIL_NUEVO,
               bc.LINKEDIN_PERFIL AS CONTACTO_LINKEDIN,
               bc.HAS_EMAIL AS CONTACTO_HAS_EMAIL
        FROM {DB}.CORE.DIM_CUENTAS c
        JOIN {DB}.CORE.DIM_INDUSTRIAS i ON c.INDUSTRIA_ID = i.INDUSTRIA_ID
        LEFT JOIN {DB}.CORE.DIM_TERRITORIOS t ON c.TERRITORIO_ID = t.TERRITORIO_ID
        LEFT JOIN {DB}.CORE.DIM_EJECUTIVOS e ON t.EJECUTIVO_ID = e.EJECUTIVO_ID
        LEFT JOIN {DB}.CORE.DIM_EJECUTIVOS ve ON c.VENDEDOR_EXTERNO_ID = ve.EJECUTIVO_ID
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
    """Carga todos los contactos de una cuenta especifica, ordenados por prioridad."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT CONTACTO_ID, CUENTA_ID, NOMBRE_COMPLETO, PRIMER_NOMBRE, APELLIDO,
               CARGO, NIVEL_CARGO, DEPARTAMENTO, EMAIL, TELEFONO,
               LINKEDIN_PERFIL, FUENTE, PRIORIDAD, HAS_EMAIL, HAS_PHONE,
               ES_PRINCIPAL, CONTACTADO, COMENTARIOS
        FROM {DB}.CORE.DIM_CONTACTOS
        WHERE CUENTA_ID = %s
        ORDER BY ES_PRINCIPAL DESC, PRIORIDAD ASC, HAS_EMAIL DESC, CREATED_AT DESC
    """, (cuenta_id,))
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


def load_interacciones(cuenta_id):
    """Carga historial de interacciones de una cuenta."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT h.INTERACCION_ID, h.CONTACTO_ID, c.NOMBRE_COMPLETO AS CONTACTO_NOMBRE,
               h.TIPO_INTERACCION, h.DESCRIPCION, h.FECHA_INTERACCION, h.CREATED_BY
        FROM {DB}.CORE.HIST_INTERACCIONES h
        LEFT JOIN {DB}.CORE.DIM_CONTACTOS c ON h.CONTACTO_ID = c.CONTACTO_ID
        WHERE h.CUENTA_ID = %s
        ORDER BY h.FECHA_INTERACCION DESC
    """, (cuenta_id,))
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


def load_vendedores_externos():
    """Carga ejecutivos con ROL='EXTERNO' para asignacion."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT EJECUTIVO_ID, NOMBRE || ' ' || COALESCE(APELLIDO, '') AS NOMBRE_COMPLETO
        FROM {DB}.CORE.DIM_EJECUTIVOS
        WHERE ACTIVO = TRUE AND ROL = 'EXTERNO'
        ORDER BY NOMBRE
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


def load_industrias():
    """Carga lista de industrias existentes en DIM_INDUSTRIAS."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT INDUSTRIA_ID, INDUSTRIA_NOMBRE
        FROM {DB}.CORE.DIM_INDUSTRIAS
        ORDER BY INDUSTRIA_NOMBRE
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


def load_cuentas_lista():
    """Carga lista minima de cuentas para selector (ID + nombre)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT CUENTA_ID, ACCT_NAME
        FROM {DB}.CORE.DIM_CUENTAS
        ORDER BY ACCT_NAME
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


# =============================================================
# FUNCIONES DE ESCRITURA (write-back a Snowflake)
# =============================================================

def _exec_write(sql, params=None):
    """Ejecuta un INSERT/UPDATE y limpia cache."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    cur.close()
    conn.close()
    st.cache_data.clear()


def insert_contacto(cuenta_id, nombre, apellido, email, telefono, cargo, linkedin):
    """Inserta un contacto manual en DIM_CONTACTOS."""
    nombre_completo = f"{nombre} {apellido}".strip()
    has_email = bool(email and email.strip())
    has_phone = bool(telefono and telefono.strip())
    _exec_write(f"""
        INSERT INTO {DB}.CORE.DIM_CONTACTOS
            (CUENTA_ID, NOMBRE_COMPLETO, PRIMER_NOMBRE, APELLIDO, CARGO, EMAIL, TELEFONO,
             LINKEDIN_PERFIL, FUENTE, PRIORIDAD, HAS_EMAIL, HAS_PHONE, ES_PRINCIPAL)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'MANUAL', 1, %s, %s, FALSE)
    """, (cuenta_id, nombre_completo, nombre, apellido or None, cargo or None,
          email or None, telefono or None, linkedin or None, has_email, has_phone))


def set_contacto_principal(cuenta_id, contacto_id):
    """Marca un contacto como principal (y desmarca los demas de la cuenta)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"UPDATE {DB}.CORE.DIM_CONTACTOS SET ES_PRINCIPAL = FALSE WHERE CUENTA_ID = %s", (cuenta_id,))
    cur.execute(f"UPDATE {DB}.CORE.DIM_CONTACTOS SET ES_PRINCIPAL = TRUE WHERE CONTACTO_ID = %s", (contacto_id,))
    cur.close()
    conn.close()
    st.cache_data.clear()


def toggle_contactado(contacto_id, valor):
    """Marca/desmarca un contacto como contactado."""
    _exec_write(f"UPDATE {DB}.CORE.DIM_CONTACTOS SET CONTACTADO = %s WHERE CONTACTO_ID = %s", (valor, contacto_id))


def update_comentarios(contacto_id, texto):
    """Actualiza comentarios de un contacto."""
    _exec_write(f"UPDATE {DB}.CORE.DIM_CONTACTOS SET COMENTARIOS = %s WHERE CONTACTO_ID = %s",
                (texto or None, contacto_id))


def toggle_vendedor_externo(cuenta_id, ejecutivo_id):
    """Asigna o quita vendedor externo a una cuenta."""
    _exec_write(f"UPDATE {DB}.CORE.DIM_CUENTAS SET VENDEDOR_EXTERNO_ID = %s WHERE CUENTA_ID = %s",
                (ejecutivo_id, cuenta_id))


def update_industria_cuenta(cuenta_id, industria_id):
    """Actualiza la industria de una cuenta."""
    _exec_write(f"UPDATE {DB}.CORE.DIM_CUENTAS SET INDUSTRIA_ID = %s, UPDATED_AT = CURRENT_TIMESTAMP() WHERE CUENTA_ID = %s",
                (industria_id, cuenta_id))


def update_cuenta_campos(cuenta_id, campos: dict):
    """Actualiza multiples campos de una cuenta en DIM_CUENTAS.
    campos: dict de {COLUMN_NAME: value}
    """
    if not campos:
        return
    set_parts = []
    vals = []
    for col, val in campos.items():
        set_parts.append(f"{col} = %s")
        vals.append(val)
    set_parts.append("UPDATED_AT = CURRENT_TIMESTAMP()")
    vals.append(cuenta_id)
    _exec_write(
        f"UPDATE {DB}.CORE.DIM_CUENTAS SET {', '.join(set_parts)} WHERE CUENTA_ID = %s",
        tuple(vals)
    )


def marcar_como_lead(cuenta_id, descripcion):
    """Marca una cuenta como lead."""
    _exec_write(f"""
        UPDATE {DB}.CORE.DIM_CUENTAS
        SET ES_LEAD = TRUE, LEAD_DESCRIPCION = %s, LEAD_FECHA = CURRENT_TIMESTAMP(),
            ESTATUS = 'LEAD'
        WHERE CUENTA_ID = %s
    """, (descripcion, cuenta_id))


def desmarcar_lead(cuenta_id):
    """Quita la marca de lead de una cuenta."""
    _exec_write(f"""
        UPDATE {DB}.CORE.DIM_CUENTAS
        SET ES_LEAD = FALSE, LEAD_DESCRIPCION = NULL, LEAD_FECHA = NULL,
            ESTATUS = 'PROSPECTO'
        WHERE CUENTA_ID = %s
    """, (cuenta_id,))


def insert_interaccion(contacto_id, cuenta_id, tipo, descripcion, created_by="Pedro"):
    """Registra una interaccion con un contacto."""
    _exec_write(f"""
        INSERT INTO {DB}.CORE.HIST_INTERACCIONES
            (CONTACTO_ID, CUENTA_ID, TIPO_INTERACCION, DESCRIPCION, CREATED_BY)
        VALUES (%s, %s, %s, %s, %s)
    """, (contacto_id, cuenta_id, tipo, descripcion, created_by))


def insert_lead_manual(cuenta_nombre, industria_id, contacto_nombre, contacto_email,
                       contacto_telefono, contacto_cargo, contacto_linkedin, descripcion,
                       vendedor_externo_id=None, interaccion_tipo=None, interaccion_desc=None,
                       cuenta_existente_id=None):
    """Crea un lead manual: usa cuenta existente o crea nueva, crea contacto, y opcionalmente interaccion."""
    conn = get_connection()
    cur = conn.cursor()

    if cuenta_existente_id:
        cuenta_id = int(cuenta_existente_id)
        upd_parts = ["ES_LEAD = TRUE", "LEAD_DESCRIPCION = %s",
                     "LEAD_FECHA = CURRENT_TIMESTAMP()", "ESTATUS = 'LEAD'"]
        upd_vals = [descripcion]
        if industria_id:
            upd_parts.append("INDUSTRIA_ID = %s")
            upd_vals.append(industria_id)
        if vendedor_externo_id:
            upd_parts.append("VENDEDOR_EXTERNO_ID = %s")
            upd_vals.append(vendedor_externo_id)
        upd_vals.append(cuenta_id)
        cur.execute(f"UPDATE {DB}.CORE.DIM_CUENTAS SET {', '.join(upd_parts)} WHERE CUENTA_ID = %s",
                    tuple(upd_vals))
    else:
        ind_id = industria_id if industria_id else 8
        cur.execute(f"""
            INSERT INTO {DB}.CORE.DIM_CUENTAS
                (ACCT_NAME, INDUSTRIA_ID, ESTATUS, ES_LEAD, LEAD_DESCRIPCION, LEAD_FECHA, VENDEDOR_EXTERNO_ID)
            VALUES (%s, %s, 'LEAD', TRUE, %s, CURRENT_TIMESTAMP(), %s)
        """, (cuenta_nombre, ind_id, descripcion, vendedor_externo_id))
        cur.execute(f"SELECT MAX(CUENTA_ID) FROM {DB}.CORE.DIM_CUENTAS WHERE ACCT_NAME = %s", (cuenta_nombre,))
        cuenta_id = cur.fetchone()[0]

    nombre_completo = contacto_nombre.strip()
    parts = nombre_completo.split(" ", 1)
    primer_nombre = parts[0]
    apellido = parts[1] if len(parts) > 1 else None
    has_email = bool(contacto_email and contacto_email.strip())
    has_phone = bool(contacto_telefono and contacto_telefono.strip())
    cur.execute(f"""
        INSERT INTO {DB}.CORE.DIM_CONTACTOS
            (CUENTA_ID, NOMBRE_COMPLETO, PRIMER_NOMBRE, APELLIDO, CARGO, EMAIL, TELEFONO,
             LINKEDIN_PERFIL, FUENTE, PRIORIDAD, HAS_EMAIL, HAS_PHONE, ES_PRINCIPAL, CONTACTADO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'MANUAL', 1, %s, %s, TRUE, FALSE)
    """, (cuenta_id, nombre_completo, primer_nombre, apellido, contacto_cargo or None,
          contacto_email or None, contacto_telefono or None, contacto_linkedin or None,
          has_email, has_phone))

    if interaccion_tipo and interaccion_desc and interaccion_desc.strip():
        cur.execute(f"SELECT MAX(CONTACTO_ID) FROM {DB}.CORE.DIM_CONTACTOS WHERE CUENTA_ID = %s", (cuenta_id,))
        contacto_id = cur.fetchone()[0]
        cur.execute(f"""
            INSERT INTO {DB}.CORE.HIST_INTERACCIONES
                (CONTACTO_ID, CUENTA_ID, TIPO_INTERACCION, DESCRIPCION, CREATED_BY)
            VALUES (%s, %s, %s, %s, 'LEAD_MANUAL')
        """, (contacto_id, cuenta_id, interaccion_tipo, interaccion_desc.strip()))

    cur.close()
    conn.close()
    st.cache_data.clear()


# =============================================================
# DIALOG: TARJETA DE DETALLE DE CUENTA (popup)
# =============================================================

@st.dialog("Detalle de Cuenta", width="large")
def mostrar_tarjeta_cuenta(acct_name):
    """Dialog popup con detalle completo de una cuenta + contactos + gestion + pitch IA."""
    row = df[df["ACCT_NAME"] == acct_name]
    if row.empty:
        st.error("Cuenta no encontrada.")
        return
    row = row.iloc[0]
    cuenta_id = int(row["CUENTA_ID"])

    # -- Tarjeta de datos principales --
    st.markdown(f"### {acct_name}")
    if row.get("ES_LEAD") and row["ES_LEAD"]:
        st.success("LEAD ACTIVO")

    # Info de solo lectura
    if row.get("TERRITORIO") and str(row["TERRITORIO"]).strip():
        st.markdown(f"**Territorio:** {row['TERRITORIO']} (Ejecutivo: {row['EJECUTIVO']})")
    if row.get("INDUSTRIA_DETALLE") and str(row["INDUSTRIA_DETALLE"]).strip():
        st.caption(f"Sub-industria: {row['INDUSTRIA_DETALLE']}")
    if row["NOTAS"] and str(row["NOTAS"]).strip():
        st.info(f"**Notas:** {row['NOTAS']}")

    # -- Editar datos de la cuenta --
    st.markdown("---")
    st.markdown("**Editar datos de la cuenta**")

    df_industrias = load_industrias()
    vendedores = load_vendedores_externos()

    ind_nombres = list(df_industrias["INDUSTRIA_NOMBRE"]) if not df_industrias.empty else []
    ind_ids = list(df_industrias["INDUSTRIA_ID"]) if not df_industrias.empty else []
    ind_map = dict(zip(ind_nombres, ind_ids))
    current_ind_name = row["INDUSTRIA_NOMBRE"]
    current_ind_idx = ind_nombres.index(current_ind_name) if current_ind_name in ind_nombres else 0

    tamano_opciones = ["", "Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
    current_tamano = row["TAMANO_EMPRESA"] if row["TAMANO_EMPRESA"] and str(row["TAMANO_EMPRESA"]).strip() else ""
    current_tamano_idx = tamano_opciones.index(current_tamano) if current_tamano in tamano_opciones else 0

    ve_nombres = ["Sin asignar"]
    ve_ids = [None]
    if not vendedores.empty:
        ve_nombres += list(vendedores["NOMBRE_COMPLETO"].str.strip())
        ve_ids += list(vendedores["EJECUTIVO_ID"].astype(int))
    ve_map = dict(zip(ve_nombres, ve_ids))
    current_ve = row.get("VENDEDOR_EXTERNO_ID")
    current_ve_idx = 0
    if pd.notna(current_ve):
        for i, vid in enumerate(ve_ids):
            if vid == int(current_ve):
                current_ve_idx = i
                break

    cur_empleados = int(row["NUM_EMPLEADOS_ESTIMADO"]) if pd.notna(row["NUM_EMPLEADOS_ESTIMADO"]) else 0
    cur_revenue = float(row["REVENUE_ESTIMADO_USD"]) if pd.notna(row["REVENUE_ESTIMADO_USD"]) else 0.0
    cur_pais = str(row["BILLING_COUNTRY"]).strip() if row.get("BILLING_COUNTRY") and str(row["BILLING_COUNTRY"]).strip() else ""
    cur_estado = str(row["BILLING_STATE"]).strip() if row.get("BILLING_STATE") and str(row["BILLING_STATE"]).strip() else ""
    cur_web = str(row["SITIO_WEB"]).strip() if row.get("SITIO_WEB") and str(row["SITIO_WEB"]).strip() else ""
    cur_linkedin = str(row["LINKEDIN_EMPRESA"]).strip() if row.get("LINKEDIN_EMPRESA") and str(row["LINKEDIN_EMPRESA"]).strip() else ""

    with st.form(f"edit_cuenta_{cuenta_id}"):
        ef1, ef2 = st.columns(2)
        ed_industria = ef1.selectbox("Industria", ind_nombres, index=current_ind_idx, key=f"ed_ind_{cuenta_id}")
        ed_tamano = ef2.selectbox("Tamano empresa", tamano_opciones, index=current_tamano_idx, key=f"ed_tam_{cuenta_id}")

        ef3, ef4 = st.columns(2)
        ed_empleados = ef3.number_input("Num. empleados estimado", min_value=0, value=cur_empleados, step=1, key=f"ed_emp_{cuenta_id}")
        ed_revenue = ef4.number_input("Revenue estimado (USD)", min_value=0.0, value=cur_revenue, step=100000.0, format="%.2f", key=f"ed_rev_{cuenta_id}")

        ef5, ef6 = st.columns(2)
        ed_pais = ef5.text_input("Pais", value=cur_pais, key=f"ed_pais_{cuenta_id}")
        ed_estado = ef6.text_input("Estado / Ciudad", value=cur_estado, key=f"ed_estado_{cuenta_id}")

        ef7, ef8 = st.columns(2)
        ed_web = ef7.text_input("Sitio web", value=cur_web, key=f"ed_web_{cuenta_id}")
        ed_linkedin = ef8.text_input("LinkedIn empresa", value=cur_linkedin, key=f"ed_lk_{cuenta_id}")

        ed_vendedor = st.selectbox("Vendedor externo", ve_nombres, index=current_ve_idx, key=f"ed_ve_{cuenta_id}")

        guardar = st.form_submit_button("Guardar Cambios", type="primary")
        if guardar:
            cambios = {}
            new_ind_id = ind_map.get(ed_industria)
            old_ind_id = ind_map.get(current_ind_name)
            if new_ind_id != old_ind_id:
                cambios["INDUSTRIA_ID"] = new_ind_id
            new_tam = ed_tamano if ed_tamano else None
            old_tam = current_tamano if current_tamano else None
            if new_tam != old_tam:
                cambios["TAMANO_EMPRESA"] = new_tam
            new_emp = ed_empleados if ed_empleados > 0 else None
            old_emp = cur_empleados if cur_empleados > 0 else None
            if new_emp != old_emp:
                cambios["NUM_EMPLEADOS_ESTIMADO"] = new_emp
            new_rev = ed_revenue if ed_revenue > 0 else None
            old_rev = cur_revenue if cur_revenue > 0 else None
            if new_rev != old_rev:
                cambios["REVENUE_ESTIMADO_USD"] = new_rev
            new_pais = ed_pais.strip() if ed_pais.strip() else None
            old_pais = cur_pais if cur_pais else None
            if new_pais != old_pais:
                cambios["BILLING_COUNTRY"] = new_pais
            new_est = ed_estado.strip() if ed_estado.strip() else None
            old_est = cur_estado if cur_estado else None
            if new_est != old_est:
                cambios["BILLING_STATE"] = new_est
            new_web = ed_web.strip() if ed_web.strip() else None
            old_web = cur_web if cur_web else None
            if new_web != old_web:
                cambios["SITIO_WEB"] = new_web
            new_lk = ed_linkedin.strip() if ed_linkedin.strip() else None
            old_lk = cur_linkedin if cur_linkedin else None
            if new_lk != old_lk:
                cambios["LINKEDIN_EMPRESA"] = new_lk
            new_ve_id = ve_map.get(ed_vendedor)
            old_ve_id = int(current_ve) if pd.notna(current_ve) else None
            if new_ve_id != old_ve_id:
                cambios["VENDEDOR_EXTERNO_ID"] = new_ve_id

            if cambios:
                update_cuenta_campos(cuenta_id, cambios)
                st.success("Cambios guardados.")
                st.rerun()
            else:
                st.info("No hay cambios que guardar.")

    # -- Contactos --
    st.markdown("---")
    st.markdown("**Contactos**")
    df_contactos = load_contactos_cuenta(cuenta_id)

    if not df_contactos.empty:
        for idx, ct in df_contactos.iterrows():
            cid = int(ct["CONTACTO_ID"])
            es_principal = ct["ES_PRINCIPAL"]
            contactado = ct["CONTACTADO"]
            with st.container(border=True):
                cc1, cc2, cc3, cc4 = st.columns([3, 1.5, 1, 1])
                with cc1:
                    nombre = ct["NOMBRE_COMPLETO"] or "Sin nombre"
                    cargo_txt = ct["CARGO"] or ""
                    nivel_txt = ct["NIVEL_CARGO"] or ""
                    badge = " **(Principal)**" if es_principal else ""
                    st.markdown(f"**{nombre}**{badge} | {cargo_txt} {nivel_txt}")
                    parts = []
                    if ct["EMAIL"] and str(ct["EMAIL"]).strip():
                        parts.append(email_link_md(ct["EMAIL"]))
                    if ct["TELEFONO"] and str(ct["TELEFONO"]).strip():
                        parts.append(f"Tel: {ct['TELEFONO']}")
                    if ct["LINKEDIN_PERFIL"] and str(ct["LINKEDIN_PERFIL"]).strip():
                        parts.append(f"[LinkedIn]({ct['LINKEDIN_PERFIL']})")
                    if parts:
                        st.markdown(" | ".join(parts))
                with cc2:
                    if not es_principal:
                        if st.button("Hacer Principal", key=f"principal_{cid}"):
                            set_contacto_principal(cuenta_id, cid)
                            st.rerun()
                    else:
                        st.caption("Contacto principal")
                with cc3:
                    new_val = st.checkbox("Contactado", value=contactado, key=f"contactado_{cid}")
                    if new_val != contactado:
                        toggle_contactado(cid, new_val)
                        st.rerun()
                with cc4:
                    if st.button("+ Interaccion", key=f"inter_btn_{cid}"):
                        st.session_state[f"show_inter_{cid}"] = True

                # Comentarios
                current_comment = ct["COMENTARIOS"] or ""
                with st.expander("Comentarios", expanded=bool(current_comment)):
                    new_comment = st.text_area("", value=current_comment,
                                               key=f"comment_{cid}", height=60,
                                               placeholder="Agregar comentarios...")
                    if new_comment != current_comment:
                        if st.button("Guardar", key=f"save_comment_{cid}"):
                            update_comentarios(cid, new_comment)
                            st.rerun()

                # Form interaccion inline
                if st.session_state.get(f"show_inter_{cid}"):
                    with st.container(border=True):
                        st.caption(f"Registrar interaccion con {nombre}")
                        ic1, ic2 = st.columns(2)
                        tipo_inter = ic1.selectbox("Tipo", ["LLAMADA", "EMAIL", "REUNION", "LINKEDIN", "OTRO"],
                                                   key=f"tipo_inter_{cid}")
                        desc_inter = ic2.text_input("Descripcion", key=f"desc_inter_{cid}",
                                                    placeholder="Detalle de la interaccion...")
                        ib1, ib2 = st.columns(2)
                        if ib1.button("Registrar", key=f"reg_inter_{cid}", type="primary"):
                            if desc_inter.strip():
                                insert_interaccion(cid, cuenta_id, tipo_inter, desc_inter)
                                st.session_state.pop(f"show_inter_{cid}", None)
                                st.rerun()
                            else:
                                st.warning("Ingresa una descripcion.")
                        if ib2.button("Cancelar", key=f"cancel_inter_{cid}"):
                            st.session_state.pop(f"show_inter_{cid}", None)
                            st.rerun()

                # Si contactado, opcion de marcar como lead activo
                if contactado and not (row.get("ES_LEAD") and row["ES_LEAD"]):
                    if st.button("Marcar como Lead Activo", key=f"lead_btn_{cid}"):
                        st.session_state[f"show_lead_{cuenta_id}"] = True

    else:
        st.warning("Sin contactos registrados para esta cuenta.")

    # -- Form: Marcar como Lead Activo --
    if st.session_state.get(f"show_lead_{cuenta_id}"):
        with st.container(border=True):
            st.markdown("**Marcar como Lead Activo**")
            lead_desc = st.text_area("Descripcion del lead", key=f"lead_desc_{cuenta_id}",
                                     placeholder="Porque es un lead activo? Detalle de la oportunidad...")
            lb1, lb2 = st.columns(2)
            if lb1.button("Confirmar Lead Activo", key=f"confirm_lead_{cuenta_id}", type="primary"):
                marcar_como_lead(cuenta_id, lead_desc)
                st.session_state.pop(f"show_lead_{cuenta_id}", None)
                st.rerun()
            if lb2.button("Cancelar", key=f"cancel_lead_{cuenta_id}"):
                st.session_state.pop(f"show_lead_{cuenta_id}", None)
                st.rerun()

    # -- Form: Agregar Contacto Nuevo --
    st.markdown("---")
    with st.expander("Agregar Contacto Nuevo"):
        with st.form(f"form_contacto_{cuenta_id}", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            f_nombre = fc1.text_input("Nombre *", placeholder="Nombre")
            f_apellido = fc2.text_input("Apellido", placeholder="Apellido")
            fc3, fc4 = st.columns(2)
            f_email = fc3.text_input("Email", placeholder="correo@empresa.com")
            f_telefono = fc4.text_input("Telefono", placeholder="+52 ...")
            fc5, fc6 = st.columns(2)
            f_cargo = fc5.text_input("Puesto/Cargo", placeholder="Director de TI")
            f_linkedin = fc6.text_input("LinkedIn", placeholder="https://linkedin.com/in/...")
            submitted = st.form_submit_button("Agregar Contacto", type="primary")
            if submitted:
                if not f_nombre.strip():
                    st.error("El nombre es obligatorio.")
                elif not (f_email.strip() or f_telefono.strip() or f_linkedin.strip()):
                    st.error("Se requiere al menos un metodo de contacto (email, telefono o LinkedIn).")
                else:
                    insert_contacto(cuenta_id, f_nombre.strip(), f_apellido.strip(),
                                    f_email.strip(), f_telefono.strip(), f_cargo.strip(), f_linkedin.strip())
                    st.success(f"Contacto {f_nombre} agregado.")
                    st.rerun()

    # -- Historial de Interacciones --
    df_inter = load_interacciones(cuenta_id)
    if not df_inter.empty:
        st.markdown("---")
        with st.expander(f"Historial de Interacciones ({len(df_inter)})", expanded=False):
            for _, inter in df_inter.iterrows():
                fecha = str(inter["FECHA_INTERACCION"])[:16] if inter["FECHA_INTERACCION"] else ""
                st.markdown(
                    f"- **{inter['TIPO_INTERACCION']}** | {inter['CONTACTO_NOMBRE'] or 'N/A'} | "
                    f"{fecha} | {inter['DESCRIPCION'] or ''}"
                )

    # -- Accion: Generar Pitch con IA --
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
    if pd.notna(row.get("NUM_EMPLEADOS_ESTIMADO")):
        contexto += f" (~{int(row['NUM_EMPLEADOS_ESTIMADO'])} empleados)"
    if pd.notna(row.get("REVENUE_ESTIMADO_USD")):
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
# SECCION: PIPELINE DE LEADS
# =============================================================

st.subheader("Pipeline de Leads")
st.caption("Cuentas marcadas como leads activos — click en el nombre para ver detalle completo")

df_leads = df_filtered[df_filtered["ES_LEAD"] == True].copy()

lk1, lk2, lk3, lk4 = st.columns(4)
total_leads = len(df_leads)
lk1.metric("Total Leads", total_leads)
if total_leads > 0 and "LEAD_FECHA" in df_leads.columns:
    df_leads["LEAD_FECHA_DT"] = pd.to_datetime(df_leads["LEAD_FECHA"], errors="coerce")
    hoy = pd.Timestamp.now()
    inicio_semana = hoy - pd.Timedelta(days=hoy.weekday())
    leads_semana = df_leads[df_leads["LEAD_FECHA_DT"] >= inicio_semana].shape[0]
    lk2.metric("Leads esta semana", leads_semana)
else:
    lk2.metric("Leads esta semana", 0)
if total_leads > 0:
    leads_con_ve = df_leads["VENDEDOR_EXTERNO_ID"].notna().sum()
    lk3.metric("Con vendedor externo", int(leads_con_ve))
else:
    lk3.metric("Con vendedor externo", 0)
lk4.metric("Conversion", f"{(total_leads / len(df_filtered) * 100):.1f}%" if len(df_filtered) > 0 else "0%")

if st.button("+ Nuevo Lead Manual", type="primary", key="btn_nuevo_lead"):
    st.session_state["show_lead_manual_form"] = True

if st.session_state.get("show_lead_manual_form"):
    with st.container(border=True):
        st.markdown("**Crear Lead Manual**")
        df_industrias_form = load_industrias()
        df_cuentas_form = load_cuentas_lista()
        vendedores_form = load_vendedores_externos()

        with st.form("form_lead_manual", clear_on_submit=True):
            lm_es_existente = st.checkbox("Cuenta existente del territorio", value=False,
                                           help="Marcar si la cuenta ya esta en el territorio")

            lm_cuenta_existente_id = None
            lm_empresa = ""
            if lm_es_existente:
                if not df_cuentas_form.empty:
                    cuentas_opciones = [""] + [f"{r['ACCT_NAME']} (ID:{r['CUENTA_ID']})" for _, r in df_cuentas_form.iterrows()]
                    sel_cuenta = st.selectbox("Buscar cuenta existente", cuentas_opciones,
                                              help="Selecciona la cuenta del territorio")
                    if sel_cuenta:
                        lm_cuenta_existente_id = int(sel_cuenta.split("ID:")[1].rstrip(")"))
                        lm_empresa = sel_cuenta.split(" (ID:")[0]
                else:
                    st.warning("No hay cuentas en el territorio.")
            else:
                lm_empresa = st.text_input("Nombre de Empresa *", placeholder="Empresa S.A. de C.V.")

            lf_ind_col, lf_ve_col = st.columns(2)
            with lf_ind_col:
                ind_nombres = list(df_industrias_form["INDUSTRIA_NOMBRE"]) if not df_industrias_form.empty else []
                ind_ids = list(df_industrias_form["INDUSTRIA_ID"]) if not df_industrias_form.empty else []
                ind_map = dict(zip(ind_nombres, ind_ids))
                lm_industria_sel = st.selectbox("Industria", ["Sin Clasificar"] + [n for n in ind_nombres if n != "Sin Clasificar"],
                                                 help="Seleccionar industria de la lista")
                lm_industria_id = ind_map.get(lm_industria_sel, 8)

            with lf_ve_col:
                if not vendedores_form.empty:
                    ve_nombres = ["Sin asignar"] + list(vendedores_form["NOMBRE_COMPLETO"].str.strip())
                    ve_ids = [None] + list(vendedores_form["EJECUTIVO_ID"].astype(int))
                    ve_map = dict(zip(ve_nombres, ve_ids))
                    lm_vendedor_sel = st.selectbox("Vendedor Externo", ve_nombres)
                    lm_vendedor_id = ve_map.get(lm_vendedor_sel)
                else:
                    st.caption("No hay vendedores externos registrados")
                    lm_vendedor_id = None

            lf3, lf4 = st.columns(2)
            lm_contacto = lf3.text_input("Nombre del Contacto *", placeholder="Juan Perez")
            lm_cargo = lf4.text_input("Cargo", placeholder="Director de TI")
            lf5, lf6 = st.columns(2)
            lm_email = lf5.text_input("Email", placeholder="correo@empresa.com")
            lm_telefono = lf6.text_input("Telefono", placeholder="+52 ...")
            lm_linkedin = st.text_input("LinkedIn del Contacto", placeholder="https://linkedin.com/in/...")

            lm_descripcion = st.text_area("Descripcion del Lead *",
                                          placeholder="Porque es un lead? Detalle de la oportunidad...")

            st.markdown("---")
            st.markdown("**Interaccion comercial existente** *(opcional)*")
            lm_hay_interaccion = st.checkbox("Registrar interaccion comercial previa")
            lm_inter_tipo = None
            lm_inter_desc = None
            if lm_hay_interaccion:
                li1, li2 = st.columns(2)
                lm_inter_tipo = li1.selectbox("Tipo de interaccion",
                                               ["LLAMADA", "EMAIL", "REUNION", "WHATSAPP", "LINKEDIN", "OTRO"])
                lm_inter_desc = li2.text_input("Descripcion de la interaccion",
                                                placeholder="Detalle breve de la interaccion...")

            lf_sub1, lf_sub2 = st.columns(2)
            submitted_lead = lf_sub1.form_submit_button("Crear Lead", type="primary")
            cancel_lead = lf_sub2.form_submit_button("Cancelar")
            if submitted_lead:
                if not lm_es_existente and not lm_empresa.strip():
                    st.error("El nombre de empresa es obligatorio.")
                elif lm_es_existente and not lm_cuenta_existente_id:
                    st.error("Selecciona una cuenta existente del territorio.")
                elif not lm_contacto.strip():
                    st.error("El nombre del contacto es obligatorio.")
                elif not (lm_email.strip() or lm_telefono.strip() or lm_linkedin.strip()):
                    st.error("Se requiere al menos un metodo de contacto (email, telefono o LinkedIn).")
                elif not lm_descripcion.strip():
                    st.error("La descripcion del lead es obligatoria.")
                elif lm_hay_interaccion and (not lm_inter_desc or not lm_inter_desc.strip()):
                    st.error("Si registras interaccion, la descripcion es obligatoria.")
                else:
                    insert_lead_manual(
                        cuenta_nombre=lm_empresa.strip(),
                        industria_id=int(lm_industria_id),
                        contacto_nombre=lm_contacto.strip(),
                        contacto_email=lm_email.strip(),
                        contacto_telefono=lm_telefono.strip(),
                        contacto_cargo=lm_cargo.strip(),
                        contacto_linkedin=lm_linkedin.strip(),
                        descripcion=lm_descripcion.strip(),
                        vendedor_externo_id=lm_vendedor_id,
                        interaccion_tipo=lm_inter_tipo if lm_hay_interaccion else None,
                        interaccion_desc=lm_inter_desc if lm_hay_interaccion else None,
                        cuenta_existente_id=lm_cuenta_existente_id
                    )
                    st.success(f"Lead '{lm_empresa.strip()}' creado exitosamente.")
                    st.session_state.pop("show_lead_manual_form", None)
                    st.rerun()
            if cancel_lead:
                st.session_state.pop("show_lead_manual_form", None)
                st.rerun()

# Tabla de leads
if not df_leads.empty:
    df_leads_display = df_leads[["ACCT_NAME", "INDUSTRIA_NOMBRE", "CONTACTO_PRINCIPAL",
                                  "LEAD_DESCRIPCION", "LEAD_FECHA", "VENDEDOR_EXTERNO",
                                  "BILLING_STATE", "BILLING_COUNTRY"]].copy()
    df_leads_display = df_leads_display.fillna("")
    df_leads_display = df_leads_display.rename(columns={
        "ACCT_NAME": "Empresa", "INDUSTRIA_NOMBRE": "Industria",
        "CONTACTO_PRINCIPAL": "Contacto", "LEAD_DESCRIPCION": "Descripcion",
        "LEAD_FECHA": "Fecha", "VENDEDOR_EXTERNO": "Vendedor",
        "BILLING_STATE": "Estado", "BILLING_COUNTRY": "Pais"
    })
    if "Fecha" in df_leads_display.columns:
        df_leads_display["Fecha"] = pd.to_datetime(df_leads_display["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

    with st.container(border=True):
        lhc = st.columns([2, 1.2, 1.3, 2, 0.8, 1, 0.8, 0.8])
        for col, h in zip(lhc, ["Empresa", "Industria", "Contacto", "Descripcion", "Fecha", "Vendedor", "Estado", "Pais"]):
            col.markdown(f"**{h}**")
        for i, (_, lr) in enumerate(df_leads_display.iterrows()):
            lrc = st.columns([2, 1.2, 1.3, 2, 0.8, 1, 0.8, 0.8])
            with lrc[0]:
                if st.button(f":link: {lr['Empresa']}", key=f"lead_{i}", use_container_width=True):
                    st.session_state["_open_cuenta"] = lr["Empresa"]
                    st.rerun()
            lrc[1].write(lr["Industria"])
            lrc[2].write(lr["Contacto"])
            lrc[3].caption(lr["Descripcion"][:80] + "..." if len(str(lr["Descripcion"])) > 80 else lr["Descripcion"])
            lrc[4].write(lr["Fecha"])
            lrc[5].write(lr["Vendedor"])
            lrc[6].write(lr["Estado"])
            lrc[7].write(lr["Pais"])
else:
    st.info("No hay leads activos aun. Marca contactos como 'Contactado' y marca como lead activo desde el detalle de cuenta, o usa el boton 'Nuevo Lead Manual'.")

# =============================================================
# PESTANAS PRINCIPALES
# =============================================================

tab_graficas, tab_top10, tab_top15, tab_detalle, tab_insights = st.tabs(
    ["Graficas", "Top 10 Global", "Top 15 por Industria", "Detalle Cuentas", "Insights Industria"]
)

# =============================================================
# TAB: GRAFICAS
# =============================================================

with tab_graficas:
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
            tamano_order = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
            tamano_counts = df_filtered["TAMANO_EMPRESA"].value_counts().reindex(tamano_order).dropna().reset_index()
            tamano_counts.columns = ["Tamano", "Cuentas"]
            fig_tam = px.bar(tamano_counts, x="Tamano", y="Cuentas", text="Cuentas",
                             color="Tamano", color_discrete_sequence=px.colors.sequential.Viridis)
            fig_tam.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=10, b=0))
            fig_tam.update_traces(textposition="outside")
            st.plotly_chart(fig_tam, use_container_width=True)

    # Leads activos por industria
    with st.container(border=True):
        st.subheader("Leads Activos por Industria")
        df_leads_graf = df_filtered[df_filtered["ES_LEAD"] == True]
        if not df_leads_graf.empty:
            leads_by_ind = df_leads_graf["INDUSTRIA_NOMBRE"].value_counts().reset_index()
            leads_by_ind.columns = ["Industria", "Leads"]
            fig_leads = px.bar(leads_by_ind, x="Leads", y="Industria", orientation="h",
                               text="Leads", color="Industria",
                               color_discrete_sequence=px.colors.qualitative.Bold)
            fig_leads.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=10, b=0))
            fig_leads.update_traces(textposition="outside")
            st.plotly_chart(fig_leads, use_container_width=True)
        else:
            st.info("No hay leads activos aun.")

# =============================================================
# TAB: TOP 10 CUENTAS DE INTERES
# =============================================================

with tab_top10:
    st.subheader("Top 10 Cuentas Principales de Interes (Global)")
    st.caption("Ranking global combinado: datos obtenidos (0-5) + tamano empresa (0-5) | Click en empresa para ver detalle")

    tamano_score_map_g = {"Micro": 1, "Pequena": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
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
# TAB: TOP 15 CUENTAS POR INDUSTRIA
# =============================================================

with tab_top15:
    st.subheader("Top 15 Cuentas de Interes por Industria")

    ind_list_top = sorted(df_filtered["INDUSTRIA_NOMBRE"].unique())
    sel_ind_top15 = st.selectbox("Selecciona industria para Top 15", ind_list_top,
                                  index=ind_list_top.index("Manufacturing & Industrial") if "Manufacturing & Industrial" in ind_list_top else 0)
    st.caption(f"Ranking combinado: datos obtenidos + tamano | Click en empresa para ver detalle — {sel_ind_top15}")

    tamano_score_map = {"Micro": 1, "Pequena": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
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
# TAB: DETALLE DE CUENTAS
# =============================================================

with tab_detalle:
    st.subheader("Detalle de Cuentas")
    st.caption("Click en el nombre de la empresa para ver detalle completo, contactos y generar pitch")

    buscar_cuenta = st.text_input("Buscar cuenta", placeholder="Nombre de empresa...",
                                  key="buscar_detalle", label_visibility="collapsed")
    df_busqueda = df_filtered.copy()
    if buscar_cuenta.strip():
        df_busqueda = df_busqueda[df_busqueda["ACCT_NAME"].str.contains(buscar_cuenta.strip(), case=False, na=False)]

    df_detail = df_busqueda[["ACCT_NAME", "INDUSTRIA_NOMBRE", "CONTACTO_PRINCIPAL", "CONTACTO_CARGO",
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
# TAB: INSIGHTS POR INDUSTRIA
# =============================================================

with tab_insights:
    st.subheader("Insights por Industria: Casos de Uso Snowflake")
    st.caption("Tendencias, retos y oportunidades generadas con Cortex AI para cada industria")

    industrias_con_insights = sorted(df_casos["INDUSTRIA_NOMBRE"].unique())
    if industrias_con_insights:
        tabs_insights = st.tabs(industrias_con_insights)

        for tab_ins, industria in zip(tabs_insights, industrias_con_insights):
            with tab_ins:
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
# APERTURA UNICA DEL DIALOG (evita duplicados)
# =============================================================

if "_open_cuenta" in st.session_state and st.session_state["_open_cuenta"]:
    _cuenta_abrir = st.session_state.pop("_open_cuenta")
    mostrar_tarjeta_cuenta(_cuenta_abrir)

# =============================================================
# PIE DE PAGINA
# =============================================================

st.divider()
st.caption("Dashboard de Territorios Comerciales v3 | DB_TERRITORIOS_COMERCIALES | Snowflake + Cortex AI | EGOS BI")
