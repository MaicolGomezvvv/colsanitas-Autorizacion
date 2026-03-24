from __future__ import annotations

import csv
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from app.logging_config import get_application_logger
except ModuleNotFoundError:
    from logging_config import get_application_logger

load_dotenv()
logger = get_application_logger("service")

LOGIN_URL = os.getenv(
    "COLSANITAS_LOGIN_URL",
    "https://portal.colsanitas.com/sso/login?service=https%3A%2F%2Fappcore.colsanitas.com%2FValidadorDerechos%2Fpages%2Fgestion%2FValidacionDerechos.seam",
)
PORTAL_USERNAME = os.getenv("COLSANITAS_USERNAME")
PORTAL_PASSWORD = os.getenv("COLSANITAS_PASSWORD")
APP_ROOT = Path(__file__).resolve().parents[1]
DOWNLOAD_DIR = Path(os.getenv("COLSANITAS_DOWNLOAD_DIR", str(APP_ROOT / "downloads"))).resolve()
HEADLESS = os.getenv("COLSANITAS_HEADLESS", "true").strip().lower() in {"1", "true", "yes", "si"}
TIMEOUT = int(os.getenv("COLSANITAS_TIMEOUT", "60"))
RESULT_TIMEOUT = int(os.getenv("COLSANITAS_RESULT_TIMEOUT", "180"))
AUTH_TIMEOUT = int(os.getenv("COLSANITAS_AUTH_TIMEOUT", str(TIMEOUT)))
AUTH_STEP_TIMEOUT = int(os.getenv("COLSANITAS_AUTH_STEP_TIMEOUT", "20"))
HEADLESS_MODE = os.getenv("COLSANITAS_HEADLESS_MODE", "legacy").strip().lower()
CHROME_BINARY = os.getenv("CHROME_BINARY")
HEADLESS_WINDOW_SIZE = "1920,1080"
VISIBLE_WINDOW_SIZE = "1280,800"

USERNAME_XPATH = '//*[@id="username"]'
PASSWORD_XPATH = '//*[@id="password"]'
LOGIN_BUTTON_XPATH = (
    "//input[contains(@placeholder, 'Ingresar') or contains(@value, 'Ingresar') or @type='submit']"
    " | //button[contains(., 'Ingresar') or @type='submit']"
)
REPORTES_XPATH = "//*[contains(text(),'Reportes')]"
REPORTE_INSTITUCION_XPATH = "//*[contains(text(),'Reporte de Institucion')]"
FECHA_DESDE_XPATH = '//*[@id="formReporteRegistrosAtencion:j_id45:fechaDesdeInputDate"]'
FECHA_HASTA_XPATH = '//*[@id="formReporteRegistrosAtencion:j_id51:fechaHastaInputDate"]'
GENERAR_REPORTE_XPATH = '//*[@id="formReporteRegistrosAtencion:buttonGenerarReporte"]'
PROCESANDO_MODAL_XPATH = '//*[@id="formReporteRegistrosAtencion:ajaxProcesandoReporteInstitucionContentTable"]/tbody/tr/td'
PROCESANDO_ACEPTAR_XPATH = '//*[@id="formReporteRegistrosAtencion:buttonAceptarProcesando"]'
TABLA_REPORTE_WRAPPER_XPATH = '//*[@id="table_wrapper"]/div[2]'
FILA_REPORTE_XPATH = '//*[@id="table"]/tbody/tr'
ESTADO_REPORTE_XPATH = '//*[@id="table"]/tbody/tr[1]/td[5]'
DESCARGAR_FINALIZADO_XPATH = '//*[@id="table"]/tbody/tr[1]/td[6]/a/span'
DESCARGAR_EN_PROCESO_XPATH = '//*[@id="table"]/tbody/tr[1]/td[6]/a/span'


class PortalConfigError(RuntimeError):
    pass


class PortalAutomationError(RuntimeError):
    pass


class ReporteSinRegistrosError(RuntimeError):
    pass


def consultar_reporte_institucion(fecha_desde: str, fecha_hasta: str, documento: str) -> dict[str, Any]:
    csv_path = descargar_reporte_institucion(fecha_desde, fecha_hasta)
    registros = filtrar_registros(csv_path, documento)

    if not registros:
        raise ReporteSinRegistrosError(
            f"No se encontraron registros para el documento {documento} en el archivo {csv_path.name}."
        )

    return {
        "fecha_desde": fecha_desde,
        "fecha_hasta": fecha_hasta,
        "documento": documento,
        "archivo": csv_path.name,
        "ruta_archivo": str(csv_path),
        "total_registros": len(registros),
        "registros": registros,
    }


def descargar_reporte_institucion(fecha_desde: str, fecha_hasta: str) -> Path:
    validar_configuracion()
    download_dir = preparar_directorio_descargas()
    driver = crear_driver(download_dir)
    etapa = "inicio"
    registrar_progreso(f"Iniciando consulta de reporte desde {fecha_desde} hasta {fecha_hasta}.")

    try:
        wait = WebDriverWait(driver, TIMEOUT)

        etapa = "carga de login"
        registrar_progreso("Abriendo portal de login.")
        driver.get(LOGIN_URL)
        etapa = "autenticacion"
        registrar_progreso("Escribiendo credenciales y enviando login.")
        escribir_input(driver, wait, USERNAME_XPATH, PORTAL_USERNAME or "")
        password_input = escribir_input(driver, wait, PASSWORD_XPATH, PORTAL_PASSWORD or "")
        autenticar_en_portal(driver, wait, password_input)

        etapa = "navegacion a reportes"
        registrar_progreso("Login enviado. Navegando al menu Reportes.")
        hacer_click(driver, wait, REPORTES_XPATH)
        hacer_click(driver, wait, REPORTE_INSTITUCION_XPATH)

        etapa = "captura de fechas"
        registrar_progreso("Ingresando rango de fechas del reporte.")
        escribir_input(driver, wait, FECHA_DESDE_XPATH, fecha_desde)
        escribir_input(driver, wait, FECHA_HASTA_XPATH, fecha_hasta)

        etapa = "generacion del reporte"
        registrar_progreso("Generando reporte en el portal.")
        hacer_click(driver, wait, GENERAR_REPORTE_XPATH)

        etapa = "espera del resultado"
        registrar_progreso("Esperando estado del reporte en la tabla.")
        esperar_reporte_listo(driver, wait)

        etapa = "descarga del archivo"
        inicio_descarga = time.time()
        registrar_progreso("Reporte finalizado. Esperando boton de descarga.")
        esperar_boton_descarga(driver, wait)
        registrar_progreso("Descargando archivo CSV.")
        hacer_click(driver, wait, DESCARGAR_FINALIZADO_XPATH)

        archivo = esperar_archivo_descargado(download_dir, inicio_descarga)
        registrar_progreso(f"Archivo descargado correctamente: {archivo.name}")
        return archivo
    except TimeoutException as exc:
        if etapa == "autenticacion":
            capturar_diagnostico_autenticacion(driver, download_dir)
        registrar_progreso(f"Timeout detectado en la etapa: {etapa}")
        raise PortalAutomationError(
            f"El portal no respondió dentro del tiempo esperado durante la etapa: {etapa}."
        ) from exc
    except WebDriverException as exc:
        registrar_progreso(f"Error de Selenium en la etapa {etapa}: {exc}")
        raise PortalAutomationError(f"Error de Selenium al ejecutar la automatización: {exc}") from exc
    finally:
        registrar_progreso("Cerrando navegador Selenium.")
        driver.quit()


def filtrar_registros(csv_path: Path, documento: str) -> list[dict[str, str]]:
    last_error: UnicodeDecodeError | None = None

    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with csv_path.open("r", encoding=encoding, newline="") as file_handle:
                reader = csv.DictReader(file_handle)
                if not reader.fieldnames:
                    raise PortalAutomationError("El CSV descargado no contiene encabezados.")

                encabezados = [header.strip() for header in reader.fieldnames]
                if "NUM_DOC" not in encabezados:
                    raise PortalAutomationError("El CSV descargado no contiene la columna NUM_DOC.")

                registros: list[dict[str, str]] = []
                for row in reader:
                    registro = normalizar_registro(row)
                    if registro.get("NUM_DOC", "").strip() == documento:
                        registros.append(registro)

                return registros
        except UnicodeDecodeError as exc:
            last_error = exc

    raise PortalAutomationError(f"No fue posible leer el CSV descargado: {last_error}")


def normalizar_registro(row: dict[str, Any]) -> dict[str, str]:
    return {
        (key or "").strip(): (value.strip() if isinstance(value, str) else "")
        for key, value in row.items()
    }


def validar_configuracion() -> None:
    if not PORTAL_USERNAME:
        raise PortalConfigError("Debe configurar COLSANITAS_USERNAME en el archivo .env.")

    if not PORTAL_PASSWORD:
        raise PortalConfigError("Debe configurar COLSANITAS_PASSWORD en el archivo .env.")


def preparar_directorio_descargas() -> Path:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    return DOWNLOAD_DIR


def crear_driver(download_dir: Path) -> webdriver.Chrome:
    chrome_options = Options()

    if HEADLESS:
        if HEADLESS_MODE in {"new", "chrome-new"}:
            chrome_options.add_argument("--headless=new")
        else:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument(f"--window-size={HEADLESS_WINDOW_SIZE}")
        chrome_options.add_argument("--use-angle=swiftshader")
        chrome_options.add_argument("--enable-unsafe-swiftshader")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    else:
        chrome_options.add_argument(f"--window-size={VISIBLE_WINDOW_SIZE}")

    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    if CHROME_BINARY:
        chrome_options.binary_location = CHROME_BINARY

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {
            "behavior": "allow",
            "downloadPath": str(download_dir),
        },
    )

    if not HEADLESS:
        ancho, alto = (int(valor) for valor in VISIBLE_WINDOW_SIZE.split(",", maxsplit=1))
        driver.set_window_position(40, 40)
        driver.set_window_size(ancho, alto)

    return driver


def hacer_click(driver: webdriver.Chrome, wait: WebDriverWait, xpath: str) -> None:
    elemento = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elemento)

    try:
        elemento.click()
    except WebDriverException:
        try:
            driver.execute_script("arguments[0].click();", elemento)
        except WebDriverException:
            hacer_click_por_xpath_js(driver, xpath)


def escribir_input(driver: webdriver.Chrome, wait: WebDriverWait, xpath: str, value: str) -> Any:
    elemento = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elemento)

    try:
        elemento.click()
        elemento.send_keys(Keys.CONTROL, "a")
        elemento.send_keys(Keys.DELETE)
        elemento.send_keys(value)
    except WebDriverException:
        driver.execute_script(
            """
            const element = arguments[0];
            const newValue = arguments[1];
            element.removeAttribute('readonly');
            element.value = newValue;
            element.dispatchEvent(new Event('input', { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
            element.dispatchEvent(new Event('blur', { bubbles: true }));
            """,
            elemento,
            value,
        )

    return elemento


def autenticar_en_portal(driver: webdriver.Chrome, wait: WebDriverWait, password_input: Any) -> None:
    # Some SSO pages behave differently in headless mode; try multiple submit strategies.
    metodos_envio = [
        ("click en boton", lambda: enviar_login_por_click(driver, wait)),
        ("tecla ENTER", lambda: enviar_login_por_enter(password_input)),
        ("submit JS del formulario", lambda: enviar_login_por_js(driver, password_input)),
    ]

    ultimo_error: Exception | None = None

    for indice, (nombre_metodo, metodo) in enumerate(metodos_envio, start=1):
        registrar_progreso(f"Intento de autenticacion {indice}: {nombre_metodo}.")

        try:
            metodo()
        except Exception as exc:  # noqa: BLE001
            ultimo_error = exc
            registrar_progreso(f"Fallo el envio por {nombre_metodo}: {exc}")
            continue

        try:
            esperar_login_completado(driver, timeout=AUTH_STEP_TIMEOUT)
            return
        except TimeoutException as exc:
            ultimo_error = exc
            bloqueo = detectar_bloqueo_antibot(driver)
            if bloqueo:
                raise PortalAutomationError(mensaje_bloqueo_antibot(driver, bloqueo)) from exc

            if sigue_en_login(driver):
                registrar_progreso(
                    f"No se completo login con {nombre_metodo}. Aun en login, se intentara otro metodo."
                )
                continue

            # If the page changed context but report menu is not yet visible, wait once with full timeout.
            esperar_login_completado(driver, timeout=AUTH_TIMEOUT)
            return

    raise TimeoutException(f"No fue posible autenticarse en el portal. Ultimo error: {ultimo_error}")


def enviar_login_por_click(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    wait_corto = WebDriverWait(driver, min(TIMEOUT, AUTH_STEP_TIMEOUT))
    hacer_click(driver, wait_corto, LOGIN_BUTTON_XPATH)


def enviar_login_por_enter(password_input: Any) -> None:
    password_input.send_keys(Keys.ENTER)


def enviar_login_por_js(driver: webdriver.Chrome, password_input: Any) -> None:
    resultado = driver.execute_script(
        """
        const passwordElement = arguments[0];
        if (!passwordElement) {
            return false;
        }

        const form = passwordElement.closest('form');
        if (form) {
            form.submit();
            return true;
        }

        const submitButton = document.evaluate(
            arguments[1],
            document,
            null,
            XPathResult.FIRST_ORDERED_NODE_TYPE,
            null,
        ).singleNodeValue;

        if (submitButton) {
            submitButton.click();
            return true;
        }

        return false;
        """,
        password_input,
        LOGIN_BUTTON_XPATH,
    )

    if not resultado:
        raise WebDriverException("No se pudo enviar el login por JavaScript.")


def esperar_login_completado(driver: webdriver.Chrome, timeout: int = AUTH_TIMEOUT) -> None:
    espera_login = WebDriverWait(driver, timeout)

    def _login_completado(current_driver: webdriver.Chrome) -> bool:
        url_actual = current_driver.current_url.lower()
        sigue_en_sso = "portal.colsanitas.com/sso/login" in url_actual
        tiene_form_login = bool(
            current_driver.find_elements(By.XPATH, USERNAME_XPATH)
            and current_driver.find_elements(By.XPATH, PASSWORD_XPATH)
        )
        menu_reportes_visible = bool(current_driver.find_elements(By.XPATH, REPORTES_XPATH))
        return menu_reportes_visible or (not sigue_en_sso and not tiene_form_login)

    espera_login.until(_login_completado)


def sigue_en_login(driver: webdriver.Chrome) -> bool:
    url_actual = driver.current_url.lower()
    if "portal.colsanitas.com/sso/login" in url_actual:
        return True

    tiene_usuario = bool(driver.find_elements(By.XPATH, USERNAME_XPATH))
    tiene_password = bool(driver.find_elements(By.XPATH, PASSWORD_XPATH))
    return tiene_usuario and tiene_password


def esperar_modal_y_aceptar(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    wait.until(EC.visibility_of_element_located((By.XPATH, PROCESANDO_MODAL_XPATH)))
    hacer_click(driver, wait, PROCESANDO_ACEPTAR_XPATH)


def esperar_reporte_listo(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    fin_espera = time.time() + RESULT_TIMEOUT
    ultimo_estado = ""
    ultimo_log_sin_estado = 0.0
    ultimo_log_en_proceso = 0.0
    ultima_confirmacion_modal = 0.0
    ultimo_contexto_tabla = ""

    while time.time() < fin_espera:
        driver.switch_to.default_content()

        if elemento_visible(driver, PROCESANDO_MODAL_XPATH) and time.time() - ultima_confirmacion_modal >= 2:
            registrar_progreso("Modal de procesamiento detectado. Confirmando aviso.")
            hacer_click(driver, wait, PROCESANDO_ACEPTAR_XPATH)
            ultima_confirmacion_modal = time.time()

        contexto_tabla = cambiar_a_contexto_tabla_si_existe(driver)
        if contexto_tabla and contexto_tabla != ultimo_contexto_tabla:
            registrar_progreso(f"Contenedor de tabla detectado en: {contexto_tabla}")
            ultimo_contexto_tabla = contexto_tabla

        estado_actual = obtener_estado_reporte(driver)
        if estado_actual and estado_actual != ultimo_estado:
            registrar_progreso(f"Estado actual del reporte: {estado_actual}")
            ultimo_estado = estado_actual

        if not estado_actual and time.time() - ultimo_log_sin_estado >= 10:
            resumen_tabla = obtener_resumen_tabla_reporte(driver)
            registrar_progreso(
                "Aun no aparece fila de estado en la tabla. "
                f"URL actual: {driver.current_url}. Tabla: {resumen_tabla}"
            )
            ultimo_log_sin_estado = time.time()

        if estado_actual == "Finalizado" and elemento_descargable(driver, DESCARGAR_FINALIZADO_XPATH):
            registrar_progreso("Estado Finalizado detectado. El enlace de descarga ya esta disponible.")
            return

        if estado_actual == "En proceso" and time.time() - ultimo_log_en_proceso >= 10:
            registrar_progreso("El reporte sigue en proceso en td[5]. Esperando finalizacion.")
            ultimo_log_en_proceso = time.time()

        time.sleep(1)

    registrar_progreso(
        "No se detecto estado Finalizado a tiempo. "
        f"Ultimo estado observado: {ultimo_estado or 'sin fila en tabla'}. "
        f"URL final: {driver.current_url}."
    )
    raise TimeoutException("El reporte no llego al estado Finalizado dentro del tiempo esperado.")


def elemento_visible(driver: webdriver.Chrome, xpath: str) -> bool:
    elementos = driver.find_elements(By.XPATH, xpath)
    return any(elemento.is_displayed() for elemento in elementos)


def elemento_descargable(driver: webdriver.Chrome, xpath: str) -> bool:
    elementos = driver.find_elements(By.XPATH, xpath)
    for elemento in elementos:
        if elemento.is_displayed() and elemento.is_enabled():
            return True

    datos_xpath = inspeccionar_xpath_js(driver, xpath)
    return bool(datos_xpath.get("exists") and datos_xpath.get("visible"))

    return False


def esperar_boton_descarga(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    if elemento_descargable(driver, DESCARGAR_FINALIZADO_XPATH):
        return

    boton = wait.until(EC.visibility_of_element_located((By.XPATH, DESCARGAR_FINALIZADO_XPATH)))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton)
    wait.until(EC.element_to_be_clickable((By.XPATH, DESCARGAR_FINALIZADO_XPATH)))


def esperar_archivo_descargado(download_dir: Path, inicio_descarga: float) -> Path:
    end_time = time.time() + TIMEOUT
    ultimo_tamano: dict[Path, int] = {}

    while time.time() < end_time:
        archivos = sorted(download_dir.glob("*.csv"), key=lambda item: item.stat().st_mtime, reverse=True)
        for archivo in archivos:
            if archivo.stat().st_mtime + 1 < inicio_descarga:
                continue

            tamano_actual = archivo.stat().st_size
            tamano_previo = ultimo_tamano.get(archivo)
            ultimo_tamano[archivo] = tamano_actual

            if tamano_previo is not None and tamano_previo == tamano_actual and tamano_actual > 0:
                return archivo

        if any(download_dir.glob("*.crdownload")):
            time.sleep(1)
            continue

        time.sleep(1)

    raise PortalAutomationError("No se detectó la descarga del archivo CSV dentro del tiempo esperado.")


def obtener_estado_reporte(driver: webdriver.Chrome) -> str:
    texto_fila = obtener_texto_fila_reporte(driver)
    if "Finalizado" in texto_fila:
        return "Finalizado"

    if "En proceso" in texto_fila:
        return "En proceso"

    elementos_estado = driver.find_elements(By.XPATH, ESTADO_REPORTE_XPATH)
    if elementos_estado:
        texto_estado = extraer_texto_elemento(elementos_estado[0])
        if "Finalizado" in texto_estado:
            return "Finalizado"
        if "En proceso" in texto_estado:
            return "En proceso"
        if texto_estado:
            return texto_estado

    texto_tabla = obtener_texto_tabla_reporte(driver)
    if "Finalizado" in texto_tabla:
        return "Finalizado"

    if "En proceso" in texto_tabla:
        return "En proceso"

    return ""


def obtener_texto_fila_reporte(driver: webdriver.Chrome) -> str:
    datos_xpath = inspeccionar_xpath_js(driver, FILA_REPORTE_XPATH)
    if datos_xpath.get("text"):
        return str(datos_xpath["text"])

    elementos_fila = driver.find_elements(By.XPATH, FILA_REPORTE_XPATH)
    if not elementos_fila:
        return ""

    return extraer_texto_elemento(elementos_fila[0])


def obtener_texto_tabla_reporte(driver: webdriver.Chrome) -> str:
    datos_xpath = inspeccionar_xpath_js(driver, TABLA_REPORTE_WRAPPER_XPATH)
    if datos_xpath.get("text"):
        return str(datos_xpath["text"])

    elementos = driver.find_elements(By.XPATH, TABLA_REPORTE_WRAPPER_XPATH)
    if not elementos:
        return ""

    textos = []
    for elemento in elementos:
        texto = extraer_texto_elemento(elemento)
        if texto:
            textos.append(texto)

    return " ".join(textos)


def obtener_resumen_tabla_reporte(driver: webdriver.Chrome, limite: int = 220) -> str:
    texto = obtener_texto_tabla_reporte(driver)
    if not texto:
        return "sin contenido"

    texto = " ".join(texto.split())
    if len(texto) <= limite:
        return texto

    return f"{texto[:limite]}..."


def registrar_progreso(mensaje: str) -> None:
    logger.info(mensaje)


def capturar_diagnostico_autenticacion(driver: webdriver.Chrome, download_dir: Path) -> None:
    marca = datetime.now().strftime("%Y%m%d_%H%M%S")
    carpeta_debug = download_dir / "debug_auth"
    carpeta_debug.mkdir(parents=True, exist_ok=True)

    screenshot_path = carpeta_debug / f"auth_timeout_{marca}.png"
    html_path = carpeta_debug / f"auth_timeout_{marca}.html"

    try:
        driver.save_screenshot(str(screenshot_path))
        registrar_progreso(f"Diagnostico guardado (screenshot): {screenshot_path}")
    except Exception as exc:  # noqa: BLE001
        registrar_progreso(f"No fue posible guardar screenshot de autenticacion: {exc}")

    try:
        html_path.write_text(driver.page_source, encoding="utf-8")
        registrar_progreso(f"Diagnostico guardado (html): {html_path}")
    except Exception as exc:  # noqa: BLE001
        registrar_progreso(f"No fue posible guardar HTML de autenticacion: {exc}")


def detectar_bloqueo_antibot(driver: webdriver.Chrome) -> str:
    titulo = (driver.title or "").strip()
    pagina = (driver.page_source or "").lower()
    url_actual = (driver.current_url or "").lower()

    if "radware block page" in titulo.lower() or "h-captcha" in pagina or "perfdrive" in pagina:
        return "radware-hcaptcha"

    if "captcha" in pagina and "bot" in pagina:
        return "captcha-botwall"

    if "validate.perfdrive.com" in url_actual:
        return "perfdrive-validate"

    return ""


def extraer_incident_id_bloqueo(driver: webdriver.Chrome) -> str:
    try:
        pagina = driver.page_source or ""
        marcador = "Incident ID:"
        indice = pagina.find(marcador)
        if indice == -1:
            return ""

        fragmento = pagina[indice : indice + 180]
        texto_limpio = " ".join(fragmento.replace("<", " <").replace(">", "> ").split())
        partes = texto_limpio.split(marcador, maxsplit=1)
        if len(partes) < 2:
            return ""

        posible_id = partes[1].split()[0].strip()
        return posible_id.replace("</b>", "").replace("<", "").replace(">", "")
    except Exception:  # noqa: BLE001
        return ""


def mensaje_bloqueo_antibot(driver: webdriver.Chrome, tipo_bloqueo: str) -> str:
    incident_id = extraer_incident_id_bloqueo(driver)
    suffix_incident = f" Incident ID: {incident_id}." if incident_id else ""
    return (
        "El portal bloqueo la sesion automatizada con un desafio anti-bot "
        f"({tipo_bloqueo}) durante la autenticacion en modo headless.{suffix_incident} "
        "Pruebe con COLSANITAS_HEADLESS=false o gestione el desbloqueo con el portal."
    )


def extraer_texto_elemento(elemento: Any) -> str:
    texto_visible = (elemento.text or "").strip()
    if texto_visible:
        return " ".join(texto_visible.split())

    texto_dom = (elemento.get_attribute("textContent") or "").strip()
    if texto_dom:
        return " ".join(texto_dom.split())

    texto_interno = (elemento.get_attribute("innerText") or "").strip()
    if texto_interno:
        return " ".join(texto_interno.split())

    return ""


def hacer_click_por_xpath_js(driver: webdriver.Chrome, xpath: str) -> None:
    resultado = driver.execute_script(
        """
        const xpath = arguments[0];
        const node = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
        if (!node) {
            return false;
        }
        node.scrollIntoView({ block: 'center' });
        node.click();
        return true;
        """,
        xpath,
    )
    if not resultado:
        raise WebDriverException(f"No fue posible hacer click por JS sobre el xpath: {xpath}")


def inspeccionar_xpath_js(driver: webdriver.Chrome, xpath: str) -> dict[str, Any]:
    return driver.execute_script(
        """
        const xpath = arguments[0];
        const node = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
        if (!node) {
            return { exists: false, visible: false, text: '' };
        }

        const text = (node.textContent || node.innerText || '').trim().replace(/\s+/g, ' ');
        const style = window.getComputedStyle(node);
        const visible = style && style.display !== 'none' && style.visibility !== 'hidden';
        return { exists: true, visible, text };
        """,
        xpath,
    )


def cambiar_a_contexto_tabla_si_existe(driver: webdriver.Chrome) -> str:
    if xpath_existe_en_contexto_actual(driver, FILA_REPORTE_XPATH) or xpath_existe_en_contexto_actual(
        driver, TABLA_REPORTE_WRAPPER_XPATH
    ):
        return "documento principal"

    driver.switch_to.default_content()
    frames = driver.find_elements(By.TAG_NAME, "iframe") + driver.find_elements(By.TAG_NAME, "frame")
    for indice, frame in enumerate(frames):
        driver.switch_to.default_content()
        driver.switch_to.frame(frame)
        if xpath_existe_en_contexto_actual(driver, FILA_REPORTE_XPATH) or xpath_existe_en_contexto_actual(
            driver, TABLA_REPORTE_WRAPPER_XPATH
        ):
            return f"iframe[{indice}]"

    driver.switch_to.default_content()
    return ""


def xpath_existe_en_contexto_actual(driver: webdriver.Chrome, xpath: str) -> bool:
    datos_xpath = inspeccionar_xpath_js(driver, xpath)
    return bool(datos_xpath.get("exists"))


def validar_formato_fecha(fecha: str) -> str:
    try:
        datetime.strptime(fecha, "%d/%m/%Y")
    except ValueError as exc:
        raise ValueError("La fecha debe tener el formato dd/mm/yyyy.") from exc

    return fecha