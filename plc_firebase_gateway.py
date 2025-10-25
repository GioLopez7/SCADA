"""
Gateway PLC-Firebase adaptado para estructura UMNG
Conecta TIA Portal (PLC S7) con Firebase Realtime Database
Usa las direcciones de memoria reales del proyecto
"""

import firebase_admin
from firebase_admin import credentials, db
from snap7 import client
from snap7.util import *
import time
from datetime import datetime
import struct
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------- Configuración PLC -----------------
PLC_IP = '192.168.0.10'  # Cambiar por la IP real de tu PLC
PLC_RACK = 0
PLC_SLOT = 1

# Firebase configuration
FIREBASE_CREDS = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://console.firebase.google.com/u/0/project/scada-3bc42/firestore/databases/-default-/data/~2Fcontrol_commands~2FHOCYW3jHFck3AOKlElqf?hl=es-419"

# Frecuencia de actualización
UPDATE_INTERVAL = 1.0  # segundos

# ----------------- Mapeo de Variables UMNG -----------------
"""
Según tu estructura en TIA Portal:

TELEMETRÍA (PLC → Firebase):
- Nivel_Tanque: Int en %MW2
- Sensor_Nivel_Norm: Real en %MD6
- Velocidad_Final: Int en %MW16
- Clock_2Hz: Bool en %M0.3
- Luz_Setpoint: Bool en %Q0.2 (alarma cuando alcanza setpoint)
- LEH_Nivel_Alto: Bool en %I0.3
- LEL_Nivel_Bajo: Bool en %I0.4
- Setpoint: Int en %MW4

COMANDOS (Firebase → PLC):
- Start: Bool en %I0.0
- Stop: Bool en %I0.1
- Emergency: Bool en %I0.2
- Setpoint: Int en %MW4 (para escribir nuevo setpoint)
"""

# ----------------- Inicializar Firebase -----------------
try:
    cred = credentials.Certificate(FIREBASE_CREDS)
    firebase_admin.initialize_app(cred, {
        'databaseURL': FIREBASE_DB_URL
    })
    logger.info("✓ Firebase inicializado correctamente")
except Exception as e:
    logger.error(f"✗ Error inicializando Firebase: {e}")
    exit(1)

# ----------------- Clase Gateway -----------------
class PLCFirebaseGateway:
    def __init__(self):
        self.plc = client.Client()
        self.connected = False
        self.last_command_id = None
        self.last_setpoint = None
        
    def connect_plc(self):
        """Conecta al PLC S7"""
        try:
            self.plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
            self.connected = True
            logger.info(f"✓ Conectado al PLC en {PLC_IP}")
            return True
        except Exception as e:
            logger.error(f"✗ Error conectando al PLC: {e}")
            self.connected = False
            return False
    
    def disconnect_plc(self):
        """Desconecta del PLC"""
        if self.connected:
            self.plc.disconnect()
            self.connected = False
            logger.info("PLC desconectado")
    
    def read_telemetry_from_plc(self):
        """
        Lee datos de telemetría del PLC usando las direcciones reales
        
        Lectura desde área de Marcas (%M) y Entradas/Salidas (%I/%Q)
        """
        try:
            # Leer área de marcas M0-M20 (suficiente para cubrir todas las variables)
            merker_data = self.plc.read_area(0x83, 0, 0, 20)  # 0x83 = Merker (M)
            
            # Leer entradas digitales I0.0-I0.7
            inputs_data = self.plc.read_area(0x81, 0, 0, 1)   # 0x81 = Inputs (I)
            
            # Leer salidas digitales Q0.0-Q0.7
            outputs_data = self.plc.read_area(0x82, 0, 0, 1)  # 0x82 = Outputs (Q)
            
            # Parsear datos según estructura
            # Nivel_Tanque: Int en MW2 (bytes 2-3)
            nivel_tanque_raw = get_int(merker_data, 2)  # %MW2
            
            # Sensor_Nivel_Norm: Real en MD6 (bytes 6-9)
            sensor_nivel_norm = get_real(merker_data, 6)  # %MD6
            
            # Setpoint: Int en MW4 (bytes 4-5)
            setpoint = get_int(merker_data, 4)  # %MW4
            
            # Velocidad_Final: Int en MW16 (bytes 16-17)
            velocidad_final = get_int(merker_data, 16)  # %MW16
            
            # Clock_2Hz: Bool en M0.3
            clock_2hz = get_bool(merker_data, 0, 3)  # %M0.3
            
            # Luz_Setpoint: Bool en Q0.2
            luz_setpoint = get_bool(outputs_data, 0, 2)  # %Q0.2
            
            # LEH_Nivel_Alto: Bool en I0.3
            leh_nivel_alto = get_bool(inputs_data, 0, 3)  # %I0.3
            
            # LEL_Nivel_Bajo: Bool en I0.4
            lel_nivel_bajo = get_bool(inputs_data, 0, 4)  # %I0.4
            
            # Error: Int en MW10 (opcional)
            error = get_int(merker_data, 10)  # %MW10
            
            # Construir objeto de telemetría
            telemetry = {
                'timestamp': datetime.now().isoformat(),
                'level_cm': round(float(sensor_nivel_norm), 2),  # Nivel normalizado (Real)
                'level_raw': int(nivel_tanque_raw),              # Nivel raw (Int)
                'vfd_rpm': int(velocidad_final),                 # Velocidad final (RPM)
                'vfd_speedcmd': int(velocidad_final),            # Mismo valor
                'setpoint': int(setpoint),                       # Setpoint actual
                'blink_2hz': int(clock_2hz),                     # Parpadeo 2Hz
                'reached_sp': int(luz_setpoint),                 # Alcanzó setpoint
                'low_level': int(lel_nivel_bajo),                # Alarma nivel bajo
                'high_level': int(leh_nivel_alto),               # Alarma nivel alto
                'error': int(error)                              # Error del sistema
            }
            
            return telemetry
        except Exception as e:
            logger.error(f"Error leyendo telemetría del PLC: {e}")
            return None
    
    def write_telemetry_to_firebase(self, telemetry):
        """Escribe telemetría en Firebase"""
        try:
            ref = db.reference('telemetry_samples')
            ref.push(telemetry)
            logger.debug(f"Telemetría: Nivel={telemetry['level_cm']}cm, RPM={telemetry['vfd_rpm']}, SP={telemetry['setpoint']}")
            
            # Actualizar estado actual
            status_ref = db.reference('current_status')
            status_ref.update({
                'last_update': telemetry['timestamp'],
                'level_cm': telemetry['level_cm'],
                'vfd_rpm': telemetry['vfd_rpm'],
                'setpoint': telemetry['setpoint'],
                'system_running': telemetry['blink_2hz'] == 1,
                'alarm_low': telemetry['low_level'] == 1,
                'alarm_high': telemetry['high_level'] == 1
            })
            
            return True
        except Exception as e:
            logger.error(f"Error escribiendo a Firebase: {e}")
            return False
    
    def check_commands_from_firebase(self):
        """Lee comandos pendientes desde Firebase"""
        try:
            ref = db.reference('control_commands')
            
            # Obtener comandos no procesados
            commands = ref.order_by_child('processed').equal_to(False).limit_to_first(1).get()
            
            if commands:
                for cmd_id, cmd_data in commands.items():
                    # Evitar procesar el mismo comando múltiples veces
                    if cmd_id == self.last_command_id:
                        continue
                    
                    logger.info(f"📩 Nuevo comando recibido: {cmd_id}")
                    
                    # Procesar comando
                    success = self.write_command_to_plc(cmd_data)
                    
                    if success:
                        # Marcar como procesado
                        ref.child(cmd_id).update({
                            'processed': True,
                            'processed_at': datetime.now().isoformat()
                        })
                        self.last_command_id = cmd_id
                        logger.info(f"✓ Comando {cmd_id} procesado")
                    else:
                        logger.error(f"✗ Error procesando comando {cmd_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error leyendo comandos de Firebase: {e}")
            return False
    
    def write_command_to_plc(self, command):
        """
        Escribe comandos en el PLC
        
        NOTA: Start, Stop, Emergency están en entradas (%I) en tu configuración,
        lo cual es inusual. Normalmente serían salidas o marcas.
        
        Si quieres controlar desde Firebase, deberías usar marcas (%M) o salidas (%Q).
        Por ahora, escribiré en marcas alternativas que puedes leer en tu programa.
        """
        try:
            # Opción 1: Escribir en marcas de comando (usar M14-M15 que parecen libres)
            # Leer marcas actuales
            merker_data = bytearray(self.plc.read_area(0x83, 0, 14, 6))
            
            # Escribir comandos en M14.x (puedes ajustar según necesites)
            # M14.1 = Pulso Start desde Firebase
            # M14.2 = Pulso Stop desde Firebase
            # M14.3 = Pulso Emergency desde Firebase
            
            if command.get('cmd_start', 0) == 1:
                set_bool(merker_data, 0, 1, True)  # M14.1
                logger.info("  → START enviado (M14.1)")
            
            if command.get('cmd_stop', 0) == 1:
                set_bool(merker_data, 0, 2, True)  # M14.2
                logger.info("  → STOP enviado (M14.2)")
            
            if command.get('cmd_estop', 0) == 1:
                set_bool(merker_data, 0, 3, True)  # M14.3
                logger.info("  → E-STOP enviado (M14.3)")
            
            # Escribir setpoint si existe
            if 'sp_ref_cm' in command:
                sp_value = int(command['sp_ref_cm'])
                # Escribir en MW4 (Setpoint)
                set_int(merker_data, 4, sp_value)
                logger.info(f"  → Setpoint: {sp_value} cm (MW4)")
                self.last_setpoint = sp_value
            
            # Escribir al PLC
            self.plc.write_area(0x83, 0, 14, merker_data)
            
            # Limpiar pulsos después de un breve delay
            if command.get('cmd_start', 0) == 1 or command.get('cmd_stop', 0) == 1 or command.get('cmd_estop', 0) == 1:
                time.sleep(0.3)  # Dar tiempo al PLC para detectar el pulso
                
                # Reset de pulsos
                reset_data = bytearray(self.plc.read_area(0x83, 0, 14, 6))
                set_bool(reset_data, 0, 1, False)  # M14.1
                set_bool(reset_data, 0, 2, False)  # M14.2
                set_bool(reset_data, 0, 3, False)  # M14.3
                self.plc.write_area(0x83, 0, 14, reset_data)
            
            return True
        except Exception as e:
            logger.error(f"Error escribiendo comando al PLC: {e}")
            return False
    
    def cleanup_old_telemetry(self, days_to_keep=7):
        """Limpia datos antiguos de Firebase"""
        try:
            ref = db.reference('telemetry_samples')
            cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
            
            all_data = ref.get()
            if all_data:
                count = 0
                for key, value in all_data.items():
                    try:
                        timestamp = datetime.fromisoformat(value['timestamp']).timestamp()
                        if timestamp < cutoff_time:
                            ref.child(key).delete()
                            count += 1
                    except:
                        pass
                
                if count > 0:
                    logger.info(f"🗑️  {count} registros antiguos eliminados")
        except Exception as e:
            logger.error(f"Error limpiando datos antiguos: {e}")
    
    def run(self):
        """Loop principal del gateway"""
        logger.info("🚀 Iniciando Gateway PLC-Firebase UMNG...")
        logger.info(f"   PLC: {PLC_IP}")
        logger.info(f"   Intervalo: {UPDATE_INTERVAL}s")
        logger.info(f"   Estructura: Variables en Marcas (%M), Entradas (%I), Salidas (%Q)")
        
        cleanup_counter = 0
        
        while True:
            try:
                # Conectar si no está conectado
                if not self.connected:
                    logger.info("Intentando conectar al PLC...")
                    if not self.connect_plc():
                        logger.warning("Reintentando en 5 segundos...")
                        time.sleep(5)
                        continue
                
                # Leer telemetría del PLC
                telemetry = self.read_telemetry_from_plc()
                
                if telemetry:
                    # Enviar a Firebase
                    self.write_telemetry_to_firebase(telemetry)
                    
                    # Mostrar alarmas si existen
                    if telemetry['low_level'] == 1:
                        logger.warning("⚠️  ALARMA: Nivel bajo activado")
                    if telemetry['high_level'] == 1:
                        logger.warning("⚠️  ALARMA: Nivel alto activado")
                
                # Verificar comandos desde Firebase
                self.check_commands_from_firebase()
                
                # Limpiar datos antiguos cada 1000 iteraciones
                cleanup_counter += 1
                if cleanup_counter >= 1000:
                    self.cleanup_old_telemetry()
                    cleanup_counter = 0
                
                # Esperar antes de la siguiente iteración
                time.sleep(UPDATE_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("\n⏹️  Deteniendo gateway...")
                break
            except Exception as e:
                logger.error(f"Error en loop principal: {e}")
                self.connected = False
                time.sleep(5)
        
        # Cleanup
        self.disconnect_plc()
        logger.info("Gateway detenido correctamente")

# ----------------- Main -----------------
if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║         Gateway PLC S7 ↔ Firebase Realtime DB            ║
    ║              UMNG - Supervisión Industrial                ║
    ║                                                           ║
    ║  Variables mapeadas:                                      ║
    ║  • Nivel: %MW2 (Int) / %MD6 (Real)                       ║
    ║  • RPM: %MW16                                             ║
    ║  • Setpoint: %MW4                                         ║
    ║  • Clock 2Hz: %M0.3                                       ║
    ║  • Luz SP: %Q0.2                                          ║
    ║  • Alarma Alta: %I0.3                                     ║
    ║  • Alarma Baja: %I0.4                                     ║
    ║                                                           ║
    ║  Comandos Firebase → PLC:                                 ║
    ║  • Start: M14.1                                           ║
    ║  • Stop: M14.2                                            ║
    ║  • Emergency: M14.3                                       ║
    ║  • Setpoint: %MW4                                         ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Instrucciones importantes
    print("⚠️  IMPORTANTE: En tu programa TIA Portal, debes leer:")
    print("   - M14.1 para detectar comando Start desde Firebase")
    print("   - M14.2 para detectar comando Stop desde Firebase")
    print("   - M14.3 para detectar comando Emergency desde Firebase")
    print("   - MW4 contiene el setpoint actualizado desde Firebase")
    print("")
    print("💡 Presiona Ctrl+C para detener el gateway\n")
    
    gateway = PLCFirebaseGateway()
    
    try:
        gateway.run()
    except Exception as e:
        logger.error(f"Error fatal: {e}")
    finally:
        gateway.disconnect_plc()
        logger.info("Programa terminado")