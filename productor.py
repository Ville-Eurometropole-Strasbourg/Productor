from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QFileDialog, QErrorMessage
from qgis.core import QgsTask, QgsMessageLog, QgsApplication, Qgis
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction
from .resources import *
from .productor_dialog import ProductorDialog
import os.path
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__)) + '\\include\\python')
import psycopg2

import subprocess

class RestoreTask(QgsTask):
    def __init__(self, pg_string, password):
        super().__init__('Restoring backup')
        self.pg_string = pg_string
        self.password = password     

    def run(self):
        os.environ['PGPASSWORD'] = self.password
        try:
            process = subprocess.Popen(self.pg_string, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            output = stdout.decode('utf-8') + stderr.decode('utf-8')
            process.wait()
            QgsMessageLog.logMessage(output, 'Productor', level=Qgis.Info)
            return True
        except Exception as e:
            QgsMessageLog.logMessage(str(e), 'Productor', level=Qgis.Critical)
            return False

        
class DumpTask(QgsTask):
    def __init__(self, pg_string):
        super().__init__('Dumping table')
        self.pg_string = pg_string
    def run(self):
        try:
            process = os.popen(self.pg_string)
            process.close()
            return True
        except Exception as e:
            QgsMessageLog.logMessage(str(e), 'Productor', level=Qgis.Critical)
            return False

class Productor:
    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            self.plugin_dir,
            'Productor_{}.qm'.format(locale))
        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)
        self.actions = []
        self.menu = self.tr(u'&Productor')
        self.first_start = None

    def tr(self, message):
        return QCoreApplication.translate('Productor', message)

    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):
        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)
        if status_tip is not None:
            action.setStatusTip(status_tip)
        if whats_this is not None:
            action.setWhatsThis(whats_this)
        if add_to_toolbar:
            self.iface.addToolBarIcon(action)
        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)
        self.actions.append(action)
        return action

    def initGui(self):
        icon_path = ':/plugins/productor/icon.png'
        self.add_action(
            icon_path,
            text=self.tr(u'Passage PROD'),
            callback=self.run,
            parent=self.iface.mainWindow())
        self.first_start = True

    def unload(self):
        for action in self.actions:
            self.iface.removePluginMenu(
                self.tr(u'&Productor'),
                action)
            self.iface.removeToolBarIcon(action)

    def run(self):
        if self.first_start == True:
            self.first_start = False
            self.dlg = ProductorDialog()
        self.dlg.pushButton.clicked.connect(self.dump)
        self.dlg.pushButton_2.clicked.connect(self.closeEvent)
        self.dlg.pushButton_3.clicked.connect(self.connection)
        self.dlg.pushButton_4.clicked.connect(self.restore)
        self.dlg.checkBox_2.stateChanged.connect(self.checkBox_2_state_changed)
        self.dlg.checkBox.stateChanged.connect(self.checkBox_state_changed)
        self.dlg.toolButton.clicked.connect(self.choose)
        self.dlg.toolButton_2.clicked.connect(self.choose_2)
        self.dlg.comboBox_3.activated.connect(self.table)
        self.dlg.show()
        self.dlg.closeEvent = self.closeEvent

    def checkBox_2_state_changed(self, state):
        if state == Qt.Checked:
            self.dlg.checkBox.setChecked(False)

    def checkBox_state_changed(self, state):
        if state == Qt.Checked:
            self.dlg.checkBox_2.setChecked(False)

    def table(self):
        if self.dlg.lineEdit_2.text() != 'sigli':
            conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        else:
            conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        try:
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            self.schema = self.dlg.comboBox_3.currentText()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE';", (self.schema,))
            tables = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT table_name FROM information_schema.views WHERE table_schema = %s;", (self.schema,))
            self.views = [row[0] for row in cur.fetchall()]
            self.dlg.comboBox_2.clear()
            self.dlg.comboBox_2.addItems(sorted(tables + self.views))
            self.dlg.lineEdit_2.setStyleSheet(f'QWidget {{background-color:  #009900;}}')
        except psycopg2.Error as err:
            self.dlg.lineEdit_2.setStyleSheet(f'QWidget {{background-color:  #ff0000;}}')
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage('Erreur de Connection' + ':' + str(err))
        finally:
            cur.close() if cur else None
            conn.close() if conn else None
  
    def restore(self):
        pg_path = str(os.path.join(os.path.dirname(__file__))) + "\\include\\python\\pg_restore.exe"
        pg_path = pg_path.replace('/', '\\')
        database = self.dlg.lineEdit_4.text()
        password = self.dlg.lineEdit_5.text()
        folder = self.folder_path_import
        files = os.listdir(folder)
        conn_string = 'postgresql://{}:{}@bdsigli.cus.fr:34000/{}'.format(database, password, database)
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        first_pass =  0 
        for file in files:
            name, ext = os.path.splitext(file)
            if file == "1_enums.sql" and first_pass == 0:
                with open('{}\\1_enums.sql'.format(folder), 'r', encoding="cp1252") as f:
                    lines = f.readlines()
                sql_lines = []
                for line in lines:
                    line = line.strip()
                    if line.startswith('--') or line.startswith('#'):
                        continue
                    if not line:
                        continue
                    if line.startswith('CREATE') or line.startswith('ALTER') or line.startswith('DROP'):
                        sql_lines.append(line)
            for sql_line in sql_lines:
                try:
                    cur.execute(sql_line)
                except psycopg2.errors.DuplicateObject :
                    conn.rollback()
                conn.commit()

            if file == "2_fonctions.sql" and first_pass == 0:
                with open('{}\\2_fonctions.sql'.format(folder), 'r', encoding="cp1252") as f:
                    sql = f.read()
                    functions = sql.split("$function$;\n")
                    for i in range(len(functions) - 1):
                        function = functions[i]
                        if function.strip() == "":
                            continue
                        try:
                            cur.execute(function + "$function$;")
                        except psycopg2.errors.DuplicateObject:
                            conn.rollback()
                        conn.commit()
                    last_function = functions[-1]
                    if last_function.strip() != "":
                        try:
                            cur.execute(last_function)
                        except psycopg2.errors.DuplicateObject:
                            conn.rollback()
                        conn.commit()              

            if ext == ".backup":
                pg_string = r'{} --host bdsigli.cus.fr --port 34000 --no-owner --username "{}"  --section=pre-data --section=data --section=post-data --dbname "{}" "{}\{}" '.format(pg_path, database, database, folder, file)
                task = RestoreTask(pg_string, self.dlg.lineEdit_5.text())
                QgsApplication.taskManager().addTask(task)
                while QgsApplication.taskManager().count() > 0:
                    QCoreApplication.processEvents()
            
            first_pass = 1

        cur.close()
        conn.close()
        
    def dump(self) :
        try :
            if self.dlg.lineEdit_2.text() != 'sigli' : 
                conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
            else :
                conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            written_functions = []
            written_enums = []
            cst_val = []
            pg_path = str(os.path.join(os.path.dirname(__file__))) + "\\include\\python\\pg_dump.exe"
            pg_path = pg_path.replace('/', '\\')
            database = self.dlg.lineEdit_2.text()
            schema = self.dlg.comboBox_3.currentText()
            tables = [item.text() for item in self.dlg.comboBox_2.selectedItems()]
            folder = self.folder_path + '\\export_sql' 
            self.dlg.progressBar.setRange(0, 100)
            total = len(tables)
            progress = 10
            if self.dlg.lineEdit_2.text() != 'sigli' : 
                url ='bdsigli.cus.fr'
                encoding = 'WIN1252'
            else :
                url = 'bpsigli.cus.fr'
                encoding = 'UTF8'
            if os.path.exists(folder) is False : 
                os.mkdir(folder)
            self.dlg.progressBar.setValue(progress)
            for nb, table in enumerate(tables) :
                progress = int((nb + 1) * 100 / total)
                if self.dlg.checkBox.isChecked():
                    pg_string = r'{} --host {} --port 34000 --format=c --no-owner --encoding {} --table {}.{} {} > "{}\3_{}.backup"'.format(pg_path, url, encoding, schema, table, database, folder, table)
                else:
                    pg_string = r'{} --host {} --port 34000 --format=p --schema-only --no-owner --section=data --section=pre-data --section=post-data --encoding {} --table {}.{} {} > "{}\3_{}.sql"'.format(pg_path, url, encoding, schema, table, database, folder, table)
                task = DumpTask(pg_string)
                QgsApplication.taskManager().addTask(task)
                while QgsApplication.taskManager().count() > 0:
                    QCoreApplication.processEvents()
                if self.dlg.checkBox_2.isChecked():
                    file_object = open(r'{}\3_{}.sql'.format(folder, table), 'r+', encoding="cp1252")
                    content = file_object.read()
                    file_object.seek(0,0)
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n' + content)
                    file_object.close()
                # ENUMS
                cur.execute("SELECT c.column_name, n.nspname || '.' || t.typname AS type, c.table_name FROM information_schema.columns c JOIN pg_type t ON c.udt_name = t.typname JOIN pg_namespace n ON t.typnamespace = n.oid LEFT JOIN pg_namespace n1 ON t.typnamespace = n1.oid JOIN information_schema.tables t2 ON c.table_schema = t2.table_schema AND c.table_name = t2.table_name WHERE t.typcategory = 'E' AND c.table_name = '{}'".format(table))
                columns_table = cur.fetchall()
                for c in columns_table:
                    column_type = c[1]  
                    cur.execute("SELECT format( 'CREATE TYPE %s AS ENUM (%s);', enumtypid::regtype, string_agg(quote_literal(enumlabel), ', ') ) FROM pg_enum WHERE enumtypid::regtype = '{}'::regtype GROUP BY enumtypid;".format(column_type))
                    val = cur.fetchone()[0]
                    cst_val.append(str(val))
                cst_val = list(dict.fromkeys(cst_val))
                file_object = open('{}\\1_enums.sql'.format(folder), 'a', encoding="cp1252")
                if file_object.tell() == 0 :
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                    file_object.write('--Création des Enumérations\n')
                for valeur in cst_val :
                    if valeur not in written_enums:
                        file_object.write('{}\n'.format(str(valeur)))
                        written_enums.append((str(valeur)))
                file_object.close()
                # FUNCTIONS
                cur.execute("SELECT pg_proc.proname AS function_name,pg_trigger.tgname AS trigger_name,pg_namespace.nspname AS schema_name FROM pg_trigger LEFT JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid LEFT JOIN pg_proc ON pg_trigger.tgfoid = pg_proc.oid LEFT JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid WHERE pg_class.relname = '{}' AND NOT pg_proc.proname LIKE 'RI_FKey_%' ".format(table))
                result_table_functions = cur.fetchall()
                table_list_functions = []
                for row in result_table_functions:
                    schema_table = row[2] + '.' + row[0]
                    table_list_functions.append(schema_table)
                file_object = open('{}\\2_fonctions.sql'.format(folder), 'a', encoding="cp1252")
                if file_object.tell() == 0 :
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                    file_object.write('--Création des Fonctions\n')
                for table in table_list_functions:
                    cur.execute("SELECT pg_get_functiondef('{}'::regproc)".format(table))
                    function = cur.fetchone()
                    if function[0] not in written_functions:
                        file_object.write('{};\n'.format(function[0]))
                        written_functions.append(function[0])
                file_object.close()
                progress = int((nb + 1) * 100 / total)
                self.dlg.progressBar.setValue(progress)
            self.dlg.progressBar.setValue(0)
            cur.close() 
            conn.close() 
        except Exception as e : 
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage(str(e))
            self.dlg.progressBar.setValue(0)
            cur.close() 
            conn.close() 
            pass 
    
    def choose(self):
        self.folder_path = QFileDialog.getExistingDirectory(self.dlg, 'Select Folder')
        self.folder_path = self.folder_path.replace('/', '\\')
        if self.folder_path:
            self.dlg.lineEdit.setText(self.folder_path)
    
    def choose_2(self):
        self.folder_path_import = QFileDialog.getExistingDirectory(self.dlg, 'Select Folder')
        self.folder_path_import = self.folder_path_import.replace('/', '\\')
        if self.folder_path_import:
            self.dlg.lineEdit_3.setText(self.folder_path_import)

    def connection(self) : 
        if self.dlg.lineEdit_2.text() != 'sigli' : 
            conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        else :
            conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        try :
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name !~ '^(pg_|information_schema)';")
            rows = cur.fetchall()
            list = [row[0] for row in rows]
            self.dlg.comboBox_2.clear()
            self.dlg.comboBox_3.clear()
            self.dlg.comboBox_3.addItems(list)
            self.dlg.lineEdit_2.setStyleSheet(f'QWidget {{background-color:  #009900;}}')
        except Exception as err :
            self.dlg.lineEdit_2.setStyleSheet(f'QWidget {{background-color:  #ff0000;}}')
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage('Erreur de Connection' + ':' + str(err))
            cur.close()
            conn.close()
        cur.close() if cur and not cur.closed else None
        conn.close() if conn and not conn.closed else None

    def closeEvent(self, event):
        self.dlg.comboBox_2.clear()
        self.dlg.comboBox_3.clear()
        self.dlg.lineEdit_2.clear()
        self.dlg.lineEdit.clear()
        self.dlg.lineEdit_2.setStyleSheet("")
        try : 
            self.dlg.toolButton.clicked.disconnect()
        except : 
            pass
        self.dlg.progressBar.setValue(0)
        self.dlg.close()