from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QFileDialog, QErrorMessage, QListWidgetItem, QAbstractItemView
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
            process = subprocess.Popen(self.pg_string, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            output = stdout.decode('utf-8') + stderr.decode('utf-8')
            process.wait()
            QgsMessageLog.logMessage(output, 'Productor', level=Qgis.Info)
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
        self.dlg.listWidget.setDragEnabled(True)
        self.dlg.listWidget.setAcceptDrops(True)
        self.dlg.listWidget.setDropIndicatorShown(True)
        self.dlg.listWidget.setDragDropMode(QAbstractItemView.InternalMove)
        self.dlg.pushButton.clicked.connect(self.dump)
        self.dlg.pushButton_2.clicked.connect(self.closeEvent)
        self.dlg.pushButton_3.clicked.connect(self.connection)
        self.dlg.pushButton_4.clicked.connect(self.restore)
        self.dlg.toolButton.clicked.connect(self.choose)
        self.dlg.toolButton_2.clicked.connect(self.choose_2)
        self.dlg.comboBox_3.activated.connect(self.table)
        self.dlg.toolButton_3.clicked.connect(self.choose_3)
        self.dlg.pushButton_5.clicked.connect(self.connection_2)
        self.dlg.comboBox.activated.connect(self.enum_fill_table)
        self.dlg.pushButton_6.clicked.connect(self.enumerations)
        self.dlg.add_enum.clicked.connect(self.add_enum)
        self.dlg.delete_enum.clicked.connect(self.delete_enum)
        self.dlg.show()
        self.dlg.closeEvent = self.closeEvent

    def add_enum(self): 
        item = QListWidgetItem('Nouvelle valeur')
        item.setFlags(item.flags() | QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsDragEnabled)
        self.dlg.listWidget.addItem(item)

    def delete_enum(self):
        current_item = self.dlg.listWidget.currentItem()
        if current_item is not None:
            self.dlg.listWidget.takeItem(self.dlg.listWidget.row(current_item))

    def table(self):
        if self.dlg.lineEdit_2.text() != 'sigli':
            conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        else:
            conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        try:
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            self.schema = self.dlg.comboBox_3.currentText()
            cur.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = %s;", (self.schema,))
            self.tables = {row[0]: row[1] for row in cur.fetchall()}
            self.dlg.comboBox_2.clear()
            self.dlg.comboBox_2.addItems(sorted(self.tables.keys()))
        except psycopg2.Error as err:
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage('Erreur de chargement des tables' + ':' + str(err))
        finally:
            cur.close() if cur else None
            conn.close() if conn else None
        
    def restore(self):
        try :
            pg_path = str(os.path.join(os.path.dirname(__file__))) + "\\include\\python\\pg_restore.exe"
            pg_path = pg_path.replace('/', '\\')
            database = self.dlg.lineEdit_4.text()
            password = self.dlg.lineEdit_5.text()
            conn_string = 'postgresql://{}:{}@bdsigli.cus.fr:34000/{}'.format(database, password, database)
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            folder = self.folder_path_import
            files = os.listdir(folder)
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
                        try:
                            cur.execute(sql)
                        except psycopg2.errors.DuplicateObject:
                            conn.rollback()
                        conn.commit()
                first_pass = 1 
            files = os.listdir(folder + '\\structures')                   
            for file in files:
                name, ext = os.path.splitext(file)
                if ext == ".sql":

                    with open('{}\\structures\\{}'.format(folder, file), 'r', encoding="UTF8") as f:
                        sql = f.read()
                        try:
                            self.iface.messageBar().pushMessage("restore ok", level=Qgis.Critical)
                            cur.execute(sql)
                        except Exception as e :
                            self.iface.messageBar().pushMessage(str(e), level=Qgis.Critical)
                            conn.rollback()
                        conn.commit() 
            """
            files = os.listdir(folder + '\\données')                   
            for file in files:
                name, ext = os.path.splitext(file)
                if ext == ".backup":
                    parts = name.split("__-__")
                    schema = parts[0].split("_")[1]
                    table = parts[1]
                    cur.execute("SELECT admin_sigli.devalide_triggers('{}', '{}');".format(schema, table))
                    pg_string = r'{} --host bdsigli.cus.fr --port 34000 --no-owner --username "{}"  --data-only  --dbname "{}" "{}\\données\\{}" '.format(pg_path, database, database, folder, file)
                    task = RestoreTask(pg_string, self.dlg.lineEdit_5.text())
                    QgsApplication.taskManager().addTask(task)
                    while QgsApplication.taskManager().count() > 0:
                        QCoreApplication.processEvents()  
                    cur.execute("SELECT admin_sigli.valide_triggers('{}', '{}');".format(schema, table))
            """
        except Exception as e : 
            self.error_dialog = QErrorMessage()
            
            self.error_dialog.showMessage(str(e))
            self.dlg.progressBar_2.setValue(0)
            pass 
        cur.close() if cur is not None else None
        conn.close() if conn is not None else None

    def dumper(self, cur, url, encoding, schema, table, database, folder, dump_nbr, nb, total, cst_val, type):
        pg_path = str(os.path.join(os.path.dirname(__file__))) + "\\include\\python\\pg_dump.exe"
        pg_path = pg_path.replace('/', '\\')
        """
        if self.dlg.checkBox.isChecked() and type != 'view':
            dump_nbr = '4'
            pg_string = r'{} --host {} --port 34000 --format=c --no-owner --data-only --encoding {} --table {}.{} {} > "{}\données\{}_{}__-__{}.backup"'.format(pg_path, url, encoding, schema, table, database, folder, dump_nbr, schema, table)
            task = DumpTask(pg_string)
            QgsApplication.taskManager().addTask(task)
            while QgsApplication.taskManager().count() > 0:
                QCoreApplication.processEvents()
        """
        if type != 'view': 
            dump_nbr = '3'
        pg_string = r'{} --host {} --port 34000 --format=p --schema-only  --no-owner --section=data --section=pre-data --section=post-data --encoding {} --table {}.{} {} > "{}\structures\{}_{}.sql"'.format(pg_path, url, encoding, schema, table, database, folder, dump_nbr, table)
        task = DumpTask(pg_string)
        QgsApplication.taskManager().addTask(task)
        while QgsApplication.taskManager().count() > 0:
            QCoreApplication.processEvents()
        file_object = open(r'{}\structures\{}_{}.sql'.format(folder, dump_nbr, table), 'r+', encoding="cp1252")
        if encoding != 'UTF8':
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
            if valeur not in self.written_enums:
                file_object.write('{}\n'.format(str(valeur)))
                self.written_enums.append((str(valeur)))
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
            if function[0] not in self.written_functions:
                file_object.write('{};\n'.format(function[0]))
                self.written_functions.append(function[0])
        file_object.close()
        progress = int((nb + 1) * 100 / total)
        self.dlg.progressBar.setValue(progress) 

    def dump(self) :
        try :
            if self.dlg.lineEdit_2.text() != 'sigli' : 
                conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
            else :
                conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            self.written_functions = []
            self.written_enums = []
            cst_val = []
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
                if os.path.exists(folder + '\\structures') is False:
                    os.mkdir(folder + '\\structures')
                if os.path.exists(folder + '\\données') is False:
                    os.mkdir(folder + '\\données')
            self.dlg.progressBar.setValue(progress)
            for nb, table in enumerate(tables) :
                progress = int((nb + 1) * 100 / total)
                if table in self.tables:
                    if self.tables[table] == 'VIEW':
                        cur.execute(
                                    """
                                    SELECT DISTINCT
                                    pg_class.oid::regclass::text as table_name
                                    FROM pg_rewrite
                                    JOIN pg_depend ON
                                    pg_depend.classid = 'pg_rewrite'::regclass AND
                                    pg_depend.objid = pg_rewrite.oid AND
                                    pg_depend.refclassid = 'pg_class'::regclass AND
                                    pg_depend.refobjid <> pg_rewrite.ev_class
                                    JOIN pg_class ON
                                    pg_class.oid = pg_depend.refobjid AND
                                    pg_class.relkind IN ('r','f','p','v','m')
                                    WHERE
                                    pg_rewrite.ev_class = '{}.{}'::regclass
                                                            """.format(schema, table) )
                        view_tables= {row[0] for row in cur.fetchall()}
                        for view_table in view_tables : 
                            schema_view, table_view = view_table.split('.')
                            dump_nbr = "3"
                            type = 'table'
                            self.dumper(cur, url, encoding, schema_view, table_view, database, folder, dump_nbr, nb, total, cst_val, type)
                        dump_nbr = "5"
                        type = 'view'
                    else : 
                        dump_nbr = "3"
                        type = 'table'
                self.dumper(cur, url, encoding, schema, table, database, folder, dump_nbr, nb, total, cst_val, type)
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
    
    def choose_3(self):
        self.folder_path_import = QFileDialog.getExistingDirectory(self.dlg, 'Select Folder')
        self.folder_path_import = self.folder_path_import.replace('/', '\\')
        if self.folder_path_import:
            self.dlg.lineEdit_7.setText(self.folder_path_import)

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
            list = sorted([row[0] for row in rows])
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

    def connection_2(self):
        if self.dlg.lineEdit_6.text() != 'sigli' : 
            conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_6.text())
        else :
            conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_6.text())
        try :
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT  n.nspname || '.' || t.typname AS type FROM information_schema.columns c JOIN pg_type t ON c.udt_name = t.typname JOIN pg_namespace n ON t.typnamespace = n.oid LEFT JOIN pg_namespace n1 ON t.typnamespace = n1.oid JOIN information_schema.tables t2 ON c.table_schema = t2.table_schema AND c.table_name = t2.table_name WHERE t.typcategory = 'E'")
            rows = cur.fetchall()
            list = sorted([row[0] for row in rows])
            self.dlg.comboBox.clear()
            self.dlg.comboBox.addItems(list)
            self.dlg.lineEdit_6.setStyleSheet(f'QWidget {{background-color:  #009900;}}')
        except Exception as err :
            self.dlg.lineEdit_6.setStyleSheet(f'QWidget {{background-color:  #ff0000;}}')
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage('Erreur de Connection' + ':' + str(err))
            cur.close()
            conn.close()
        cur.close() if cur and not cur.closed else None
        conn.close() if conn and not conn.closed else None

    def enum_fill_table(self):
        if self.dlg.lineEdit_6.text() != 'sigli':
            conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_6.text())
        else:
            conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_6.text())
        try:
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            enum = self.dlg.comboBox.currentText()
            cur.execute("SELECT enumlabel FROM pg_enum WHERE enumtypid = '{}'::regtype;".format(enum))
            values = {row[0] for row in cur.fetchall()}
            self.dlg.listWidget.clear()
            for value in sorted(values):
                item = QListWidgetItem(value)
                item.setFlags(item.flags() | QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsDragEnabled)
                self.dlg.listWidget.addItem(item)
        except psycopg2.Error as err:
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage('Erreur de chargement des valeurs d''énumérations' + ':' + str(err))
        finally:
            cur.close() if cur else None
            conn.close() if conn else None

    def enumerations(self): 
        try :
            if self.dlg.lineEdit_6.text() != 'sigli' : 
                conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_6.text())
            else :
                conn_string = 'postgresql://@bpsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_6.text())
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            enum_list = []
            folder = self.dlg.lineEdit_7.text()
            progress = 10
            self.dlg.progressBar_3.setValue(progress)
            cur.execute("SELECT DISTINCT c.column_name, c.table_schema || '.' || c.table_name AS table_name FROM information_schema.columns c JOIN pg_type t ON c.udt_name = t.typname JOIN pg_namespace n ON t.typnamespace = n.oid LEFT JOIN pg_namespace n1 ON t.typnamespace = n1.oid JOIN information_schema.tables t2 ON c.table_schema = t2.table_schema AND c.table_name = t2.table_name WHERE t.typcategory = 'E' AND n.nspname || '.' || t.typname = '{}'".format(self.dlg.comboBox.currentText()))
            curfetch = cur.fetchall()
            column_table_dict = {row[1]: row[0] for row in curfetch}
            table_column_dict = {row[0]: row[1] for row in curfetch}
            file_object = open('{}\\1_enums.sql'.format(folder), 'w', encoding="cp1252")
            if file_object.tell() == 0 :
                file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                file_object.write('--Création des Enumérations\n\n')
            values = [self.dlg.listWidget.item(index).text() for index in range(self.dlg.listWidget.count())]
            enum_values = ", ".join(["'{}'".format(value.replace("'", "''")) for value in values])
            enum_name = self.dlg.comboBox.currentText()
            table_name = enum_name.split(".")[1]
            enum_old = "ALTER TYPE {enum_name} RENAME TO {table_name}_old;".format(enum_name = enum_name, table_name = table_name)
            enum_string = "CREATE TYPE {enum_name} AS ENUM ({enum_values});".format(enum_name=enum_name, enum_values=enum_values)
            cur.close()
            conn.close()
            file_object.write('--Rennomage de l\'ancienne énumération\n')
            file_object.write('{}\n\n'.format(enum_old))
            file_object.write('--Création du nouvel énumérateur\n')
            file_object.write('{}\n\n'.format(enum_string))
            file_object.write('\n\n')
            file_object.write('-- !! EFFECTUER VOS OPERATION DE CHANGEMENT DANS LES TABLES ICI CI BESOIN. POUR RAJOUTER UNE VALEUR, MODIFIER L\'ENUM_OLD AVANT !! --\n')
            file_object.write('\n\n')
            file_object.write('--Recast des types\n')
            for key, value in column_table_dict.items():
                enum_recast = "ALTER TABLE {key} ALTER COLUMN {value} TYPE {enum_name} USING {value}::text::{enum_name};".format(value=value, key=key, enum_name=enum_name)
                file_object.write('{}\n'.format(enum_recast))
                enum_list.append(enum_recast)
            for key, value in table_column_dict.items():
                enum_recast = "ALTER TABLE {value} ALTER COLUMN {key} TYPE {enum_name} USING {key}::text::{enum_name};".format(value=value, key=key, enum_name=enum_name)
                if enum_recast not in enum_list:
                    file_object.write('{}\n'.format(enum_recast))
            file_object.write('\n')
            file_object.write('--Supression de l\'ancienne énumération\n')
            enum_drop = "DROP TYPE {enum_name}_old ;".format(enum_name=enum_name, enum_values=enum_values)
            file_object.write('{}\n'.format(enum_drop))
            file_object.close()
            self.dlg.progressBar_3.setValue(0)
        except Exception as e :
            self.dlg.progressBar_3.setValue(0)
            pass
    
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