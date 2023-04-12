from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QFileDialog, QErrorMessage
from qgis.core import QgsTask, QgsMessageLog, QgsApplication
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction
from .resources import *
from .productor_dialog import ProductorDialog
import os.path
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__)) + '\\include\\python')
import sqlalchemy as db
import psycopg2
import geoalchemy2

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
            QgsMessageLog.logMessage(str(e), 'Productor', level=QgsMessageLog.CRITICAL)
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
        self.dlg.checkBox_2.stateChanged.connect(self.checkBox_2_state_changed)
        self.dlg.checkBox.stateChanged.connect(self.checkBox_state_changed)
        self.dlg.toolButton.clicked.connect(self.choose)
        self.dlg.comboBox_3.activated.connect(self.table)
        self.dlg.show()
        self.dlg.closeEvent = self.closeEvent

    def checkBox_2_state_changed(self, state):
        if state == Qt.Checked:
            self.dlg.checkBox.setChecked(False)

    def checkBox_state_changed(self, state):
        if state == Qt.Checked:
            self.dlg.checkBox_2.setChecked(False)

    def table(self) :
        self.schema = self.dlg.comboBox_3.currentText()
        tables = self.insp.get_table_names(schema = self.schema)
        self.dlg.comboBox_2.clear()
        self.dlg.comboBox_2.addItems(sorted(tables))

    def dump(self) :
        try :
            enum_list = []
            str_id = None
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
            if os.path.exists(folder) is False : 
                os.mkdir(folder)
            self.dlg.progressBar.setValue(progress)
            for nb, table in enumerate(tables) :
                progress = int((nb + 1) * 100 / total)
                if self.dlg.checkBox.isChecked():
                    pg_string = r'{} --host bdsigli.cus.fr --port 34000 --format=c --no-owner --encoding WIN1252 --table {}.{} {} > "{}\{}.backup"'.format(pg_path, schema, table, database, folder, table)
                else:
                    pg_string = r'{} --host bdsigli.cus.fr --port 34000 --format=p --schema-only --no-owner --section=pre-data --section=post-data --encoding WIN1252 --table {}.{} {} > "{}\{}.sql"'.format(pg_path, schema, table, database, folder, table)
                task = DumpTask(pg_string)
                QgsApplication.taskManager().addTask(task)
                while QgsApplication.taskManager().count() > 0:
                    QCoreApplication.processEvents()
                if self.dlg.checkBox_2.isChecked():
                    file_object = open('{}\{}.sql'.format(folder, table), 'r+', encoding="cp1252")
                    content = file_object.read()
                    file_object.seek(0,0)
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n' + content)
                    file_object.close()
                # ENUMS
                columns_table = self.insp.get_columns(table, schema)
                self.cur.execute("SELECT format( 'CREATE TYPE %s AS ENUM (%s);', enumtypid::regtype, string_agg(quote_literal(enumlabel), ', ') ) FROM pg_enum GROUP BY enumtypid;")
                enum_list = self.cur.fetchall()
                # GRANTS
                for c in columns_table : 
                    if 'seq' in str(c['default']) :
                        str_id = str(c['default'])
                        sub1 = "nextval('"
                        sub2 = "_seq'::regclass"
                        idx1 = str_id.index(sub1)
                        idx2 = str_id.index(sub2)
                        str_id = str_id[idx1 + len(sub1) : idx2]
                    for i in enum_list :
                        if str(c['type']) in i[0] :
                            cst_val.append(i[0])    
                cst_val = list(dict.fromkeys(cst_val))
                if str_id :
                    file_object = open('{}\{}_grants.sql'.format(folder, table), 'w', encoding="cp1252")
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                    file_object.write(f'''---Grants
                    GRANT SELECT ON TABLE {str_id}_seq TO role_sigli_c;
                    GRANT USAGE ON SCHEMA {schema} TO role_sigli_c;
                    GRANT SELECT ON TABLE {schema}.{table} TO role_sigli_c;
                    GRANT USAGE ON TABLE {str_id}_seq TO role_sigli_c;
                    GRANT USAGE ON SCHEMA {schema} TO role_sigli_{schema}_a;
                    GRANT UPDATE ON TABLE {str_id}_seq TO role_sigli_{schema}_a;
                    GRANT SELECT ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT UPDATE ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT INSERT ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT USAGE ON TABLE {str_id}_seq TO role_sigli_{schema}_a;
                    GRANT DELETE ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT SELECT ON TABLE {str_id}_seq TO role_sigli_{schema}_a;''')
                    file_object.close()
                    file_object = open('{}\{}_seq.sql'.format(folder, table), 'w', encoding="cp1252")
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                    file_object.write(f'''---Sequence
                    CREATE SEQUENCE IF NOT EXISTS {str_id}_seq
                        START WITH 1
                        INCREMENT BY 1
                        NO MINVALUE
                        NO MAXVALUE
                        CACHE 1;
                    ALTER SEQUENCE {str_id}_seq
                        OWNER TO sigli;''')
                    file_object.close()
                else :
                    file_object = open('{}\{}_grants.sql'.format(folder, table), 'w', encoding="cp1252")
                    file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                    file_object.write(f'''---Grants
                    GRANT USAGE ON SCHEMA {schema} TO role_sigli_c;
                    GRANT SELECT ON TABLE {schema}.{table} TO role_sigli_c;
                    GRANT USAGE ON SCHEMA {schema} TO role_sigli_{schema}_a;
                    GRANT SELECT ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT UPDATE ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT INSERT ON TABLE {schema}.{table} TO role_sigli_{schema}_a;
                    GRANT DELETE ON TABLE {schema}.{table} TO role_sigli_{schema}_a;''')
                    file_object.close()
                file_object = open('{}\{}_enums.sql'.format(folder, table), 'w', encoding="cp1252")
                file_object.write('--########### encodage fichier cp1252 ###(controle: n°1: éàçêè )####\n')
                file_object.write('--Création des Enumérations\n')
                for val in cst_val : 
                    file_object.write('{}\n'.format(val))
                file_object.close()
                progress = int((nb + 1) * 100 / total)
                self.dlg.progressBar.setValue(progress)
            self.dlg.progressBar.setValue(0)
        except Exception as e : 
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage(str(e))
            self.dlg.progressBar.setValue(0)
            pass 
    
    def choose(self):
        self.folder_path = QFileDialog.getExistingDirectory(self.dlg, 'Select Folder')
        self.folder_path = self.folder_path.replace('/', '\\')
        if self.folder_path:
            self.dlg.lineEdit.setText(self.folder_path)

    def connection(self) : 
        conn_string = 'postgresql://@bdsigli.cus.fr:34000/{}'.format(self.dlg.lineEdit_2.text())
        try :
            engine = db.create_engine(conn_string)
            self.insp = db.inspect(engine)
            list = self.insp.get_schema_names()
            conn = psycopg2.connect(conn_string)
            self.cur = conn.cursor()
            self.dlg.comboBox_2.clear()
            self.dlg.comboBox_3.clear()
            self.dlg.comboBox_3.addItems(list)
            self.dlg.lineEdit_2.setStyleSheet(f'QWidget {{background-color:  #009900;}}')
        except db.exc.SQLAlchemyError as err :
            self.dlg.lineEdit_2.setStyleSheet(f'QWidget {{background-color:  #ff0000;}}')
            self.error_dialog = QErrorMessage()
            self.error_dialog.showMessage('Erreur de Connection' + ':' + str(err))
    
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

