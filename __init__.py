# -*- coding: utf-8 -*-
"""
/***************************************************************************
 Productor
                                 A QGIS plugin
 Passages en production
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                             -------------------
        begin                : 2021-12-15
        copyright            : (C) 2021 by Eurométropole de Strasbourg
        email                : clement.zitouni@strasbourg.eu
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load Productor class from file Productor.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    #
    from .productor import Productor
    return Productor(iface)
