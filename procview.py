#!/usr/bin/env python

import sys
from PyQt4 import QtGui,QtCore

class LeftPane(QtGui.QWidget):
	def __init__(self):
		super(LeftPane,self).__init__()
		okButton = QtGui.QPushButton("OK")
		cancelButton = QtGui.QPushButton("Cancel")
	
		hbox = QtGui.QHBoxLayout()
		hbox.addStretch(1)
		hbox.addWidget(okButton)
		hbox.addWidget(cancelButton)
	
		vbox = QtGui.QVBoxLayout()
		vbox.addStretch(1)
		vbox.addLayout(hbox)
	
		self.setLayout(vbox)

		self.setGeometry(300, 300, 300, 150)
		self.setWindowTitle('Buttons')
		self.show()

class Example(QtGui.QMainWindow):

	def __init__(self):
		super(Example, self).__init__()
		self.initUI()

	def initUI(self):
		QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
		self.setToolTip('This is a <b>QWidget</b> widget')

		self.statusBar().showMessage('Ready')

		exitAction = QtGui.QAction(QtGui.QIcon('exit.png'), '&Exit', self)
		exitAction.setShortcut('Ctrl+Q')
		exitAction.setStatusTip('Exit JobView')
		exitAction.triggered.connect(QtGui.qApp.quit)

		menubar = self.menuBar()
		filemenu = menubar.addMenu('&File')
		filemenu.addAction(exitAction)

		btn = LeftPane()
		btn.move(0, 0)
		btn.show()

		self.resize(250, 150)
		self.center()
		self.setWindowTitle('Tooltips')
		self.show()

	def changeMessage(self):
		self.statusBar().showMessage('Button clicked!')

	def closeEvent(self, event):
		reply = QtGui.QMessageBox.question(self, 'Message',
			"Are you sure you want to quit?", QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
		if reply == QtGui.QMessageBox.Yes:
			event.accept()
		else:
			event.ignore()

	def center(self):
		qr = self.frameGeometry()
		cp = QtGui.QDesktopWidget().availableGeometry().center()
		qr.moveCenter(cp)
		self.move(qr.topLeft())

def main():
	app = QtGui.QApplication(sys.argv)
	ex = LeftPane()

	sys.exit(app.exec_())

if __name__ == "__main__":
	main()
