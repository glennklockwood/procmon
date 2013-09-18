# Makefile for procmon
# Author: Doug Jacobsen <dmjacobsen@lbl.gov>
# 2013/02/17
# 

INSTALL_PATH=/global/project/projectdirs/genepool/www/procmon

install: houseHunter.html .htaccess
	chmod a+rX $^
	cp -p houseHunter.html $(INSTALL_PATH)/houseHunter-dev.html

prod: houseHunter.html .htaccess
	chmod a+rX $^
	cp -p houseHunter.html $(INSTALL_PATH)/houseHunter.html
