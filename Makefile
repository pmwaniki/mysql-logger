VENV = venv
CWD = $(shell pwd)

virtual_env:
	apt install python3-venv
#	@which python3
#	pip3 install --upgrade pip3
	python3 -m venv $(VENV)
	#./$(VENV)/bin/activate

install:
	make virtual_env
	./$(VENV)/bin/python -m pip install --upgrade pip
	./$(VENV)/bin/python -m pip install -r requirements.txt
	#cp  mysql-binlog.service /etc/systemd/system/mysql-binlog.service
	sed 's@<CWD>@$(CWD)@g' mysql-binlog.service > /etc/systemd/system/mysql-binlog.service
	sed 's@<CWD>@$(CWD)@g' mysql-kafka-upload.service > /etc/systemd/system/mysql-kafka-upload.service

#	systemctl deamon-reload
#
#	systemctl enable mysql-binlog.service
#	systemctl enable mysql-kafka-upload.service

uninstall:
	-systemctl stop mysql-binlog.service
	-systemctl stop mysql-kafka-upload.service
	-rm -f /etc/systemd/system/mysql-binlog.service
	-rm -f /etc/systemd/system/mysql-kafka-upload.service
	systemctl daemon-reload
	rm -rf $(VENV)
