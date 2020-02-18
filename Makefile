##
# Copyright (C) 2020 Aiven Oy
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
##

short_ver = $(shell git describe --abbrev=0 2>/dev/null || echo 0.0.1)
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

SOURCES := \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3Config.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3Constants.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3MultipartUpload.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3OutputStream.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3SinkConnector.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3SinkTask.java \
	build.gradle \
	gradle/ \
	gradlew \
	aiven-kafka-connect-s3.spec

all: rpm

build-dep:
	sudo dnf install -y --allowerasing --best \
	   rpm-build java-1.8.0-openjdk-devel

clean:
	$(RM) -r rpm/ rpmbuild/

rpm: $(SOURCES)
	mkdir -p rpmbuild/
	git archive --output=rpmbuild/aiven-kafka-connect-s3-src.tar --prefix=aiven-kafka-connect-s3-$(short_ver)/ HEAD
	rpmbuild -bb aiven-kafka-connect-s3.spec \
	    --define '_topdir $(CURDIR)/rpmbuild' \
	    --define '_sourcedir $(CURDIR)/rpmbuild' \
	    --define 'major_version $(short_ver)' \
	    --define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	mkdir -p "$@/"
	cp "$(CURDIR)/rpmbuild/RPMS/noarch"/*.rpm "$@/"

test:
	./gradlew -Pmodule_version=0.0.1 test
