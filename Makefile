short_ver = $(shell git describe --abbrev=0 2>/dev/null || echo 0.0.1)
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

SOURCES := \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3Config.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3Constants.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3MultipartUpload.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3OutputStream.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3SinkConnector.java \
	src/main/java/io/aiven/kafka/connect/s3/AivenKafkaConnectS3SinkTask.java \
	pom.xml \
	aiven-kafka-connect-s3.spec

all: rpm

build-dep:
	sudo dnf install -y --allowerasing --best \
	   java-1.8.0-openjdk-devel \
	   maven

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
	mvn -Dmodule_version=0.0.1 test
