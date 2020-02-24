Name: aiven-kafka-connect-s3
Version: %{major_version}
Release: %{minor_version}%{?dist}
Summary: Aiven Kafka Connect S3 Connector
Group: Applications/Internet
License: Apache (v2)
URL: https://aiven.io/
Source0: aiven-kafka-connect-s3-src.tar
BuildArch: noarch
BuildRequires: java
Requires: java
Packager: Heikki Nousiainen <htn@aiven.io>

%description
Aiven Kafka Connect S3 Connector

%prep
%setup

%build
./gradlew -Pmodule_version=%{major_version} clean build

%install
%{__mkdir_p} %{buildroot}/opt/aiven-kafka/libs
install build/libs/aiven-kafka-connect-s3-%{version}.jar %{buildroot}/opt/aiven-kafka/libs/aiven-kafka-connect-s3-%{version}.jar

%files
/opt/aiven-kafka/libs/aiven-kafka-connect-s3-%{version}.jar

%changelog
* Wed Oct  5 2016 Heikki Nousiainen <htn@aiven.io>
- First build
