Name: aiven-kafka-connect-s3
Version: %{major_version}
Release: %{minor_version}%{?dist}
Summary: Aiven Kafka Connect S3 Connector
Group: Applications/Internet
License: Apache (v2)
URL: https://aiven.io/
Source0: aiven-kafka-connect-s3-src.tar
BuildArch: noarch
BuildRequires: java, maven
Requires: java
Packager: Heikki Nousiainen <htn@aiven.io>

%description
Aiven Kafka Connect S3 Connector

%prep
%setup

%build
mvn -Dmodule_version=%{major_version} package

%install
%{__mkdir_p} %{buildroot}/opt/aiven-kafka/libs
install target/aiven-kafka-connect-s3-%{version}.jar %{buildroot}/opt/aiven-kafka/libs/aiven-kafka-connect-s3-%{version}.jar

%files
/opt/aiven-kafka/libs/aiven-kafka-connect-s3-%{version}.jar

%changelog
* Wed Oct  5 2016 Heikki Nousiainen <htn@aiven.io>
- First build

* Thurs Dec 5 2019 Alan Kehoe <alankehoe111@gmail.com>
- Add the following template variables [utc_year, utc_month, utc_day, local_year, local_month, local_day]
